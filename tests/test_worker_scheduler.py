"""Tests for worker.py and scheduler.py: process supervision, worker execution loops, and cron scheduling."""

import os
import sys
import tempfile
import time
import subprocess
import signal
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

_TMPDB = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_TMPDB.close()
os.environ["DATAFLOW_DB"] = _TMPDB.name

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import config
from config import init_db, ResolvedConfig, CronJobConfig, PipelineConfig
import jobs
import worker
import scheduler
from worker import Worker
from scheduler import start_scheduler, _enqueue_scheduled, _prune_audit_logs_job


@pytest.fixture(scope="module", autouse=True)
def _init():
    init_db()
    yield
    if os.path.exists(_TMPDB.name):
        try:
            os.remove(_TMPDB.name)
        except Exception:
            pass


# --- Scheduler Tests ---

def test_enqueue_scheduled(monkeypatch):
    enqueued = []
    monkeypatch.setattr(jobs, "enqueue", lambda pipeline, trigger, max_attempts: enqueued.append((pipeline, trigger, max_attempts)))
    
    _enqueue_scheduled("pipe1", 3)
    assert enqueued == [("pipe1", "schedule", 3)]


def test_prune_audit_logs_job(monkeypatch):
    pruned_days = []
    monkeypatch.setattr(config, "prune_audit_logs", lambda days: pruned_days.append(days))
    
    from settings import settings as app_settings
    app_settings.set("audit_retention_days", "45")
    
    _prune_audit_logs_job()
    assert pruned_days == [45]


def test_start_scheduler(monkeypatch):
    # Mock BackgroundScheduler
    mock_sched = MagicMock()
    mock_sched_class = MagicMock(return_value=mock_sched)
    monkeypatch.setattr(scheduler, "BackgroundScheduler", mock_sched_class)
    
    cfg = ResolvedConfig(
        pipelines={
            "p1": PipelineConfig(name="p1", source_path="s", sink_path="d", alerts={"on_failure": "none"})
        },
        cronjobs={
            "cron1": CronJobConfig(
                name="cron1", pipeline="p1", schedule="0 0 * * *", timezone="UTC", enabled=True,
                retry={"max_attempts": 2, "delay_seconds": 60}
            ),
            "cron2": CronJobConfig(
                name="cron2", pipeline="p1", schedule="0 0 * * *", timezone="UTC", enabled=False,
                retry={"max_attempts": 2, "delay_seconds": 60}
            )
        }
    )

    # Reset global scheduler state
    monkeypatch.setattr(scheduler, "_scheduler", None)
    
    start_scheduler(cfg)

    # Should instantiate BackgroundScheduler once
    assert mock_sched_class.call_count == 1
    
    # Verify that only enabled cron job + audit retention job are scheduled (2 jobs total)
    assert mock_sched.add_job.call_count == 2
    
    # Verify first scheduled job details
    calls = mock_sched.add_job.call_args_list
    scheduled_ids = [c.kwargs.get("id") for c in calls]
    assert "cron1" in scheduled_ids
    assert "_audit_retention" in scheduled_ids
    assert "cron2" not in scheduled_ids


def test_scheduler_uses_configured_cronjob_timezone(monkeypatch):
    mock_sched = MagicMock()
    monkeypatch.setattr(scheduler, "BackgroundScheduler", MagicMock(return_value=mock_sched))
    monkeypatch.setattr(scheduler, "_scheduler", None)

    cfg = ResolvedConfig(
        pipelines={
            "p1": PipelineConfig(name="p1", source_path="s", sink_path="d", alerts={"on_failure": "none"})
        },
        cronjobs={
            "india_midnight": CronJobConfig(
                name="india_midnight",
                pipeline="p1",
                schedule="0 0 * * *",
                timezone="Asia/Kolkata",
                enabled=True,
                retry={"max_attempts": 2, "delay_seconds": 60},
            )
        },
    )

    start_scheduler(cfg)

    scheduled_call = next(
        call for call in mock_sched.add_job.call_args_list
        if call.kwargs.get("id") == "india_midnight"
    )
    trigger = scheduled_call.args[1]
    assert trigger.timezone == ZoneInfo("Asia/Kolkata")

    next_run = trigger.get_next_fire_time(
        None, datetime(2026, 7, 16, 18, 29, tzinfo=timezone.utc)
    )
    assert next_run == datetime(2026, 7, 17, 0, 0, tzinfo=ZoneInfo("Asia/Kolkata"))


# --- Worker Loop Tests ---

def test_worker_run_job(monkeypatch):
    claimed_job = MagicMock()
    claimed_job.id = 42
    claimed_job.pipeline = "test_pipe"
    claimed_job.attempts = 1

    claim_calls = [claimed_job, None]  # Claim once, then return None to stop loop
    
    def mock_claim_next(pid):
        return claim_calls.pop(0) if claim_calls else None

    # Mocks
    monkeypatch.setattr(jobs, "claim_next", mock_claim_next)
    
    run_calls = []
    monkeypatch.setattr(worker, "run_pipeline", lambda pipe, resolved: run_calls.append(pipe))
    
    finished_status = []
    monkeypatch.setattr(jobs, "finish", lambda job_id, status, error_message=None: finished_status.append((job_id, status)))
    
    # Instantiate worker with stop event set after one iteration
    w = Worker(poll_seconds=1, stale_seconds=10)
    
    # We will mock the heartbeat loop to avoid spawning a real thread that sleeps
    monkeypatch.setattr(w, "_heartbeat_loop", lambda job_id, stop: None)
    
    w._run_job(claimed_job)
    
    # Verify pipeline was run and marked success
    assert run_calls == ["test_pipe"]
    assert finished_status == [(42, "success")]


def test_worker_run_job_failure(monkeypatch):
    claimed_job = MagicMock()
    claimed_job.id = 43
    claimed_job.pipeline = "test_pipe"
    claimed_job.attempts = 1

    def mock_run_pipeline(pipe, resolved):
        raise ValueError("Pipeline crashed")

    monkeypatch.setattr(worker, "run_pipeline", mock_run_pipeline)
    
    finished_status = []
    monkeypatch.setattr(jobs, "finish", lambda job_id, status, error_message=None: finished_status.append((job_id, status, error_message)))
    
    w = Worker(poll_seconds=1, stale_seconds=10)
    monkeypatch.setattr(w, "_heartbeat_loop", lambda job_id, stop: None)
    
    w._run_job(claimed_job)
    
    # Verify job marked failed with error details
    assert finished_status == [(43, "failed", "Pipeline crashed")]


# --- Worker Supervision Tests ---

def test_worker_process_supervision(monkeypatch):
    # Mock subprocess.Popen
    mock_popen = MagicMock()
    mock_popen.pid = 9999
    mock_popen.poll.return_value = None  # running
    
    monkeypatch.setattr(subprocess, "Popen", MagicMock(return_value=mock_popen))
    monkeypatch.setattr(worker, "_worker_proc", None)
    
    # Start supervision
    worker.start_worker_process()
    assert worker.is_worker_running() is True
    assert worker._worker_proc == mock_popen
    
    # Stop supervision
    mock_killpg = MagicMock()
    monkeypatch.setattr(os, "killpg", mock_killpg)
    monkeypatch.setattr(os, "getpgid", lambda pid: 8888)
    
    worker.stop_worker_process()
    assert worker.is_worker_running() is False
    assert mock_killpg.call_count == 1
    mock_killpg.assert_called_with(8888, signal.SIGTERM)
