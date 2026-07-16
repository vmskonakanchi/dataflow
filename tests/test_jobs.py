"""Tests for jobs.py: durable background job queue operations, state changes, retries, and crash reconciliation."""

import os
import sys
import tempfile
import time
from datetime import datetime, timedelta
import pytest

_TMPDB = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_TMPDB.close()
os.environ["DATAFLOW_DB"] = _TMPDB.name

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import config
from config import init_db, Job, engine
from sqlmodel import Session, select
import jobs


@pytest.fixture(scope="module", autouse=True)
def _init():
    init_db()
    yield
    if os.path.exists(_TMPDB.name):
        try:
            os.remove(_TMPDB.name)
        except Exception:
            pass


@pytest.fixture(autouse=True)
def _clear_jobs():
    with Session(engine) as session:
        session.execute(config.SQLModel.metadata.tables["job"].delete())
        session.commit()
    yield


# --- Enqueue & Deduplication Tests ---

def test_enqueue_basic():
    job_id = jobs.enqueue("test_pipeline", trigger="manual", max_attempts=3)
    assert job_id is not None

    with Session(engine) as session:
        job = session.get(Job, job_id)
        assert job is not None
        assert job.pipeline == "test_pipeline"
        assert job.status == "queued"
        assert job.trigger == "manual"
        assert job.max_attempts == 3
        assert job.attempts == 0
        assert (datetime.utcnow() - job.enqueued_at).total_seconds() < 5


def test_enqueue_dedupe():
    # Enqueue first job
    job1_id = jobs.enqueue("dedupe_pipe", dedupe=True)
    assert job1_id is not None

    # Enqueue second job for the same pipeline while first is queued -> should be deduped (return None)
    job2_id = jobs.enqueue("dedupe_pipe", dedupe=True)
    assert job2_id is None

    # Enqueue with dedupe=False -> should allow concurrent jobs
    job3_id = jobs.enqueue("dedupe_pipe", dedupe=False)
    assert job3_id is not None
    assert job3_id != job1_id


def test_is_pipeline_active():
    assert jobs.is_pipeline_active("active_pipe") is False
    jobs.enqueue("active_pipe")
    assert jobs.is_pipeline_active("active_pipe") is True


# --- Job Claiming Tests ---

def test_claim_next():
    job_id = jobs.enqueue("claim_pipe", trigger="manual")
    
    # Claim the job
    claimed = jobs.claim_next(worker_pid=1234)
    assert claimed is not None
    assert claimed.id == job_id
    assert claimed.status == "running"
    assert claimed.worker_pid == 1234
    assert claimed.attempts == 1
    assert claimed.heartbeat_at is not None
    assert claimed.started_at is not None

    # Try claiming again -> no queued jobs left, should return None
    assert jobs.claim_next(worker_pid=1234) is None


def test_claim_next_run_after():
    # Enqueue a job with a future run_after time
    with Session(engine) as session:
        job = Job(
            pipeline="future_pipe",
            status="queued",
            run_after=datetime.utcnow() + timedelta(hours=1),
            enqueued_at=datetime.utcnow()
        )
        session.add(job)
        session.commit()

    # Should not be claimed yet
    assert jobs.claim_next() is None


# --- Heartbeat & Finish Tests ---

def test_heartbeat():
    job_id = jobs.enqueue("hb_pipe")
    claimed = jobs.claim_next(worker_pid=111)
    orig_hb = claimed.heartbeat_at

    # Update heartbeat
    time.sleep(0.1)
    jobs.heartbeat(job_id)

    with Session(engine) as session:
        updated = session.get(Job, job_id)
        assert updated.heartbeat_at > orig_hb


def test_finish_success():
    job_id = jobs.enqueue("success_pipe")
    jobs.claim_next()

    # Finish job as success
    jobs.finish(job_id, "success")

    with Session(engine) as session:
        job = session.get(Job, job_id)
        assert job.status == "success"
        assert job.finished_at is not None
        assert job.error_message is None


def test_finish_failure_with_retries():
    # Max attempts = 2
    job_id = jobs.enqueue("retry_pipe", max_attempts=2)
    jobs.claim_next()  # attempt 1

    # Finish job as failed -> should requeue since attempt 1 < max 2
    jobs.finish(job_id, "failed", error_message="First try failed", retry_delay_seconds=10)

    with Session(engine) as session:
        job = session.get(Job, job_id)
        assert job.status == "queued"
        assert job.trigger == "retry"
        assert job.worker_pid is None
        assert job.heartbeat_at is None
        assert job.attempts == 1  # attempts preserved
        assert job.error_message == "First try failed"
        assert job.run_after > datetime.utcnow() + timedelta(seconds=5)


def test_finish_failure_no_more_retries():
    # Max attempts = 1
    job_id = jobs.enqueue("fail_pipe", max_attempts=1)
    jobs.claim_next()  # attempt 1

    # Finish job as failed -> should remain failed since attempt 1 == max 1
    jobs.finish(job_id, "failed", error_message="Permanent failure")

    with Session(engine) as session:
        job = session.get(Job, job_id)
        assert job.status == "failed"
        assert job.finished_at is not None
        assert job.error_message == "Permanent failure"


# --- Crash Reconciliation Tests ---

def test_reconcile_stale_jobs():
    now = datetime.utcnow()
    stale_time = now - timedelta(minutes=5)
    
    with Session(engine) as session:
        # 1. Stale job that can be retried (attempts=1, max=3)
        job_retry = Job(
            pipeline="stale_retry", status="running", attempts=1, max_attempts=3,
            started_at=stale_time, heartbeat_at=stale_time, worker_pid=111,
            enqueued_at=stale_time, run_after=stale_time
        )
        # 2. Stale job that has no attempts left (attempts=2, max=2)
        job_fail = Job(
            pipeline="stale_fail", status="running", attempts=2, max_attempts=2,
            started_at=stale_time, heartbeat_at=stale_time, worker_pid=222,
            enqueued_at=stale_time, run_after=stale_time
        )
        # 3. Job running fine (recent heartbeat)
        job_ok = Job(
            pipeline="stale_ok", status="running", attempts=1, max_attempts=3,
            started_at=now, heartbeat_at=now, worker_pid=333,
            enqueued_at=now, run_after=now
        )
        # 4. Job that never heartbeated but started long ago (attempts=1, max=3)
        job_never_beat = Job(
            pipeline="never_beat", status="running", attempts=1, max_attempts=3,
            started_at=stale_time, heartbeat_at=None, worker_pid=444,
            enqueued_at=stale_time, run_after=stale_time
        )
        session.add_all([job_retry, job_fail, job_ok, job_never_beat])
        session.commit()
        
        job_retry_id = job_retry.id
        job_fail_id = job_fail.id
        job_ok_id = job_ok.id
        job_never_beat_id = job_never_beat.id

    # Run reconciliation (cutoff = 60s)
    recovered = jobs.reconcile_stale(stale_seconds=60)
    assert recovered == 3  # retry, fail, never_beat should be recovered

    with Session(engine) as session:
        r_retry = session.get(Job, job_retry_id)
        assert r_retry.status == "queued"
        assert r_retry.trigger == "retry"
        assert r_retry.worker_pid is None
        assert "stale heartbeat" in r_retry.error_message

        r_fail = session.get(Job, job_fail_id)
        assert r_fail.status == "failed"
        assert r_fail.finished_at is not None
        assert "no attempts left" in r_fail.error_message

        r_ok = session.get(Job, job_ok_id)
        assert r_ok.status == "running"  # should remain untouched

        r_nb = session.get(Job, job_never_beat_id)
        assert r_nb.status == "queued"
        assert r_nb.trigger == "retry"


# --- Recent Jobs Observability Test ---

def test_recent_jobs():
    j1 = jobs.enqueue("p1")
    j2 = jobs.enqueue("p2")
    
    recent = jobs.recent_jobs(limit=10)
    assert len(recent) == 2
    # Ordered desc by enqueued_at/id
    assert recent[0].id == j2
    assert recent[1].id == j1
