"""Cron scheduler.

On each cron trigger it enqueues a job onto the durable queue; the worker
executes it (with retries handled by the queue's attempts/max_attempts). Runs
embedded in the server process — no separate CLI process required.
"""

import logging
from zoneinfo import ZoneInfo
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.triggers.cron import CronTrigger

from config import ResolvedConfig, load_configs, sqlite_url
import jobs as job_queue

logger = logging.getLogger("dataflow.scheduler")

_scheduler: "BackgroundScheduler | None" = None


def _enqueue_scheduled(pipeline_name: str, max_attempts: int):
    """Cron callback: enqueue a job for the worker to run."""
    job_id = job_queue.enqueue(pipeline_name, trigger="schedule", max_attempts=max_attempts)
    if job_id is None:
        logger.info("Skipped enqueue for '%s' (already queued/running)", pipeline_name)
    else:
        logger.info("Enqueued scheduled job %d for '%s'", job_id, pipeline_name)


def _prune_audit_logs_job():
    """Periodic callback: enforce the audit-log retention window."""
    try:
        from config import prune_audit_logs
        from settings import settings as app_settings
        days = app_settings.audit_retention_days
        n = prune_audit_logs(days)
        if n:
            logger.info("Audit retention: pruned %d row(s) older than %d days", n, days)
    except Exception as e:  # noqa: BLE001
        logger.warning("Audit retention prune failed: %s", e)


def start_scheduler(resolved_config: ResolvedConfig) -> BackgroundScheduler:
    """Build and start the scheduler (non-blocking). Returns the scheduler."""
    global _scheduler
    if _scheduler is not None:
        return _scheduler

    jobstores = {"default": SQLAlchemyJobStore(url=sqlite_url)}
    scheduler = BackgroundScheduler(jobstores=jobstores)

    active_job_ids = set()
    for cronjob in resolved_config.cronjobs.values():
        if not cronjob.enabled:
            continue
        active_job_ids.add(cronjob.name)
        scheduler.add_job(
            _enqueue_scheduled,
            CronTrigger.from_crontab(cronjob.schedule, timezone=ZoneInfo(cronjob.timezone)),
            args=[cronjob.pipeline, cronjob.retry.max_attempts],
            id=cronjob.name,
            replace_existing=True,
            misfire_grace_time=3600,
        )
        logger.info("Scheduled '%s' -> pipeline '%s' (%s %s)",
                    cronjob.name, cronjob.pipeline, cronjob.schedule, cronjob.timezone)

    # Drop persisted jobs that are no longer configured/enabled.
    try:
        for job in scheduler.get_jobs():
            if job.id not in active_job_ids and job.id != "_audit_retention":
                scheduler.remove_job(job.id)
    except Exception as e:
        logger.warning("Failed to prune deprecated scheduler jobs: %s", e)

    # Audit-log retention: prune daily, and once shortly after startup.
    scheduler.add_job(
        _prune_audit_logs_job,
        "interval",
        hours=24,
        id="_audit_retention",
        replace_existing=True,
        misfire_grace_time=3600,
        next_run_time=datetime.now(ZoneInfo("UTC")),
    )

    scheduler.start()
    _scheduler = scheduler
    logger.info("Scheduler started with %d job(s)", len(scheduler.get_jobs()))
    return scheduler
