"""Durable job queue backed by SQLite (via SQLModel).

Enqueue inserts a row; a worker atomically claims queued jobs, heartbeats while
running, and marks them done. Crashed jobs (stale heartbeat) are recovered by
reconcile_stale(). No external broker — the database is the queue.
"""

import os
from datetime import datetime, timedelta
from typing import Optional, List

from sqlalchemy import update as sa_update
from sqlmodel import Session, select

from config import engine, Job

# Terminal + active status sets
ACTIVE_STATUSES = ("queued", "running")


def enqueue(pipeline: str, trigger: str = "manual", max_attempts: int = 1,
            dedupe: bool = True) -> Optional[int]:
    """Add a job to the queue. Returns the job id, or None if deduped
    (a job for this pipeline is already queued/running)."""
    with Session(engine) as session:
        if dedupe:
            existing = session.exec(
                select(Job).where(Job.pipeline == pipeline, Job.status.in_(ACTIVE_STATUSES))
            ).first()
            if existing is not None:
                return None
        job = Job(
            pipeline=pipeline,
            status="queued",
            trigger=trigger,
            max_attempts=max_attempts,
            enqueued_at=datetime.utcnow(),
            run_after=datetime.utcnow(),
        )
        session.add(job)
        session.commit()
        session.refresh(job)
        return job.id


def claim_next(worker_pid: Optional[int] = None) -> Optional[Job]:
    """Atomically claim the next runnable job. Returns the claimed Job or None.

    Uses a guarded UPDATE (status must still be 'queued') with a rowcount check,
    so multiple workers never claim the same job.
    """
    if worker_pid is None:
        worker_pid = os.getpid()
    now = datetime.utcnow()

    with Session(engine) as session:
        candidates = session.exec(
            select(Job)
            .where(Job.status == "queued", Job.run_after <= now)
            .order_by(Job.enqueued_at)
            .limit(5)
        ).all()

        for cand in candidates:
            result = session.execute(
                sa_update(Job)
                .where(Job.id == cand.id, Job.status == "queued")
                .values(
                    status="running",
                    worker_pid=worker_pid,
                    started_at=now,
                    heartbeat_at=now,
                    attempts=Job.attempts + 1,
                )
            )
            session.commit()
            if result.rowcount == 1:
                return session.get(Job, cand.id)
        return None


def heartbeat(job_id: int) -> None:
    """Update a running job's heartbeat timestamp."""
    with Session(engine) as session:
        session.execute(
            sa_update(Job).where(Job.id == job_id).values(heartbeat_at=datetime.utcnow())
        )
        session.commit()


def finish(job_id: int, status: str, error_message: Optional[str] = None,
           retry_delay_seconds: int = 60) -> None:
    """Mark a job finished. On failure, requeue with backoff if attempts remain."""
    now = datetime.utcnow()
    with Session(engine) as session:
        job = session.get(Job, job_id)
        if job is None:
            return
        if status == "failed" and job.attempts < job.max_attempts:
            job.status = "queued"
            job.trigger = "retry"
            job.run_after = now + timedelta(seconds=retry_delay_seconds)
            job.heartbeat_at = None
            job.worker_pid = None
            job.error_message = error_message
        else:
            job.status = status
            job.finished_at = now
            job.error_message = error_message
        session.add(job)
        session.commit()


def reconcile_stale(stale_seconds: int = 60) -> int:
    """Recover jobs left 'running' by a crashed worker (stale heartbeat).
    Requeues them if attempts remain, else marks failed. Returns count recovered."""
    cutoff = datetime.utcnow() - timedelta(seconds=stale_seconds)
    now = datetime.utcnow()
    recovered = 0
    with Session(engine) as session:
        stale = session.exec(
            select(Job).where(
                Job.status == "running",
                Job.heartbeat_at.is_not(None),
                Job.heartbeat_at < cutoff,
            )
        ).all()
        # Also catch running jobs that never heartbeated but started long ago.
        never_beat = session.exec(
            select(Job).where(
                Job.status == "running",
                Job.heartbeat_at.is_(None),
                Job.started_at.is_not(None),
                Job.started_at < cutoff,
            )
        ).all()

        for job in list(stale) + list(never_beat):
            if job.attempts < job.max_attempts:
                job.status = "queued"
                job.trigger = "retry"
                job.run_after = now
                job.heartbeat_at = None
                job.worker_pid = None
                job.error_message = "Recovered after crash (stale heartbeat)"
            else:
                job.status = "failed"
                job.finished_at = now
                job.error_message = "Failed after crash (stale heartbeat, no attempts left)"
            session.add(job)
            recovered += 1
        session.commit()
    return recovered


def is_pipeline_active(pipeline: str) -> bool:
    """True if the pipeline has a queued or running job."""
    with Session(engine) as session:
        job = session.exec(
            select(Job).where(Job.pipeline == pipeline, Job.status.in_(ACTIVE_STATUSES))
        ).first()
        return job is not None


def recent_jobs(limit: int = 50) -> List[Job]:
    """Most recently enqueued jobs (for observability)."""
    with Session(engine) as session:
        return session.exec(
            select(Job).order_by(Job.enqueued_at.desc()).limit(limit)
        ).all()
