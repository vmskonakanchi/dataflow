"""Pipeline run history logging — backed by SQLModel (config database)."""

from datetime import datetime
from typing import List, Dict, Optional

from sqlmodel import Session, select
from config import engine, PipelineRun


def log_run_start(pipeline_name: str, job_name: Optional[str] = None) -> int:
    with Session(engine) as session:
        run = PipelineRun(
            pipeline_name=pipeline_name,
            job_name=job_name,
            status="started",
            started_at=datetime.utcnow(),
        )
        session.add(run)
        session.commit()
        session.refresh(run)
        return run.id


def log_run_success(run_id: int, rows_extracted: int, rows_written: int) -> None:
    with Session(engine) as session:
        run = session.get(PipelineRun, run_id)
        if run:
            run.status = "success"
            run.finished_at = datetime.utcnow()
            run.rows_extracted = rows_extracted
            run.rows_written = rows_written
            session.add(run)
            session.commit()


def log_run_failure(run_id: int, error_message: str) -> None:
    with Session(engine) as session:
        run = session.get(PipelineRun, run_id)
        if run:
            run.status = "failed"
            run.finished_at = datetime.utcnow()
            run.error_message = error_message
            session.add(run)
            session.commit()


def get_last_successful_run(pipeline_name: str) -> Optional[datetime]:
    with Session(engine) as session:
        run = session.exec(
            select(PipelineRun)
            .where(PipelineRun.pipeline_name == pipeline_name)
            .where(PipelineRun.status == "success")
            .order_by(PipelineRun.finished_at.desc())
        ).first()
        return run.finished_at if run else None


def get_run_history(pipeline_name: str, limit: int = 10) -> List[Dict]:
    with Session(engine) as session:
        runs = session.exec(
            select(PipelineRun)
            .where(PipelineRun.pipeline_name == pipeline_name)
            .order_by(PipelineRun.started_at.desc())
            .limit(limit)
        ).all()
        return [r.model_dump() for r in runs]
