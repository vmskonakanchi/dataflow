from fastapi import APIRouter, HTTPException, Depends
from typing import List, Dict, Any
from sqlmodel import Session, select
from ...config.database import get_session
from ...config.db_models import CronJob
from ...config.models import CronJobConfig
from pydantic import TypeAdapter

router = APIRouter(prefix="/cronjobs", tags=["CronJobs"])
cronjob_adapter = TypeAdapter(CronJobConfig)

@router.get("")
def list_cronjobs(session: Session = Depends(get_session)):
    cronjobs = session.exec(select(CronJob)).all()
    return [{
        "name": c.name,
        "pipeline": c.pipeline,
        "schedule": c.schedule,
        "timezone": c.timezone,
        "enabled": c.enabled,
        "retry": c.retry
    } for c in cronjobs]

@router.post("")
def add_cronjob(cronjob_data: Dict[str, Any], session: Session = Depends(get_session)):
    try:
        cronjob_config = cronjob_adapter.validate_python(cronjob_data)
        
        statement = select(CronJob).where(CronJob.name == cronjob_config.name)
        if session.exec(statement).first():
            raise HTTPException(status_code=400, detail="CronJob name already exists")
        
        db_cronjob = CronJob(
            name=cronjob_config.name,
            pipeline=cronjob_config.pipeline,
            schedule=cronjob_config.schedule,
            timezone=cronjob_config.timezone,
            enabled=cronjob_config.enabled,
            retry=cronjob_config.retry.model_dump()
        )
        session.add(db_cronjob)
        session.commit()
        return {"status": "success"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.put("/{name}")
def update_cronjob(name: str, cronjob_data: Dict[str, Any], session: Session = Depends(get_session)):
    try:
        cronjob_config = cronjob_adapter.validate_python(cronjob_data)
        
        statement = select(CronJob).where(CronJob.name == name)
        db_cronjob = session.exec(statement).first()
        if not db_cronjob:
            raise HTTPException(status_code=404, detail="CronJob not found")
        
        db_cronjob.name = cronjob_config.name
        db_cronjob.pipeline = cronjob_config.pipeline
        db_cronjob.schedule = cronjob_config.schedule
        db_cronjob.timezone = cronjob_config.timezone
        db_cronjob.enabled = cronjob_config.enabled
        db_cronjob.retry = cronjob_config.retry.model_dump()
        
        session.add(db_cronjob)
        session.commit()
        return {"status": "success"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.delete("/{name}")
def delete_cronjob(name: str, session: Session = Depends(get_session)):
    try:
        statement = select(CronJob).where(CronJob.name == name)
        db_cronjob = session.exec(statement).first()
        if not db_cronjob:
            raise HTTPException(status_code=404, detail="CronJob not found")
        
        session.delete(db_cronjob)
        session.commit()
        return {"status": "success"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
