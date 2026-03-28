from fastapi import APIRouter, HTTPException, Depends
from typing import List, Dict, Any
from sqlmodel import Session, select
from ...config.database import get_session
from ...config.db_models import Pipeline
from ...config.models import PipelineConfig
from ...executor.pipeline_runner import run_pipeline
from ...config.loader import load_configs
from ...logger.run_log import get_run_history
from pydantic import TypeAdapter

router = APIRouter(prefix="/pipelines", tags=["Pipelines"])
pipeline_adapter = TypeAdapter(PipelineConfig)

@router.get("")
def list_pipelines(session: Session = Depends(get_session)):
    pipelines = session.exec(select(Pipeline)).all()
    return [{
        "name": p.name,
        "description": p.description,
        "source": p.source,
        "source_query": p.source_query,
        "sink": p.sink,
        "sink_table": p.sink_table,
        "sink_mode": p.sink_mode,
        "sink_key": p.sink_key,
        "transforms": p.transforms,
        "alerts": p.alerts,
        "batch_size": p.batch_size
    } for p in pipelines]

@router.get("/{name}")
def get_pipeline(name: str, session: Session = Depends(get_session)):
    statement = select(Pipeline).where(Pipeline.name == name)
    p = session.exec(statement).first()
    if not p:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    return {
        "name": p.name,
        "description": p.description,
        "source": p.source,
        "source_query": p.source_query,
        "sink": p.sink,
        "sink_table": p.sink_table,
        "sink_mode": p.sink_mode,
        "sink_key": p.sink_key,
        "transforms": p.transforms,
        "alerts": p.alerts,
        "batch_size": p.batch_size
    }

@router.post("")
def add_pipeline(pipeline_data: Dict[str, Any], session: Session = Depends(get_session)):
    try:
        pipeline_config = pipeline_adapter.validate_python(pipeline_data)
        
        statement = select(Pipeline).where(Pipeline.name == pipeline_config.name)
        if session.exec(statement).first():
            raise HTTPException(status_code=400, detail="Pipeline name already exists")
        
        db_pipeline = Pipeline(
            name=pipeline_config.name,
            description=pipeline_config.description,
            source=pipeline_config.source,
            source_query=pipeline_config.source_query,
            sink=pipeline_config.sink,
            sink_table=pipeline_config.sink_table,
            sink_mode=pipeline_config.sink_mode,
            sink_key=pipeline_config.sink_key,
            transforms=[t.model_dump(by_alias=True) for t in pipeline_config.transforms],
            alerts=pipeline_config.alerts.model_dump(),
            batch_size=pipeline_config.batch_size
        )
        session.add(db_pipeline)
        session.commit()
        return {"status": "success"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.put("/{name}")
def update_pipeline(name: str, pipeline_data: Dict[str, Any], session: Session = Depends(get_session)):
    try:
        pipeline_config = pipeline_adapter.validate_python(pipeline_data)
        
        statement = select(Pipeline).where(Pipeline.name == name)
        db_pipeline = session.exec(statement).first()
        if not db_pipeline:
            raise HTTPException(status_code=404, detail="Pipeline not found")
        
        db_pipeline.name = pipeline_config.name
        db_pipeline.description = pipeline_config.description
        db_pipeline.source = pipeline_config.source
        db_pipeline.source_query = pipeline_config.source_query
        db_pipeline.sink = pipeline_config.sink
        db_pipeline.sink_table = pipeline_config.sink_table
        db_pipeline.sink_mode = pipeline_config.sink_mode
        db_pipeline.sink_key = pipeline_config.sink_key
        db_pipeline.transforms = [t.model_dump(by_alias=True) for t in pipeline_config.transforms]
        db_pipeline.alerts = pipeline_config.alerts.model_dump()
        db_pipeline.batch_size = pipeline_config.batch_size
        
        session.add(db_pipeline)
        session.commit()
        return {"status": "success"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.delete("/{name}")
def delete_pipeline(name: str, session: Session = Depends(get_session)):
    try:
        statement = select(Pipeline).where(Pipeline.name == name)
        db_pipeline = session.exec(statement).first()
        if not db_pipeline:
            raise HTTPException(status_code=404, detail="Pipeline not found")
        
        session.delete(db_pipeline)
        session.commit()
        return {"status": "success"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/{name}/run")
def trigger_pipeline(name: str):
    """Trigger a pipeline manually."""
    try:
        resolved = load_configs()
        result = run_pipeline(name, resolved)
        return {
            "status": "success",
            "rows_extracted": result.rows_extracted,
            "rows_written": result.rows_written,
            "duration": (result.finished_at - result.started_at).total_seconds()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{name}/history")
def pipeline_history(name: str):
    """Get the run history for a specific pipeline."""
    try:
        history = get_run_history(name, limit=50)
        return history
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
