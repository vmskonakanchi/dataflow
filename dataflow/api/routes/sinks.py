from fastapi import APIRouter, HTTPException, Depends
from typing import List, Dict, Any
from sqlmodel import Session, select
from ...config.database import get_session
from ...config.db_models import Sink
from ...config.models import SinkConfig
from pydantic import TypeAdapter

router = APIRouter(prefix="/sinks", tags=["Sinks"])
sink_adapter = TypeAdapter(SinkConfig)

@router.get("")
def list_sinks(session: Session = Depends(get_session)):
    sinks = session.exec(select(Sink)).all()
    return [{"name": s.name, "type": s.type, **s.config} for s in sinks]

@router.post("")
def add_sink(sink_data: Dict[str, Any], session: Session = Depends(get_session)):
    try:
        sink_config = sink_adapter.validate_python(sink_data)
        
        statement = select(Sink).where(Sink.name == sink_config.name)
        if session.exec(statement).first():
            raise HTTPException(status_code=400, detail="Sink name already exists")
        
        data = sink_config.model_dump(by_alias=True)
        name = data.pop("name")
        type_str = data.pop("type")
        
        db_sink = Sink(name=name, type=type_str, config=data)
        session.add(db_sink)
        session.commit()
        return {"status": "success"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.put("/{name}")
def update_sink(name: str, sink_data: Dict[str, Any], session: Session = Depends(get_session)):
    try:
        sink_config = sink_adapter.validate_python(sink_data)
        
        statement = select(Sink).where(Sink.name == name)
        db_sink = session.exec(statement).first()
        if not db_sink:
            raise HTTPException(status_code=404, detail="Sink not found")
        
        data = sink_config.model_dump(by_alias=True)
        db_sink.name = data.pop("name")
        db_sink.type = data.pop("type")
        db_sink.config = data
        
        session.add(db_sink)
        session.commit()
        return {"status": "success"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.delete("/{name}")
def delete_sink(name: str, session: Session = Depends(get_session)):
    try:
        statement = select(Sink).where(Sink.name == name)
        db_sink = session.exec(statement).first()
        if not db_sink:
            raise HTTPException(status_code=404, detail="Sink not found")
        
        session.delete(db_sink)
        session.commit()
        return {"status": "success"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
