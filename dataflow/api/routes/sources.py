from fastapi import APIRouter, HTTPException, Depends
from typing import List, Dict, Any
from sqlmodel import Session, select
from ...config.database import get_session
from ...config.db_models import Source
from ...config.models import SourceConfig
from pydantic import TypeAdapter

router = APIRouter(prefix="/sources", tags=["Sources"])
source_adapter = TypeAdapter(SourceConfig)

@router.get("")
def list_sources(session: Session = Depends(get_session)):
    sources = session.exec(select(Source)).all()
    return [{"name": s.name, "type": s.type, **s.config} for s in sources]

@router.post("")
def add_source(source_data: Dict[str, Any], session: Session = Depends(get_session)):
    try:
        # Validate against Pydantic model
        source_config = source_adapter.validate_python(source_data)
        
        # Check if already exists
        statement = select(Source).where(Source.name == source_config.name)
        if session.exec(statement).first():
            raise HTTPException(status_code=400, detail="Source name already exists")
        
        # Create DB model
        data = source_config.model_dump(by_alias=True)
        name = data.pop("name")
        type_str = data.pop("type")
        
        db_source = Source(name=name, type=type_str, config=data)
        session.add(db_source)
        session.commit()
        return {"status": "success"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.put("/{name}")
def update_source(name: str, source_data: Dict[str, Any], session: Session = Depends(get_session)):
    try:
        source_config = source_adapter.validate_python(source_data)
        
        statement = select(Source).where(Source.name == name)
        db_source = session.exec(statement).first()
        if not db_source:
            raise HTTPException(status_code=404, detail="Source not found")
        
        data = source_config.model_dump(by_alias=True)
        db_source.name = data.pop("name")
        db_source.type = data.pop("type")
        db_source.config = data
        
        session.add(db_source)
        session.commit()
        return {"status": "success"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.delete("/{name}")
def delete_source(name: str, session: Session = Depends(get_session)):
    try:
        statement = select(Source).where(Source.name == name)
        db_source = session.exec(statement).first()
        if not db_source:
            raise HTTPException(status_code=404, detail="Source not found")
        
        session.delete(db_source)
        session.commit()
        return {"status": "success"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
