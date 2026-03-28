from typing import Optional, List, Dict, Any
from sqlmodel import SQLModel, Field, JSON, Column
from .models import NAME_REGEX

class Source(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True, unique=True, regex=NAME_REGEX)
    type: str
    config: Dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))

class Sink(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True, unique=True, regex=NAME_REGEX)
    type: str
    config: Dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))

class Pipeline(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True, unique=True, regex=NAME_REGEX)
    description: Optional[str] = None
    source: str
    source_query: str
    sink: str
    sink_table: str
    sink_mode: str
    sink_key: Optional[str] = None
    transforms: List[Dict[str, Any]] = Field(default_factory=list, sa_column=Column(JSON))
    alerts: Dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    batch_size: Optional[int] = None

class CronJob(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True, unique=True, regex=NAME_REGEX)
    pipeline: str
    schedule: str
    timezone: str = "UTC"
    enabled: bool = True
    retry: Dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
