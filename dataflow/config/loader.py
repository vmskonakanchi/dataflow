from typing import Dict, Any, List
from sqlmodel import Session, select
from .database import engine, init_db
from .db_models import Source, Sink, Pipeline, CronJob
from .models import (
    SourceConfig, SinkConfig, PipelineConfig, CronJobConfig,
    TransformJoin
)
from pydantic import TypeAdapter
from dataclasses import dataclass

class ConfigError(Exception):
    """Custom exception for configuration errors."""
    pass

@dataclass
class ResolvedConfig:
    sources: Dict[str, SourceConfig]
    sinks: Dict[str, SinkConfig]
    pipelines: Dict[str, PipelineConfig]
    cronjobs: Dict[str, CronJobConfig]

def load_configs(config_dir: str = None) -> ResolvedConfig:
    """Load configuration from the SQLite database."""
    # Ensure tables exist
    init_db()
    
    with Session(engine) as session:
        # Load Sources
        sources_list = session.exec(select(Source)).all()
        sources_dict: Dict[str, SourceConfig] = {}
        source_adapter = TypeAdapter(SourceConfig)
        for s in sources_list:
            data = {"name": s.name, "type": s.type, **s.config}
            sources_dict[s.name] = source_adapter.validate_python(data)

        # Load Sinks
        sinks_list = session.exec(select(Sink)).all()
        sinks_dict: Dict[str, SinkConfig] = {}
        sink_adapter = TypeAdapter(SinkConfig)
        for s in sinks_list:
            data = {"name": s.name, "type": s.type, **s.config}
            sinks_dict[s.name] = sink_adapter.validate_python(data)

        # Load Pipelines
        pipelines_list = session.exec(select(Pipeline)).all()
        pipelines_dict: Dict[str, PipelineConfig] = {}
        pipeline_adapter = TypeAdapter(PipelineConfig)
        for p in pipelines_list:
            data = {
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
            pipelines_dict[p.name] = pipeline_adapter.validate_python(data)

        # Load CronJobs
        cronjobs_list = session.exec(select(CronJob)).all()
        cronjobs_dict: Dict[str, CronJobConfig] = {}
        cronjob_adapter = TypeAdapter(CronJobConfig)
        for c in cronjobs_list:
            data = {
                "name": c.name,
                "pipeline": c.pipeline,
                "schedule": c.schedule,
                "timezone": c.timezone,
                "enabled": c.enabled,
                "retry": c.retry
            }
            cronjobs_dict[c.name] = cronjob_adapter.validate_python(data)

    # Cross-validate references
    for p_name, p in pipelines_dict.items():
        if p.source not in sources_dict:
            raise ConfigError(f"Database — pipeline '{p_name}': source '{p.source}' not found")
        if p.sink not in sinks_dict:
            raise ConfigError(f"Database — pipeline '{p_name}': sink '{p.sink}' not found")
        
        for t in p.transforms:
            if isinstance(t, TransformJoin):
                if t.right_source not in sources_dict:
                    raise ConfigError(f"Database — pipeline '{p_name}': join transform right_source '{t.right_source}' not found")

    for c_name, c in cronjobs_dict.items():
        if c.pipeline not in pipelines_dict:
            raise ConfigError(f"Database — cronjob '{c_name}': pipeline '{c.pipeline}' not found")

    return ResolvedConfig(
        sources=sources_dict,
        sinks=sinks_dict,
        pipelines=pipelines_dict,
        cronjobs=cronjobs_dict
    )
