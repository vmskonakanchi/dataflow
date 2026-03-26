import json
import os
from dataclasses import dataclass
from typing import Dict, Any, List
from .models import (
    SourceConfig, SinkConfig, PipelineConfig, CronJobConfig,
    TransformJoin
)
from pydantic import ValidationError

from pydantic import TypeAdapter

class ConfigError(Exception):
    """Custom exception for configuration errors."""
    pass

@dataclass
class ResolvedConfig:
    sources: Dict[str, SourceConfig]
    sinks: Dict[str, SinkConfig]
    pipelines: Dict[str, PipelineConfig]
    cronjobs: Dict[str, CronJobConfig]

def load_configs(config_dir: str) -> ResolvedConfig:
    sources_path = os.path.join(config_dir, "sources.json")
    sinks_path = os.path.join(config_dir, "sinks.json")
    pipelines_path = os.path.join(config_dir, "pipelines.json")
    cronjobs_path = os.path.join(config_dir, "cronjobs.json")

    def load_file(path: str, model_type: Any, file_name: str) -> Any:
        if not os.path.exists(path):
            raise ConfigError(f"{file_name} not found in {config_dir}")
        try:
            with open(path, "r") as f:
                data = json.load(f)
            adapter = TypeAdapter(model_type)
            return adapter.validate_python(data)
        except json.JSONDecodeError as e:
            raise ConfigError(f"{file_name} is not a valid JSON: {str(e)}")
        except ValidationError as e:
            errors = e.errors()
            if errors:
                err = errors[0]
                loc = " -> ".join(str(l) for l in err['loc'])
                msg = err['msg']
                input_val = err.get('input', 'unknown')
                raise ConfigError(f"{file_name} — {loc}: {msg} (value: {input_val})")
            raise ConfigError(f"{file_name} — Validation error: {str(e)}")

    sources_list = load_file(sources_path, List[SourceConfig], "sources.json")
    sinks_list = load_file(sinks_path, List[SinkConfig], "sinks.json")
    pipelines_list = load_file(pipelines_path, List[PipelineConfig], "pipelines.json")
    cronjobs_list = load_file(cronjobs_path, List[CronJobConfig], "cronjobs.json")

    # 1. Check uniqueness and build dictionaries    
    sources_dict: Dict[str, SourceConfig] = {}
    for s in sources_list:
        if s.name in sources_dict:
            raise ConfigError(f"sources.json — duplicate source name found: {s.name}")
        sources_dict[s.name] = s

    sinks_dict: Dict[str, SinkConfig] = {}
    for s in sinks_list:
        if s.name in sinks_dict:
            raise ConfigError(f"sinks.json — duplicate sink name found: {s.name}")
        sinks_dict[s.name] = s

    pipelines_dict: Dict[str, PipelineConfig] = {}
    for p in pipelines_list:
        if p.name in pipelines_dict:
            raise ConfigError(f"pipelines.json — duplicate pipeline name found: {p.name}")
        pipelines_dict[p.name] = p

    cronjobs_dict: Dict[str, CronJobConfig] = {}
    for c in cronjobs_list:
        if c.name in cronjobs_dict:
            raise ConfigError(f"cronjobs.json — duplicate cronjob name found: {c.name}")
        cronjobs_dict[c.name] = c

    # 2. Cross-validate references
    for p_name, p in pipelines_dict.items():
        if p.source not in sources_dict:
            raise ConfigError(f"pipelines.json — pipeline '{p_name}': source '{p.source}' not found in sources.json")
        if p.sink not in sinks_dict:
            raise ConfigError(f"pipelines.json — pipeline '{p_name}': sink '{p.sink}' not found in sinks.json")
        
        for t in p.transforms:
            if isinstance(t, TransformJoin):
                if t.right_source not in sources_dict:
                    raise ConfigError(f"pipelines.json — pipeline '{p_name}': join transform right_source '{t.right_source}' not found in sources.json")

    for c_name, c in cronjobs_dict.items():
        if c.pipeline not in pipelines_dict:
            raise ConfigError(f"cronjobs.json — cronjob '{c_name}': pipeline '{c.pipeline}' not found in pipelines.json")

    return ResolvedConfig(
        sources=sources_dict,
        sinks=sinks_dict,
        pipelines=pipelines_dict,
        cronjobs=cronjobs_dict
    )
