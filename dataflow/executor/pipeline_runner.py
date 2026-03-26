from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from ..config.loader import ResolvedConfig
from ..config.models import (
    TransformFilter, TransformRename, TransformAggregate, TransformJoin
)
from ..connectors.base import BaseConnector
from ..transforms.base import BaseTransform
# Import all transforms to ensure they are registered
from ..transforms import filter, rename, aggregate, join
# Import all connectors to ensure they are registered
from ..connectors import postgres, mysql, csv_connector, rest_api, duckdb_connector, s3_connector
from ..logger.run_log import log_run_start, log_run_success, log_run_failure
from ..alerts.email_alert import send_failure_alert, send_row_count_alert

@dataclass
class RunResult:
    pipeline_name: str
    status: str
    started_at: datetime
    finished_at: datetime
    rows_extracted: int
    rows_written: int
    error_message: Optional[str] = None

class PipelineError(Exception):
    def __init__(self, pipeline_name: str, step: str, message: str):
        self.pipeline_name = pipeline_name
        self.step = step
        super().__init__(f"Pipeline '{pipeline_name}' failed at step '{step}': {message}")

def run_pipeline(pipeline_name: str, resolved_config: ResolvedConfig) -> RunResult:
    started_at = datetime.utcnow()
    run_id = log_run_start(pipeline_name)
    
    pipeline = resolved_config.pipelines.get(pipeline_name)
    if not pipeline:
        err_msg = f"Pipeline '{pipeline_name}' not found in resolved config"
        log_run_failure(run_id, err_msg)
        raise PipelineError(pipeline_name, "init", err_msg)

    source_config = resolved_config.sources[pipeline.source]
    sink_config = resolved_config.sinks[pipeline.sink]
    
    rows_extracted = 0
    rows_written = 0
    
    try:
        # 1. Extract
        step = "extract"
        connector = BaseConnector.get(source_config.type)
        df = connector.extract(source_config, pipeline.source_query, pipeline_name)
        rows_extracted = len(df)
        
        # 2. Transform
        step = "transform"
        for t_config in pipeline.transforms:
            # Instantiate transform with config
            transform_class = BaseTransform.get(t_config.type)
            # We need to map the pydantic config to the constructor args
            if t_config.type == "filter":
                transform = transform_class(condition=t_config.condition)
            elif t_config.type == "rename":
                transform = transform_class(from_col=t_config.from_col, to_col=t_config.to_col)
            elif t_config.type == "aggregate":
                transform = transform_class(group_by=t_config.group_by, agg=t_config.agg)
            elif t_config.type == "join":
                transform = transform_class(
                    right_source=t_config.right_source,
                    right_query=t_config.right_query,
                    join_type=t_config.join_type,
                    on=t_config.on
                )
            else:
                raise ValueError(f"Unknown transform type: {t_config.type}")
            
            df = transform.apply(df, resolved_config, pipeline_name)
            
        # 3. Load
        step = "load"
        sink_connector = BaseConnector.get(sink_config.type)
        rows_written = sink_connector.load(
            df, sink_config, pipeline.sink_table, pipeline.sink_mode, pipeline.sink_key
        )
        
        # 4. Success
        log_run_success(run_id, rows_extracted, rows_written)
        
        # 5. Row count alert
        if pipeline.alerts.on_row_count_below and rows_written < pipeline.alerts.on_row_count_below:
            if pipeline.alerts.email:
                send_row_count_alert(
                    pipeline_name, rows_written, pipeline.alerts.on_row_count_below, pipeline.alerts.email
                )
        
        return RunResult(
            pipeline_name=pipeline_name,
            status="success",
            started_at=started_at,
            finished_at=datetime.utcnow(),
            rows_extracted=rows_extracted,
            rows_written=rows_written
        )

    except Exception as e:
        error_message = str(e)
        log_run_failure(run_id, error_message)
        
        if pipeline.alerts.on_failure == "email" and pipeline.alerts.email:
            send_failure_alert(pipeline_name, error_message, pipeline.alerts.email)
            
        raise PipelineError(pipeline_name, step, error_message) from e
