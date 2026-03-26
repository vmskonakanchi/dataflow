from typing import List, Dict, Optional, Literal, Union, Annotated
from pydantic import BaseModel, Field, field_validator, model_validator, StringConstraints
import re

# Regex for names: lowercase, underscores only, no spaces, no special chars
NAME_REGEX = r"^[a-z][a-z0-9_]*$"
NameString = Annotated[str, StringConstraints(pattern=NAME_REGEX)]

class SourceConfigBase(BaseModel):
    name: NameString
    type: str

class PostgresSourceConfig(SourceConfigBase):
    type: Literal["postgres"]
    host: str
    port: Annotated[int, Field(ge=1, le=65535)]
    database: str
    username: str
    password: str
    schema_name: str = Field(default="public", alias="schema")

class MySQLSourceConfig(SourceConfigBase):
    type: Literal["mysql"]
    host: str
    port: Annotated[int, Field(ge=1, le=65535)]
    database: str
    username: str
    password: str

class CSVSourceConfig(SourceConfigBase):
    type: Literal["csv"]
    file_path: str
    delimiter: str = ","
    has_header: bool = True

class RestApiSourceConfig(SourceConfigBase):
    type: Literal["rest_api"]
    url: str
    method: Literal["GET", "POST"]
    headers: Optional[Dict[str, str]] = None
    auth_type: Optional[Literal["none", "bearer", "basic"]] = "none"
    auth_token: Optional[str] = None

SourceConfig = Annotated[
    Union[PostgresSourceConfig, MySQLSourceConfig, CSVSourceConfig, RestApiSourceConfig],
    Field(discriminator="type")
]

class SinkConfigBase(BaseModel):
    name: NameString
    type: str

class PostgresSinkConfig(SinkConfigBase):
    type: Literal["postgres"]
    host: str
    port: Annotated[int, Field(ge=1, le=65535)]
    database: str
    username: str
    password: str
    schema_name: str = Field(default="public", alias="schema")

class DuckDBSinkConfig(SinkConfigBase):
    type: Literal["duckdb"]
    file_path: str

class S3SinkConfig(SinkConfigBase):
    type: Literal["s3"]
    bucket: str
    prefix: str = ""
    region: str
    access_key: str
    secret_key: str
    file_format: Literal["parquet", "csv", "json"]

SinkConfig = Annotated[
    Union[PostgresSinkConfig, DuckDBSinkConfig, S3SinkConfig],
    Field(discriminator="type")
]

class TransformFilter(BaseModel):
    type: Literal["filter"]
    condition: str

class TransformRename(BaseModel):
    type: Literal["rename"]
    from_col: str = Field(alias="from")
    to_col: str = Field(alias="to")

class TransformAggregate(BaseModel):
    type: Literal["aggregate"]
    group_by: List[str]
    agg: Dict[str, Literal["SUM", "COUNT", "AVG", "MIN", "MAX"]]

class TransformJoin(BaseModel):
    type: Literal["join"]
    right_source: str
    right_query: str
    join_type: Literal["inner", "left", "right"]
    on: str

TransformConfig = Annotated[
    Union[TransformFilter, TransformRename, TransformAggregate, TransformJoin],
    Field(discriminator="type")
]

class AlertConfig(BaseModel):
    on_failure: Literal["email", "none"]
    email: Optional[str] = None
    on_row_count_below: Optional[int] = Field(default=None, ge=1)

    @model_validator(mode="after")
    def validate_email_required(self) -> 'AlertConfig':
        if self.on_failure == "email" and not self.email:
            raise ValueError("email is required when on_failure is email")
        return self

class PipelineConfig(BaseModel):
    name: NameString
    description: Optional[str] = None
    source: str
    source_query: str
    sink: str
    sink_table: str
    sink_mode: Literal["append", "upsert", "replace"]
    sink_key: Optional[str] = None
    transforms: List[TransformConfig] = []
    alerts: AlertConfig

    @model_validator(mode="after")
    def validate_sink_key(self) -> 'PipelineConfig':
        if self.sink_mode == "upsert" and not self.sink_key:
            raise ValueError("sink_key is required when sink_mode is upsert")
        return self

class RetryConfig(BaseModel):
    max_attempts: int = Field(default=3, ge=1, le=10)
    delay_seconds: int = Field(default=60, ge=10, le=3600)

class CronJobConfig(BaseModel):
    name: NameString
    pipeline: str
    schedule: str
    timezone: str = "UTC"
    enabled: bool
    retry: RetryConfig

    @field_validator("schedule")
    @classmethod
    def validate_cron(cls, v: str) -> str:
        parts = v.split()
        if len(parts) != 5:
            raise ValueError("schedule must be a valid 5-part cron expression")
        return v

# Remove SourcesFile, SinksFile, PipelinesFile, CronJobsFile as we will validate lists directly
