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

class LocalFileSourceConfig(SourceConfigBase):
    type: Literal["local_file"]
    file_path: str
    file_format: Literal["parquet", "csv", "json"] = "parquet"
    delimiter: str = ","       # used when file_format is csv
    has_header: bool = True    # used when file_format is csv

class S3SourceConfig(SourceConfigBase):
    type: Literal["s3"]
    bucket: str
    key: str
    region: str = "us-east-1"
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    file_format: Literal["parquet", "csv", "json"] = "parquet"
    public: bool = False  # set true for public requester-pays buckets (adds RequestPayer='requester')

class DuckDBSourceConfig(SourceConfigBase):
    type: Literal["duckdb"]
    file_path: str

SourceConfig = Annotated[
    Union[PostgresSourceConfig, MySQLSourceConfig, CSVSourceConfig, LocalFileSourceConfig, S3SourceConfig, DuckDBSourceConfig],
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

class MySQLSinkConfig(SinkConfigBase):
    type: Literal["mysql"]
    host: str
    port: Annotated[int, Field(ge=1, le=65535)] = 3306
    database: str
    username: str
    password: str

class CSVSinkConfig(SinkConfigBase):
    type: Literal["csv"]
    file_path: str
    delimiter: str = ","
    mode: Literal["replace", "append"] = "replace"  # replace overwrites, append adds rows
class S3SinkConfig(SinkConfigBase):
    type: Literal["s3"]
    bucket: str
    prefix: str = ""
    region: str
    access_key: str
    secret_key: str
    file_format: Literal["parquet", "csv", "json"]

class LocalFileSinkConfig(SinkConfigBase):
    type: Literal["local_file"]
    directory: Optional[str] = None  # For versioned output
    file_path: Optional[str] = None  # For fixed output
    file_format: Literal["parquet", "csv", "json"] = "parquet"
    mode: Literal["replace", "append"] = "replace"

    @model_validator(mode="after")
    def validate_path_or_dir(self) -> 'LocalFileSinkConfig':
        if not self.directory and not self.file_path:
            raise ValueError("Either 'directory' or 'file_path' must be provided for local_file sink")
        return self

SinkConfig = Annotated[
    Union[PostgresSinkConfig, MySQLSinkConfig, DuckDBSinkConfig, S3SinkConfig, LocalFileSinkConfig, CSVSinkConfig],
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
    batch_size: Optional[int] = Field(default=None, ge=1000, description="Rows per batch. If set, loads in chunks to avoid OOM.")

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
