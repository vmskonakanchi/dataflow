import boto3
import pandas as pd
import io
from datetime import datetime
from botocore.exceptions import ClientError
from typing import Optional
from .base import BaseConnector
from ..config.models import SourceConfig, SinkConfig, S3SinkConfig

@BaseConnector.register("s3")
class S3Connector(BaseConnector):
    def extract(self, source: SourceConfig, query: str, pipeline_name: str) -> pd.DataFrame:
        raise NotImplementedError("S3 is source only in V1")

    def load(self, df: pd.DataFrame, sink: S3SinkConfig, table: str, mode: str, key: Optional[str] = None) -> int:
        s3 = boto3.client(
            "s3",
            region_name=sink.region,
            aws_access_key_id=sink.access_key,
            aws_secret_access_key=sink.secret_key
        )
        
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        prefix = sink.prefix.rstrip("/") + "/" if sink.prefix else ""
        s3_key = f"{prefix}{table}/{table}_{timestamp}.{sink.file_format}"

        buffer = io.BytesIO()
        if sink.file_format == "parquet":
            df.to_parquet(buffer, index=False)
        elif sink.file_format == "csv":
            df.to_csv(buffer, index=False)
        elif sink.file_format == "json":
            df.to_json(buffer, orient="records")
        else:
            raise ValueError(f"Unsupported S3 file format: {sink.file_format}")

        try:
            s3.put_object(Bucket=sink.bucket, Key=s3_key, Body=buffer.getvalue())
            return len(df)
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            raise Exception(f"S3 upload failed for bucket '{sink.bucket}' with error code '{error_code}': {str(e)}")
