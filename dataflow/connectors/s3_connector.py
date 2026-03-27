import boto3
import pandas as pd
import pyarrow.parquet as pq
import io
from datetime import datetime
from botocore.exceptions import ClientError
from botocore import UNSIGNED
from botocore.config import Config
from typing import Optional
from .base import BaseConnector
from ..config.models import S3SourceConfig, S3SinkConfig


def _make_s3_client(region: str, access_key: Optional[str], secret_key: Optional[str]):
    """Returns an authenticated or anonymous S3 client."""
    if access_key and secret_key:
        return boto3.client(
            "s3",
            region_name=region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
    # No credentials → anonymous access for public buckets
    return boto3.client(
        "s3",
        region_name=region,
        config=Config(signature_version=UNSIGNED),
    )


@BaseConnector.register("s3")
class S3Connector(BaseConnector):
    def extract(self, source: S3SourceConfig, query: str, pipeline_name: str) -> pd.DataFrame:
        s3 = _make_s3_client(source.region, source.access_key, source.secret_key)
        requester_pays = source.public

        # If key ends with / or *, list and read all matching objects under the prefix
        key = source.key
        if key.endswith("/") or "*" in key:
            prefix = key.rstrip("*").rstrip("/") + "/"
            list_kwargs = {"Bucket": source.bucket, "Prefix": prefix}
            if requester_pays:
                list_kwargs["RequestPayer"] = "requester"
            response = s3.list_objects_v2(**list_kwargs)
            objects = response.get("Contents", [])
            if not objects:
                raise FileNotFoundError(f"No objects found at s3://{source.bucket}/{prefix}")

            dfs = []
            for obj in objects:
                df = self._read_object(s3, source.bucket, obj["Key"], source.file_format, requester_pays)
                dfs.append(df)
            return pd.concat(dfs, ignore_index=True)
        else:
            return self._read_object(s3, source.bucket, key, source.file_format, requester_pays)

    def extract_chunks(self, source: S3SourceConfig, query: str, pipeline_name: str, chunk_size: int):
        """True streaming for single-key parquet/csv/json. Prefix keys fall back to full-load slice."""
        # Prefix/wildcard keys must concat multiple objects — fall back to full extract then slice
        if source.key.endswith("/") or "*" in source.key:
            yield from super().extract_chunks(source, query, pipeline_name, chunk_size)
            return

        s3 = _make_s3_client(source.region, source.access_key, source.secret_key)
        get_kwargs = {"Bucket": source.bucket, "Key": source.key}
        if source.public:
            get_kwargs["RequestPayer"] = "requester"
        response = s3.get_object(**get_kwargs)
        buffer = io.BytesIO(response["Body"].read())

        if source.file_format == "parquet":
            pf = pq.ParquetFile(buffer)
            for batch in pf.iter_batches(batch_size=chunk_size):
                yield batch.to_pandas()

        elif source.file_format == "csv":
            for chunk in pd.read_csv(buffer, chunksize=chunk_size):
                yield chunk

        elif source.file_format == "json":
            # Supports newline-delimited JSON (one record per line)
            for chunk in pd.read_json(buffer, lines=True, chunksize=chunk_size):
                yield chunk

    def _read_object(self, s3_client, bucket: str, key: str, file_format: str, requester_pays: bool = False) -> pd.DataFrame:
        try:
            get_kwargs = {"Bucket": bucket, "Key": key}
            if requester_pays:
                get_kwargs["RequestPayer"] = "requester"
            response = s3_client.get_object(**get_kwargs)
            body = response["Body"].read()
            buffer = io.BytesIO(body)

            if file_format == "parquet":
                return pd.read_parquet(buffer)
            elif file_format == "csv":
                return pd.read_csv(buffer)
            elif file_format == "json":
                return pd.read_json(buffer, orient="records")
            else:
                raise ValueError(f"Unsupported S3 file format: {file_format}")
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            raise Exception(f"S3 read failed for s3://{bucket}/{key} [{error_code}]: {str(e)}")

    def load(self, df: pd.DataFrame, sink: S3SinkConfig, table: str, mode: str, key: Optional[str] = None) -> int:
        s3 = boto3.client(
            "s3",
            region_name=sink.region,
            aws_access_key_id=sink.access_key,
            aws_secret_access_key=sink.secret_key,
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
            raise Exception(f"S3 upload failed for bucket '{sink.bucket}' [{error_code}]: {str(e)}")
