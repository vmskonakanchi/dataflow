"""
FileConnector — reads from local files, S3, GCS, and static HTTP URLs.

This is the default connector and wraps the original Dataflow source logic.
Supports Parquet, JSON (ndjson/jsonl), and CSV formats with auto-detection.
"""

import glob
import os
import time

from connectors import Connector, ConnectionStatus, register


@register
class FileConnector(Connector):
    """Read files from local disk, S3, GCS, or static HTTP/HTTPS URLs."""

    @classmethod
    def connector_type(cls) -> str:
        return "file"

    @classmethod
    def metadata(cls) -> dict:
        return {
            "name": "File",
            "description": "Read from local files, S3, GCS, or HTTP URLs (Parquet, JSON, CSV)",
            "icon": "file",
            "version": "0.1.0",
            "author": "Dataflow",
            "capabilities": {"source", "sink"},
            "config_schema": {
                "path": {
                    "type": "string",
                    "required": True,
                    "label": "File Path",
                    "placeholder": "/data/input.parquet, s3://bucket/file.csv, https://...",
                    "description": "Local path (supports globs), S3 URI, GCS URI, or HTTP/HTTPS URL",
                },
            },
        }

    def read(self, conn, params: dict) -> str:
        """Set up extensions and return a DuckDB read expression for the file path."""
        path = params["path"]

        # Load extensions for remote paths
        if self._is_remote(path):
            conn.execute("INSTALL httpfs; LOAD httpfs;")
            if self._needs_s3_credentials(path):
                conn.execute("INSTALL aws; LOAD aws;")
                conn.execute(
                    "CREATE OR REPLACE SECRET (TYPE S3, PROVIDER CREDENTIAL_CHAIN, VALIDATION 'none');"
                )

        reader = self._detect_reader(path)
        return f"{reader}('{path}')"

    def write(self, conn, params: dict, source_expr: str) -> int:
        """Write data to a local or S3 parquet file."""
        path = params["path"]
        target_file_size = params.get("target_file_size")
        row_group_size = params.get("row_group_size")

        # Load extensions for remote paths
        if self._is_remote(path):
            conn.execute("INSTALL httpfs; LOAD httpfs;")
            if self._needs_s3_credentials(path):
                conn.execute("INSTALL aws; LOAD aws;")
                conn.execute(
                    "CREATE OR REPLACE SECRET (TYPE S3, PROVIDER CREDENTIAL_CHAIN, VALIDATION 'none');"
                )

        # Ensure output directory exists for local paths
        if not self._is_remote(path):
            dest_dir = os.path.dirname(os.path.abspath(path))
            os.makedirs(dest_dir, exist_ok=True)

        if target_file_size:
            os.makedirs(path, exist_ok=True)
            # Clear existing parquet files
            for f in glob.glob(os.path.join(path, "*.parquet")):
                os.remove(f)
            opts = ["FORMAT 'PARQUET'", f"FILE_SIZE_BYTES '{target_file_size}'", "OVERWRITE_OR_IGNORE"]
            if row_group_size and int(row_group_size) > 0:
                opts.append(f"ROW_GROUP_SIZE {int(row_group_size)}")
            copy_query = f"COPY (SELECT * FROM {source_expr}) TO '{path}' ({', '.join(opts)})"
        else:
            opts = ["FORMAT 'PARQUET'"]
            if row_group_size and int(row_group_size) > 0:
                opts.append(f"ROW_GROUP_SIZE {int(row_group_size)}")
            copy_query = f"COPY (SELECT * FROM {source_expr}) TO '{path}' ({', '.join(opts)})"

        res = conn.execute(copy_query).fetchone()
        return res[0] if res else 0

    def test_connection(self, params: dict) -> ConnectionStatus:
        """Validate the source path is reachable."""
        path = params.get("path", "")
        start = time.time()

        if not path:
            return ConnectionStatus(connected=False, message="No path provided")

        if self._is_remote(path):
            # For remote paths, we can't easily validate without DuckDB
            # Return optimistic status — actual errors surface at pipeline runtime
            return ConnectionStatus(
                connected=True,
                message=f"Remote path configured: {path}",
                latency_ms=round((time.time() - start) * 1000, 1),
            )

        # Local path — check glob matches
        matches = glob.glob(path)
        latency = round((time.time() - start) * 1000, 1)
        if matches:
            return ConnectionStatus(
                connected=True,
                message=f"Found {len(matches)} file(s)",
                latency_ms=latency,
                details={"files": matches[:10]},  # cap at 10 for UI display
            )
        return ConnectionStatus(
            connected=False,
            message=f"No files matched: {path}",
            latency_ms=latency,
        )

    # --- Private helpers ---

    @staticmethod
    def _is_remote(path: str) -> bool:
        return path.startswith(("s3://", "gs://", "gcs://", "http://", "https://"))

    @staticmethod
    def _needs_s3_credentials(path: str) -> bool:
        return path.startswith(("s3://", "gs://", "gcs://"))

    @staticmethod
    def _detect_reader(path: str) -> str:
        """Detect the correct DuckDB reader function based on file extension."""
        p = path.lower().split("*")[0].split("?")[0].rstrip("/\\.")
        if any(p.endswith(ext) for ext in (".json", ".ndjson", ".jsonl")):
            return "read_json"
        if any(p.endswith(ext) for ext in (".csv", ".tsv")):
            return "read_csv"
        return "read_parquet"
