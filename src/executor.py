import os
import glob
import signal
import shutil
import duckdb
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List
from config import PipelineConfig, ResolvedConfig, CheckConfig, role_disallowed_paths_by_name
from logger import log_run_start, log_run_success, log_run_failure
from alerts import send_failure_alert, send_row_count_alert, webhook_failure, webhook_low_row_count
from transforms import run_plugin, PluginError

CHECKPOINTS_DIR = ".checkpoints"

# --- Graceful Shutdown ---
_shutdown_requested = False


def _handle_shutdown(signum, frame):
    global _shutdown_requested
    _shutdown_requested = True


signal.signal(signal.SIGTERM, _handle_shutdown)
signal.signal(signal.SIGINT, _handle_shutdown)


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


class BaseExecutor(ABC):
    def __init__(self, pipeline: PipelineConfig):
        self.pipeline = pipeline

    @abstractmethod
    def execute(self) -> RunResult:
        raise NotImplementedError

    def validate_source(self):
        """Check source path exists and is not empty before running."""
        path = self.pipeline.source_path
        # For remote paths, skip local validation
        if path.startswith(("s3://", "gs://", "http://", "https://")):
            return
        # For local paths, check glob matches at least one file
        matches = glob.glob(path)
        if not matches:
            raise PipelineError(self.pipeline.name, "validate", f"Source path '{path}' matched no files")

    def validate_data_access(self):
        """Enforce the pipeline's run_as role bucket scope on every path before
        running (interim app-level data boundary). Admin/wildcard bypasses."""
        p = self.pipeline
        run_as = getattr(p, "run_as", None)
        if not run_as:
            return
        paths = [p.source_path, p.sink_path]
        for t in p.transforms:
            if getattr(t, "type", None) == "join":
                paths.append(getattr(t, "right_path", None))
        denied = role_disallowed_paths_by_name(run_as, [x for x in paths if x])
        if denied:
            raise PipelineError(
                p.name, "data_access",
                f"run_as role '{run_as}' is not permitted to access: {', '.join(denied)}",
            )


class DuckDBExecutor(BaseExecutor):
    def _needs_remote_access(self) -> bool:
        """Check if source, sink, or any join path points to remote storage (S3/GCS/HTTP)."""
        paths = [self.pipeline.source_path, self.pipeline.sink_path]
        for t in self.pipeline.transforms:
            if t.type == "join":
                paths.append(t.right_path)
        return any(
            p and (p.startswith(("s3://", "gs://", "gcs://", "http://", "https://")))
            for p in paths
        )

    def _needs_s3_credentials(self) -> bool:
        """Check if any path requires S3/GCS credentials (not needed for plain HTTP)."""
        paths = [self.pipeline.source_path, self.pipeline.sink_path]
        for t in self.pipeline.transforms:
            if t.type == "join":
                paths.append(t.right_path)
        return any(
            p and (p.startswith(("s3://", "gs://", "gcs://")))
            for p in paths
        )

    def _reader(self, path: str) -> str:
        p = path.lower().split("*")[0].split("?")[0].rstrip("/\\.")
        if any(p.endswith(ext) for ext in (".json", ".ndjson", ".jsonl")):
            return "read_json"
        if any(p.endswith(ext) for ext in (".csv", ".tsv")):
            return "read_csv"
        return "read_parquet"

    def _checkpoint_dir(self) -> str:
        return os.path.join(CHECKPOINTS_DIR, self.pipeline.name)

    def _checkpoint_path(self, step_idx: int) -> str:
        return os.path.join(self._checkpoint_dir(), f"step_{step_idx}.parquet")

    def _find_last_checkpoint(self) -> int:
        """Return index of last completed checkpoint step, or -1."""
        cp_dir = self._checkpoint_dir()
        if not os.path.exists(cp_dir):
            return -1
        existing = sorted(glob.glob(os.path.join(cp_dir, "step_*.parquet")))
        if not existing:
            return -1
        # Extract highest step number
        last_file = os.path.basename(existing[-1])
        return int(last_file.replace("step_", "").replace(".parquet", ""))

    def _cleanup_checkpoints(self):
        cp_dir = self._checkpoint_dir()
        if os.path.exists(cp_dir):
            shutil.rmtree(cp_dir)

    def _run_checks(self, conn, source_ref: str, checks: List[CheckConfig]) -> List[str]:
        """Run quality checks against transformed data. Returns list of failure messages."""
        failures = []
        for check in checks:
            if check.type == "not_null":
                for col in check.columns:
                    count = conn.execute(f"SELECT COUNT(*) FROM {source_ref} WHERE {col} IS NULL").fetchone()[0]
                    if count > 0:
                        failures.append(f"not_null: '{col}' has {count} null rows")
            elif check.type == "unique":
                for col in check.columns:
                    dupes = conn.execute(f"SELECT COUNT(*) - COUNT(DISTINCT {col}) FROM {source_ref}").fetchone()[0]
                    if dupes > 0:
                        failures.append(f"unique: '{col}' has {dupes} duplicates")
            elif check.type == "row_count_min":
                count = conn.execute(f"SELECT COUNT(*) FROM {source_ref}").fetchone()[0]
                if count < check.value:
                    failures.append(f"row_count_min: got {count}, expected >= {check.value}")
            elif check.type == "accepted_values":
                quoted = ", ".join(f"'{v}'" for v in check.values)
                count = conn.execute(f"SELECT COUNT(*) FROM {source_ref} WHERE {check.column} NOT IN ({quoted})").fetchone()[0]
                if count > 0:
                    failures.append(f"accepted_values: '{check.column}' has {count} rows with invalid values")
            elif check.type == "custom_sql":
                query = check.query.replace("output", source_ref)
                result = conn.execute(query).fetchone()[0]
                if result != check.must_be:
                    failures.append(f"custom_sql: got {result}, expected {check.must_be}")
        return failures

    def _run_python_chunked(self, conn, t, idx: int, last_source: str, chunk_rows: int) -> str:
        """Run a Python plugin memory-safely over bounded row-slices.

        Streams the current data to a temp parquet (constant RAM via COPY), then
        delegates to run_plugin_chunked, which processes each row-slice in a fresh
        subprocess. Returns a ``read_parquet(...)`` source over the chunk outputs
        so downstream SQL steps continue normally. The temp working dir is tracked
        for cleanup at the end of execute().
        """
        import tempfile
        from transforms.chunked import run_plugin_chunked

        workdir = tempfile.mkdtemp(prefix=f"df_plugin_{self.pipeline.name}_{idx}_")
        self._temp_dirs.append(workdir)
        in_parquet = os.path.join(workdir, "in.parquet")
        out_dir = os.path.join(workdir, "out")

        # Stream the current data to parquet without buffering it all in RAM.
        conn.execute(f"COPY (SELECT * FROM {last_source}) TO '{in_parquet}' (FORMAT 'PARQUET')")

        # Empty input: run once in-process on the empty table so the output keeps
        # the plugin's schema (a zero-chunk run would leave out_dir with no files).
        num_rows = conn.execute(
            f"SELECT num_rows FROM parquet_file_metadata('{in_parquet}')"
        ).fetchone()[0] or 0
        if num_rows == 0:
            arrow_in = conn.execute(f"SELECT * FROM read_parquet('{in_parquet}')").fetch_arrow_table()
            result_table = run_plugin(t.function, arrow_in, t.params)
            view_name = f"_df_step_{idx}"
            conn.register(view_name, result_table)
            return view_name

        run_plugin_chunked(
            function=t.function,
            in_parquet=in_parquet,
            out_dir=out_dir,
            params=t.params,
            chunk_rows=chunk_rows,
        )
        out_glob = os.path.join(out_dir, "*.parquet")
        return f"read_parquet('{out_glob}')"

    def execute(self) -> RunResult:
        global _shutdown_requested
        pipeline = self.pipeline
        started_at = datetime.utcnow()
        run_id = log_run_start(pipeline.name)
        conn = None
        step = "init"
        self._temp_dirs: List[str] = []

        try:
            # Input validation
            step = "validate"
            self.validate_source()

            # Enforce the run_as role's data-access (bucket) scope.
            step = "data_access"
            self.validate_data_access()

            conn = duckdb.connect()

            if pipeline.memory_limit:
                conn.execute(f"SET memory_limit = '{pipeline.memory_limit}'")
            if pipeline.threads:
                conn.execute(f"SET threads = {pipeline.threads}")

            # Enable out-of-core (streaming) execution: don't buffer the whole
            # result in RAM to preserve row order. Keeps memory footprint constant
            # regardless of dataset size — essential for large writes.
            conn.execute("SET preserve_insertion_order = false")

            # Set up remote access if any path is remote (source, sink, or join)
            if self._needs_remote_access():
                conn.execute("INSTALL httpfs; LOAD httpfs;")
                if self._needs_s3_credentials():
                    conn.execute("INSTALL aws; LOAD aws;")
                    conn.execute("CREATE OR REPLACE SECRET (TYPE S3, PROVIDER CREDENTIAL_CHAIN, VALIDATION 'none');")

            # Count rows extracted
            step = "extract_count"
            reader = self._reader(pipeline.source_path)
            res = conn.execute(f"SELECT COUNT(*) FROM {reader}('{pipeline.source_path}')").fetchone()
            rows_extracted = res[0] if res else 0

            # Determine resume point
            resume_from = -1
            if pipeline.checkpointing:
                resume_from = self._find_last_checkpoint()
                if resume_from >= 0:
                    os.makedirs(self._checkpoint_dir(), exist_ok=True)

            # Compile and execute transforms step by step
            step = "transform"
            last_source = f"{reader}('{pipeline.source_path}')"

            if resume_from >= 0:
                # Resume from checkpoint
                cp_path = self._checkpoint_path(resume_from)
                last_source = f"read_parquet('{cp_path}')"

            for idx, t in enumerate(pipeline.transforms):
                # Skip already checkpointed steps
                if idx <= resume_from:
                    continue

                # Graceful shutdown check
                if _shutdown_requested:
                    msg = f"Shutdown requested, checkpointed at step {idx - 1}"
                    log_run_failure(run_id, msg)
                    raise PipelineError(pipeline.name, "shutdown", msg)

                step_name = f"step_{idx}"

                # --- Python plugin transform (breaks the SQL chain) ---
                # ML/vector steps (UMAP, clustering, ...) can't be expressed as
                # streaming SQL. We materialize the current data to an in-memory
                # Arrow table, hand it to the plugin, and register the result back
                # as a DuckDB relation so downstream SQL steps continue normally.
                # NOTE: this is NOT out-of-core — the working set must fit in RAM.
                # Partition the pipeline (e.g. by day) to bound it, and rely on the
                # isolated worker process so it can't affect the web server.
                if t.type == "python":
                    step = step_name
                    chunk_rows = getattr(t, "chunk_rows", 0) or 0
                    try:
                        if chunk_rows > 0:
                            # Memory-safe path: stream input to a temp parquet,
                            # then run the plugin over bounded row-slices, each in
                            # a fresh subprocess (peak RAM bounded to one chunk).
                            result_source = self._run_python_chunked(
                                conn, t, idx, last_source, chunk_rows
                            )
                        else:
                            # In-process path: materialize the full table (working
                            # set must fit in RAM) and run the plugin here.
                            arrow_in = conn.execute(
                                f"SELECT * FROM {last_source}"
                            ).fetch_arrow_table()
                            result_table = run_plugin(t.function, arrow_in, t.params)
                            view_name = f"_df_step_{idx}"
                            conn.register(view_name, result_table)
                            result_source = view_name
                    except PluginError as pe:
                        raise PipelineError(pipeline.name, step_name, str(pe)) from pe

                    # Unified checkpoint handling for both paths: persist the step
                    # result via a streaming COPY so resume works identically.
                    if pipeline.checkpointing:
                        os.makedirs(self._checkpoint_dir(), exist_ok=True)
                        cp_path = self._checkpoint_path(idx)
                        conn.execute(
                            f"COPY (SELECT * FROM {result_source}) TO '{cp_path}' (FORMAT 'PARQUET')"
                        )
                        last_source = f"read_parquet('{cp_path}')"
                    else:
                        last_source = result_source
                    continue

                if t.type == "select":
                    step_sql = f"SELECT {', '.join(t.columns)} FROM {last_source}"
                elif t.type == "filter":
                    step_sql = f"SELECT * FROM {last_source} WHERE {t.condition}"
                elif t.type == "aggregate":
                    agg_cols = ", ".join(t.group_by + t.aggregates)
                    group_cols = ", ".join(t.group_by)
                    step_sql = f"SELECT {agg_cols} FROM {last_source} GROUP BY {group_cols}"
                elif t.type == "join":
                    step_sql = (
                        f"SELECT left.*, right.* FROM {last_source} AS left "
                        f"{t.join_type.upper()} JOIN read_parquet('{t.right_path}') AS right ON {t.on}"
                    )
                else:
                    step_sql = f"SELECT * FROM {last_source}"

                # If checkpointing, materialize each step
                if pipeline.checkpointing:
                    os.makedirs(self._checkpoint_dir(), exist_ok=True)
                    cp_path = self._checkpoint_path(idx)
                    conn.execute(f"COPY ({step_sql}) TO '{cp_path}' (FORMAT 'PARQUET')")
                    last_source = f"read_parquet('{cp_path}')"
                else:
                    # Build as CTE inline (wrap as subquery for next step)
                    view_name = f"_df_step_{idx}"
                    conn.execute(f"CREATE OR REPLACE TEMP VIEW {view_name} AS {step_sql}")
                    last_source = view_name

            # Write final output
            step = "quality_check"
            if pipeline.checks:
                failures = self._run_checks(conn, last_source, pipeline.checks)
                if failures:
                    msg = "Data quality checks failed:\n" + "\n".join(f"  - {f}" for f in failures)
                    raise PipelineError(pipeline.name, "quality_check", msg)

            step = "write"
            dest_dir = os.path.dirname(os.path.abspath(pipeline.sink_path))
            os.makedirs(dest_dir, exist_ok=True)

            if pipeline.sink_format == "delta":
                import pyarrow as pa
                from deltalake import write_deltalake

                # Stream the result in record batches instead of materializing the
                # entire table in memory. Keeps RAM constant for large datasets.
                result = conn.execute(f"SELECT * FROM {last_source}")
                reader = result.fetch_record_batch(100_000)
                schema = reader.schema

                rows_written = 0
                def _counting_batches():
                    nonlocal rows_written
                    for batch in reader:
                        rows_written += batch.num_rows
                        yield batch

                # Wrap the counting generator in a RecordBatchReader so deltalake
                # consumes it as a lazy Arrow stream (constant memory).
                stream = pa.RecordBatchReader.from_batches(schema, _counting_batches())

                write_kwargs = {"mode": "overwrite"}
                if pipeline.partition_by:
                    write_kwargs["partition_by"] = [pipeline.partition_by]
                write_deltalake(pipeline.sink_path, stream, **write_kwargs)
            else:
                # Parquet sink. When target_file_size is set, write MULTIPLE
                # ~N-sized files into a directory (DuckDB FILE_SIZE_BYTES);
                # otherwise write a single file. Optional row_group_size tuning.
                tfs = getattr(pipeline, "target_file_size", None)
                rgs = getattr(pipeline, "row_group_size", None)
                if tfs:
                    out_dir = pipeline.sink_path
                    os.makedirs(out_dir, exist_ok=True)
                    # Clear existing parquet files so the run reflects exactly
                    # this output (parity with single-file overwrite).
                    for f in glob.glob(os.path.join(out_dir, "*.parquet")):
                        os.remove(f)
                    opts = ["FORMAT 'PARQUET'", f"FILE_SIZE_BYTES '{tfs}'", "OVERWRITE_OR_IGNORE"]
                    if rgs and int(rgs) > 0:
                        opts.append(f"ROW_GROUP_SIZE {int(rgs)}")
                    copy_query = f"COPY (SELECT * FROM {last_source}) TO '{out_dir}' ({', '.join(opts)})"
                else:
                    opts = ["FORMAT 'PARQUET'"]
                    if rgs and int(rgs) > 0:
                        opts.append(f"ROW_GROUP_SIZE {int(rgs)}")
                    copy_query = f"COPY (SELECT * FROM {last_source}) TO '{pipeline.sink_path}' ({', '.join(opts)})"
                res = conn.execute(copy_query).fetchone()
                rows_written = res[0] if res else 0

            # Success — clean up checkpoints
            if pipeline.checkpointing:
                self._cleanup_checkpoints()

            log_run_success(run_id, rows_extracted, rows_written)

            if pipeline.alerts.on_row_count_below and rows_written < pipeline.alerts.on_row_count_below:
                if pipeline.alerts.email:
                    send_row_count_alert(pipeline.name, rows_written, pipeline.alerts.on_row_count_below, pipeline.alerts.email)
                webhook_low_row_count(pipeline.name, rows_written, pipeline.alerts.on_row_count_below)

            return RunResult(
                pipeline_name=pipeline.name, status="success",
                started_at=started_at, finished_at=datetime.utcnow(),
                rows_extracted=rows_extracted, rows_written=rows_written
            )

        except Exception as e:
            log_run_failure(run_id, str(e))
            if pipeline.alerts.on_failure == "email" and pipeline.alerts.email:
                send_failure_alert(pipeline.name, str(e), pipeline.alerts.email)
            webhook_failure(pipeline.name, str(e))
            raise PipelineError(pipeline.name, step, str(e)) from e
        finally:
            if conn:
                conn.close()
            # Remove any temp working dirs created by chunked python steps.
            for d in getattr(self, "_temp_dirs", []):
                shutil.rmtree(d, ignore_errors=True)


def run_pipeline(pipeline_name: str, resolved_config: ResolvedConfig) -> RunResult:
    pipeline = resolved_config.pipelines.get(pipeline_name)
    if not pipeline:
        raise PipelineError(pipeline_name, "init", f"Pipeline '{pipeline_name}' not found in resolved config")

    return DuckDBExecutor(pipeline).execute()
