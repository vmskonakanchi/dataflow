"""Job queue worker — runs as a separate OS process.

The server spawns and supervises this worker as its own process (via
`python -m worker`), giving full memory isolation: a heavy pipeline can no
longer OOM the web server. The worker claims queued jobs, runs them, and
heartbeats while executing so crashes are detectable. On startup it reconciles
jobs orphaned by a previous crash.

You never run this by hand — the server manages it. It's importable as a module
so `python -m worker` is the process entrypoint.
"""

import os
import sys
import time
import signal
import logging
import threading
import subprocess

import jobs
from config import load_configs
from executor import run_pipeline

logger = logging.getLogger("dataflow.worker")

_HEARTBEAT_INTERVAL = 10  # seconds


class Worker:
    def __init__(self, poll_seconds: int = 2, stale_seconds: int = 60):
        self.poll_seconds = poll_seconds
        self.stale_seconds = stale_seconds
        self.pid = os.getpid()
        self._stop = threading.Event()
        self._last_reconcile = 0.0

    def stop(self):
        self._stop.set()

    def run_forever(self):
        logger.info("Worker process %s starting", self.pid)
        # Recover orphaned jobs from a previous crash before consuming.
        try:
            recovered = jobs.reconcile_stale(self.stale_seconds)
            if recovered:
                logger.info("Recovered %d stale job(s) on startup", recovered)
        except Exception as e:
            logger.warning("Startup reconciliation failed: %s", e)

        while not self._stop.is_set():
            try:
                self._reconcile_periodically()
                job = jobs.claim_next(self.pid)
                if job is None:
                    self._stop.wait(self.poll_seconds)
                    continue
                self._run_job(job)
            except Exception as e:
                logger.error("Worker loop error: %s", e)
                self._stop.wait(self.poll_seconds)
        logger.info("Worker process %s stopping", self.pid)

    def _reconcile_periodically(self):
        now = time.time()
        if now - self._last_reconcile >= self.stale_seconds:
            self._last_reconcile = now
            try:
                jobs.reconcile_stale(self.stale_seconds)
            except Exception as e:
                logger.warning("Periodic reconciliation failed: %s", e)

    def _run_job(self, job):
        logger.info("Running job %d (pipeline=%s, attempt=%d)", job.id, job.pipeline, job.attempts)
        stop_heartbeat = threading.Event()
        hb = threading.Thread(target=self._heartbeat_loop, args=(job.id, stop_heartbeat), daemon=True)
        hb.start()
        try:
            resolved = load_configs()
            run_pipeline(job.pipeline, resolved)
            jobs.finish(job.id, "success")
            logger.info("Job %d succeeded", job.id)
        except Exception as e:
            jobs.finish(job.id, "failed", error_message=str(e))
            logger.error("Job %d failed: %s", job.id, e)
        finally:
            stop_heartbeat.set()

    def _heartbeat_loop(self, job_id: int, stop: threading.Event):
        while not stop.wait(_HEARTBEAT_INTERVAL):
            try:
                jobs.heartbeat(job_id)
            except Exception as e:
                logger.warning("Heartbeat failed for job %d: %s", job_id, e)


# --- Supervised worker process (managed by the server) ---
_worker_proc: "subprocess.Popen | None" = None


def start_worker_process() -> None:
    """Spawn the worker as a separate OS process. Idempotent."""
    global _worker_proc
    if _worker_proc is not None and _worker_proc.poll() is None:
        return  # already running

    src_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(src_dir)
    env = dict(os.environ)
    # Ensure the worker can import the src/ modules as top-level packages.
    env["PYTHONPATH"] = src_dir + os.pathsep + env.get("PYTHONPATH", "")

    kwargs = {}
    if os.name == "nt":
        # New process group so we can signal/terminate the whole tree. sys.executable
        # in a uv venv is a trampoline that spawns the real interpreter as a child,
        # so we must be able to kill descendants too (see stop_worker_process).
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        # New session so the worker (and any children) form their own process group.
        kwargs["start_new_session"] = True

    _worker_proc = subprocess.Popen(
        [sys.executable, "-m", "worker"],
        cwd=project_root,
        env=env,
        **kwargs,
    )
    logger.info("Started worker process pid=%s", _worker_proc.pid)


def stop_worker_process(timeout: int = 10) -> None:
    """Terminate the supervised worker process tree (called on server shutdown).

    On Windows, sys.executable may be a launcher trampoline that spawns the real
    interpreter as a child, so we kill the whole tree rather than just the
    immediate process.
    """
    global _worker_proc
    if _worker_proc is None:
        return

    proc = _worker_proc
    if proc.poll() is None:
        if os.name == "nt":
            # taskkill /T terminates the process and all its descendants.
            try:
                subprocess.run(
                    ["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                    capture_output=True,
                    check=False,
                )
            except Exception as e:
                logger.warning("taskkill failed for worker pid %s: %s", proc.pid, e)
                proc.kill()
        else:
            # Signal the whole process group.
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            except Exception:
                proc.terminate()
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
    _worker_proc = None


def is_worker_running() -> bool:
    return _worker_proc is not None and _worker_proc.poll() is None


def _main():
    """Process entrypoint: `python -m worker`. Reads tuning from DB settings."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    from settings import settings

    worker = Worker(
        poll_seconds=settings.worker_poll_seconds,
        stale_seconds=settings.worker_stale_seconds,
    )

    def _handle_signal(_signum, _frame):
        worker.stop()

    signal.signal(signal.SIGINT, _handle_signal)
    try:
        signal.signal(signal.SIGTERM, _handle_signal)
    except (AttributeError, ValueError):
        # SIGTERM may be unavailable on some platforms.
        pass

    worker.run_forever()


if __name__ == "__main__":
    _main()
