import sqlite3
import os
from datetime import datetime
from typing import List, Dict, Optional

DB_PATH = "dataflow_runs.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline_name  TEXT NOT NULL,
                job_name       TEXT,
                status         TEXT NOT NULL,
                started_at     TEXT NOT NULL,
                finished_at    TEXT,
                rows_extracted INTEGER,
                rows_written   INTEGER,
                error_message  TEXT
            )
        """)
        conn.commit()
    finally:
        conn.close()

def log_run_start(pipeline_name: str, job_name: Optional[str] = None) -> int:
    init_db()  # Ensure table exists
    conn = sqlite3.connect(DB_PATH)
    try:
        cursor = conn.cursor()
        started_at = datetime.utcnow().isoformat()
        cursor.execute(
            "INSERT INTO pipeline_runs (pipeline_name, job_name, status, started_at) VALUES (?, ?, ?, ?)",
            (pipeline_name, job_name, "started", started_at)
        )
        run_id = cursor.lastrowid
        conn.commit()
        return run_id
    finally:
        conn.close()

def log_run_success(run_id: int, rows_extracted: int, rows_written: int):
    conn = sqlite3.connect(DB_PATH)
    try:
        cursor = conn.cursor()
        finished_at = datetime.utcnow().isoformat()
        cursor.execute(
            "UPDATE pipeline_runs SET status = ?, finished_at = ?, rows_extracted = ?, rows_written = ? WHERE id = ?",
            ("success", finished_at, rows_extracted, rows_written, run_id)
        )
        conn.commit()
    finally:
        conn.close()

def log_run_failure(run_id: int, error_message: str):
    conn = sqlite3.connect(DB_PATH)
    try:
        cursor = conn.cursor()
        finished_at = datetime.utcnow().isoformat()
        cursor.execute(
            "UPDATE pipeline_runs SET status = ?, finished_at = ?, error_message = ? WHERE id = ?",
            ("failed", finished_at, error_message, run_id)
        )
        conn.commit()
    finally:
        conn.close()

def get_last_successful_run(pipeline_name: str) -> Optional[datetime]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT MAX(finished_at) as last_run FROM pipeline_runs WHERE pipeline_name = ? AND status = ?",
            (pipeline_name, "success")
        )
        row = cursor.fetchone()
        if row and row["last_run"]:
            return datetime.fromisoformat(row["last_run"])
        return None
    finally:
        conn.close()

def get_run_history(pipeline_name: str, limit: int = 10) -> List[Dict]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM pipeline_runs WHERE pipeline_name = ? ORDER BY started_at DESC LIMIT ?",
            (pipeline_name, limit)
        )
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()
