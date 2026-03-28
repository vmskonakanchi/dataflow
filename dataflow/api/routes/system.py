from fastapi import APIRouter, HTTPException
from typing import List, Dict, Any
import os
import sqlite3
from .utils import CONFIG_DIR
from ...config.loader import load_configs

router = APIRouter(tags=["System"])

@router.get("/health")
def health_check():
    return {"status": "ok"}

@router.get("/config")
def get_config():
    try:
        resolved = load_configs(CONFIG_DIR)
        return {
            "sources": list(resolved.sources.values()),
            "sinks": list(resolved.sinks.values()),
            "pipelines": list(resolved.pipelines.values()),
            "cronjobs": list(resolved.cronjobs.values()),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/stats")
def get_stats():
    DB_PATH = "dataflow_runs.db"
    if not os.path.exists(DB_PATH):
        return {"total_runs": 0, "success": 0, "failed": 0, "started": 0}
        
    conn = sqlite3.connect(DB_PATH)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT status, COUNT(*) FROM pipeline_runs GROUP BY status")
        rows = cursor.fetchall()
        stats = {"total_runs": 0, "success": 0, "failed": 0, "started": 0}
        for status, count in rows:
            stats[status] = count
            stats["total_runs"] += count
        return stats
    finally:
        conn.close()
