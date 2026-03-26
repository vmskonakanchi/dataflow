from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Any
import os
from ..config.loader import load_configs, ResolvedConfig
from ..logger.run_log import get_run_history, get_last_successful_run
from ..executor.pipeline_runner import run_pipeline

app = FastAPI(title="Dataflow API")

# Enable CORS for React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

CONFIG_DIR = os.environ.get("DATAFLOW_CONFIG_DIR", "./configs")

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.get("/config")
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

@app.get("/pipelines")
def list_pipelines():
    try:
        resolved = load_configs(CONFIG_DIR)
        return list(resolved.pipelines.keys())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/history/{pipeline_name}")
def history(pipeline_name: str, limit: int = 20):
    try:
        return get_run_history(pipeline_name, limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def background_run(pipeline_name: str, resolved: ResolvedConfig):
    try:
        run_pipeline(pipeline_name, resolved)
    except Exception as e:
        print(f"Background run failed: {str(e)}")

@app.post("/run/{pipeline_name}")
def trigger_run(pipeline_name: str, background_tasks: BackgroundTasks):
    try:
        resolved = load_configs(CONFIG_DIR)
        if pipeline_name not in resolved.pipelines:
            raise HTTPException(status_code=404, detail="Pipeline not found")
        
        # Run in background to avoid timeout
        background_tasks.add_task(background_run, pipeline_name, resolved)
        return {"status": "started", "pipeline": pipeline_name}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats")
def get_stats():
    import sqlite3
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

# --- Infrastructure CRUD ---

import json

def _get_config_path(filename: str) -> str:
    return os.path.join(CONFIG_DIR, filename)

def _save_json_list(filename: str, items: List[Dict[str, Any]]):
    path = _get_config_path(filename)
    with open(path, "w") as f:
        json.dump(items, f, indent=4)

@app.post("/sources")
def add_source(source: Dict[str, Any]):
    try:
        path = _get_config_path("sources.json")
        with open(path, "r") as f:
            sources = json.load(f)
        
        # Check if exists
        if any(s["name"] == source["name"] for s in sources):
            raise HTTPException(status_code=400, detail="Source name already exists")
            
        sources.append(source)
        _save_json_list("sources.json", sources)
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/sources/{name}")
def update_source(name: str, source: Dict[str, Any]):
    try:
        path = _get_config_path("sources.json")
        with open(path, "r") as f:
            sources = json.load(f)
            
        for i, s in enumerate(sources):
            if s["name"] == name:
                sources[i] = source
                _save_json_list("sources.json", sources)
                return {"status": "success"}
        raise HTTPException(status_code=404, detail="Source not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/sources/{name}")
def delete_source(name: str):
    try:
        path = _get_config_path("sources.json")
        with open(path, "r") as f:
            sources = json.load(f)
            
        new_sources = [s for s in sources if s["name"] != name]
        if len(new_sources) == len(sources):
            raise HTTPException(status_code=404, detail="Source not found")
            
        _save_json_list("sources.json", new_sources)
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/sinks")
def add_sink(sink: Dict[str, Any]):
    try:
        path = _get_config_path("sinks.json")
        with open(path, "r") as f:
            sinks = json.load(f)
            
        if any(s["name"] == sink["name"] for s in sinks):
            raise HTTPException(status_code=400, detail="Sink name already exists")
            
        sinks.append(sink)
        _save_json_list("sinks.json", sinks)
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/sinks/{name}")
def update_sink(name: str, sink: Dict[str, Any]):
    try:
        path = _get_config_path("sinks.json")
        with open(path, "r") as f:
            sinks = json.load(f)
            
        for i, s in enumerate(sinks):
            if s["name"] == name:
                sinks[i] = sink
                _save_json_list("sinks.json", sinks)
                return {"status": "success"}
        raise HTTPException(status_code=404, detail="Sink not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/sinks/{name}")
def delete_sink(name: str):
    try:
        path = _get_config_path("sinks.json")
        with open(path, "r") as f:
            sinks = json.load(f)
            
        new_sinks = [s for s in sinks if s["name"] != name]
        if len(new_sinks) == len(sinks):
            raise HTTPException(status_code=404, detail="Sink not found")
            
        _save_json_list("sinks.json", new_sinks)
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

