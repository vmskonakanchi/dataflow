"""Tests for llm.py: schema introspection, prompt generation, model registry, and mocked download/inference."""

import os
import sys
import tempfile
import pytest
from unittest.mock import MagicMock, patch
import pyarrow as pa
import pyarrow.parquet as pq

# 1. Setup mock modules for deferred imports before importing llm.py
sys.modules['llama_cpp'] = MagicMock()
sys.modules['huggingface_hub'] = MagicMock()

_TMPDB = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_TMPDB.close()
os.environ["DATAFLOW_DB"] = _TMPDB.name

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from config import init_db
init_db()
from settings import settings
settings.seed()

import llm
from llm import (
    MODEL_REGISTRY, list_models, get_model_path, is_model_downloaded,
    build_schema_context, get_schema_from_duckdb, get_all_schemas,
    download_model, get_download_status, load_model, unload_model,
    is_model_loaded, get_active_model_id, chat_completion
)


@pytest.fixture(scope="module", autouse=True)
def _init():
    init_db()
    yield
    if os.path.exists(_TMPDB.name):
        try:
            os.remove(_TMPDB.name)
        except Exception:
            pass


# --- Model Registry Tests ---

def test_registry_metadata():
    assert "phi-3-mini" in MODEL_REGISTRY
    assert "qwen2-1.5b" in MODEL_REGISTRY
    
    phi = MODEL_REGISTRY["phi-3-mini"]
    assert phi.repo_id == "bartowski/Phi-3.1-mini-4k-instruct-GGUF"
    assert phi.size_mb > 0


def test_list_models(tmp_path, monkeypatch):
    # Point models_dir to tmp_path
    monkeypatch.setattr(llm, "MODELS_DIR", tmp_path)

    # Initially none are downloaded
    models = list_models()
    assert all(m["downloaded"] is False for m in models)
    assert all(m["status"] == "not_downloaded" for m in models)

    # Simulate phi-3-mini being present
    phi_info = MODEL_REGISTRY["phi-3-mini"]
    with open(tmp_path / phi_info.filename, "w") as f:
        f.write("mock-gguf-content")

    models_updated = list_models()
    phi_model = next(m for m in models_updated if m["id"] == "phi-3-mini")
    assert phi_model["downloaded"] is True
    assert phi_model["status"] == "ready"


# --- Prompt & Introspection Tests ---

def test_build_schema_context():
    # Empty schema
    assert build_schema_context("") == ""
    assert build_schema_context(None) == ""

    # Present schema
    ctx = build_schema_context("Table X (col1 INT)")
    assert "Available table schema:" in ctx
    assert "Table X (col1 INT)" in ctx


def test_schema_introspection_duckdb(tmp_path):
    parquet_path = str(tmp_path / "intro.parquet")
    # 2 columns
    table = pa.table({"a_col": [1, 2], "b_col": ["val1", "val2"]})
    pq.write_table(table, parquet_path)

    schema = get_schema_from_duckdb(parquet_path)
    assert schema is not None
    assert "File: " in schema
    assert "a_col (INTEGER)" in schema or "a_col (INT" in schema or "a_col (BIGINT)" in schema
    assert "b_col (VARCHAR)" in schema


def test_get_all_schemas(tmp_path):
    # Write a parquet file
    p_path = tmp_path / "t1.parquet"
    table = pa.table({"col_a": [1.0]})
    pq.write_table(table, p_path)

    # Scan temp directory
    schemas = get_all_schemas(str(tmp_path))
    assert schemas is not None
    assert "col_a" in schemas


# --- Download Manager Mocks ---

def test_download_model_lifecycle(tmp_path, monkeypatch):
    monkeypatch.setattr(llm, "MODELS_DIR", tmp_path)
    
    # Mock hf_hub_download to simulate downloading
    download_started = False
    
    def mock_hf_download(repo_id, filename, local_dir, local_dir_use_symlinks):
        nonlocal download_started
        download_started = True
        # Simulate creating the file
        with open(os.path.join(local_dir, filename), "w") as f:
            f.write("mock gguf file")

    import huggingface_hub
    monkeypatch.setattr(huggingface_hub, "hf_hub_download", mock_hf_download)

    # Reset download progress tracking
    monkeypatch.setattr(llm, "_download_progress", {})

    download_model("qwen2-1.5b")
    
    # Wait for the background thread to finish downloading (very fast because it is mocked)
    import time
    for _ in range(20):
        status = get_download_status("qwen2-1.5b")
        if status["status"] == "ready":
            break
        time.sleep(0.05)

    assert download_started is True
    assert get_download_status("qwen2-1.5b")["status"] == "ready"


# --- Load & Inference Mocks ---

def test_load_unload_model(tmp_path, monkeypatch):
    monkeypatch.setattr(llm, "MODELS_DIR", tmp_path)
    
    # Simulate file exists
    phi_info = MODEL_REGISTRY["phi-3-mini"]
    with open(tmp_path / phi_info.filename, "w") as f:
        f.write("gguf")

    mock_llama_instance = MagicMock()
    mock_llama_class = MagicMock(return_value=mock_llama_instance)
    
    import llama_cpp
    monkeypatch.setattr(llama_cpp, "Llama", mock_llama_class)

    # Initial state
    assert is_model_loaded() is False
    assert get_active_model_id() is None

    # Load model
    load_model("phi-3-mini")
    assert is_model_loaded() is True
    assert get_active_model_id() == "phi-3-mini"
    assert mock_llama_class.call_count == 1

    # Unload model
    unload_model()
    assert is_model_loaded() is False
    assert get_active_model_id() is None


def test_chat_completion(monkeypatch):
    mock_llama_instance = MagicMock()
    mock_llama_instance.create_chat_completion.return_value = {
        "choices": [
            {
                "message": {
                    "content": "Sure, here is the SQL query:\n\n```sql\nSELECT * FROM read_parquet('data.parquet')\n```"
                }
            }
        ]
    }
    
    monkeypatch.setattr(llm, "_loaded_model", mock_llama_instance)
    monkeypatch.setattr(llm, "_loaded_model_id", "qwen2-1.5b")

    res = chat_completion([{"role": "user", "content": "Get SQL"}])
    
    assert res["reply"] == "Sure, here is the SQL query:\n\n```sql\nSELECT * FROM read_parquet('data.parquet')\n```"
    assert res["sql"] == "SELECT * FROM read_parquet('data.parquet')"
    assert res["model"] == "qwen2-1.5b"
