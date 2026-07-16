"""
Local LLM inference module using llama-cpp-python.

Provides:
- Model registry with pre-configured lightweight GGUF models
- Download manager (fetches models from HuggingFace on demand)
- Model loading/unloading with singleton pattern
- Chat completion inference with DuckDB SQL context
"""

import os
import threading
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from pathlib import Path
from settings import settings

# --- Configuration ---
MODELS_DIR = Path(settings.models_dir)
DEFAULT_N_CTX = 2048
DEFAULT_N_THREADS = os.cpu_count() or 4
DEFAULT_TEMPERATURE = 0.1
DEFAULT_MAX_TOKENS = 512


# --- Model Registry ---
@dataclass
class ModelInfo:
    """Metadata for a downloadable GGUF model."""
    id: str
    name: str
    description: str
    repo_id: str  # HuggingFace repo
    filename: str  # GGUF file within the repo
    size_mb: int  # Approximate download size
    parameters: str  # e.g. "1.1B", "3.8B"
    recommended_ram_gb: float


MODEL_REGISTRY: Dict[str, ModelInfo] = {
    "gemma-4-e4b": ModelInfo(
        id="gemma-4-e4b",
        name="Google Gemma 4 E4B",
        description="Google's latest model. 8B effective params, excellent reasoning and code generation. Best overall quality.",
        repo_id="ggml-org/gemma-4-E4B-it-GGUF",
        filename="gemma-4-E4B-it-Q4_K_M.gguf",
        size_mb=5340,
        parameters="8B (E4B MoE)",
        recommended_ram_gb=8.0,
    ),
    "qwen3-4b-sql": ModelInfo(
        id="qwen3-4b-sql",
        name="Qwen3 4B Text-to-SQL",
        description="Fine-tuned specifically for SQL generation. Best accuracy for query writing tasks.",
        repo_id="Ellbendls/Qwen-3-4b-Text_to_SQL-GGUF",
        filename="Qwen-3-4b-Text_to_SQL-q4_k_m.gguf",
        size_mb=2500,
        parameters="4B",
        recommended_ram_gb=4.0,
    ),
    "phi-3-mini": ModelInfo(
        id="phi-3-mini",
        name="Phi-3 Mini 3.8B",
        description="Microsoft's compact model. Good quality/speed balance for general queries and code.",
        repo_id="bartowski/Phi-3.1-mini-4k-instruct-GGUF",
        filename="Phi-3.1-mini-4k-instruct-Q4_K_M.gguf",
        size_mb=2300,
        parameters="3.8B",
        recommended_ram_gb=4.0,
    ),
    "qwen2-1.5b": ModelInfo(
        id="qwen2-1.5b",
        name="Qwen2 1.5B",
        description="Ultra-lightweight. Fastest inference on low-resource machines. Good for simple queries.",
        repo_id="Qwen/Qwen2-1.5B-Instruct-GGUF",
        filename="qwen2-1_5b-instruct-q4_k_m.gguf",
        size_mb=1000,
        parameters="1.5B",
        recommended_ram_gb=2.5,
    ),
}


# --- Download Manager ---
_download_lock = threading.Lock()
_download_progress: Dict[str, Dict[str, Any]] = {}  # model_id -> {status, progress, error}


def get_models_dir() -> Path:
    """Get the models directory, creating it if needed."""
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    return MODELS_DIR


def get_model_path(model_id: str) -> Optional[Path]:
    """Get the local file path for a model, or None if not downloaded."""
    if model_id not in MODEL_REGISTRY:
        return None
    info = MODEL_REGISTRY[model_id]
    path = get_models_dir() / info.filename
    if path.exists():
        return path
    return None


def is_model_downloaded(model_id: str) -> bool:
    """Check if a model is already downloaded locally."""
    return get_model_path(model_id) is not None


def list_models() -> List[Dict[str, Any]]:
    """List all available models with their download status."""
    models = []
    for model_id, info in MODEL_REGISTRY.items():
        downloaded = is_model_downloaded(model_id)
        # Check if currently downloading
        progress_info = _download_progress.get(model_id, {})
        status = "ready" if downloaded else progress_info.get("status", "not_downloaded")

        models.append({
            "id": info.id,
            "name": info.name,
            "description": info.description,
            "parameters": info.parameters,
            "size_mb": info.size_mb,
            "recommended_ram_gb": info.recommended_ram_gb,
            "downloaded": downloaded,
            "status": status,
            "progress": progress_info.get("progress", 0),
            "error": progress_info.get("error"),
        })
    return models


def download_model(model_id: str) -> None:
    """
    Download a model from HuggingFace Hub.
    Runs in a background thread — check progress via list_models() or get_download_status().
    """
    if model_id not in MODEL_REGISTRY:
        raise ValueError(f"Unknown model: {model_id}")

    if is_model_downloaded(model_id):
        return  # Already downloaded

    # Check if already downloading
    with _download_lock:
        if _download_progress.get(model_id, {}).get("status") == "downloading":
            return
        _download_progress[model_id] = {"status": "downloading", "progress": 0, "error": None}

    def _do_download():
        try:
            from huggingface_hub import hf_hub_download

            info = MODEL_REGISTRY[model_id]
            dest_dir = str(get_models_dir())

            # Download with progress tracking
            hf_hub_download(
                repo_id=info.repo_id,
                filename=info.filename,
                local_dir=dest_dir,
                local_dir_use_symlinks=False,
            )

            with _download_lock:
                _download_progress[model_id] = {"status": "ready", "progress": 100, "error": None}

        except Exception as e:
            with _download_lock:
                _download_progress[model_id] = {"status": "error", "progress": 0, "error": str(e)}

    thread = threading.Thread(target=_do_download, daemon=True)
    thread.start()


def get_download_status(model_id: str) -> Dict[str, Any]:
    """Get the download status for a specific model."""
    if is_model_downloaded(model_id):
        return {"status": "ready", "progress": 100, "error": None}
    return _download_progress.get(model_id, {"status": "not_downloaded", "progress": 0, "error": None})


# --- Model Loading & Inference ---
_model_lock = threading.Lock()
_loaded_model: Optional[Any] = None  # The Llama instance
_loaded_model_id: Optional[str] = None


def get_active_model_id() -> Optional[str]:
    """Return the ID of the currently loaded model, or None."""
    return _loaded_model_id


def load_model(model_id: str, n_ctx: int = DEFAULT_N_CTX, n_threads: int = DEFAULT_N_THREADS) -> bool:
    """
    Load a model into memory. Unloads any previously loaded model.
    Returns True on success, raises on failure.
    """
    global _loaded_model, _loaded_model_id

    if model_id not in MODEL_REGISTRY:
        raise ValueError(f"Unknown model: {model_id}")

    model_path = get_model_path(model_id)
    if model_path is None:
        raise FileNotFoundError(f"Model '{model_id}' is not downloaded. Download it first.")

    with _model_lock:
        # Unload existing model
        if _loaded_model is not None:
            del _loaded_model
            _loaded_model = None
            _loaded_model_id = None

        try:
            from llama_cpp import Llama

            _loaded_model = Llama(
                model_path=str(model_path),
                n_ctx=n_ctx,
                n_threads=n_threads,
                verbose=False,
            )
            _loaded_model_id = model_id
            return True

        except Exception as e:
            _loaded_model = None
            _loaded_model_id = None
            raise RuntimeError(f"Failed to load model '{model_id}': {e}") from e


def unload_model() -> None:
    """Unload the currently loaded model from memory."""
    global _loaded_model, _loaded_model_id
    with _model_lock:
        if _loaded_model is not None:
            del _loaded_model
            _loaded_model = None
            _loaded_model_id = None


def is_model_loaded() -> bool:
    """Check if any model is currently loaded."""
    return _loaded_model is not None


# --- System Prompt for SQL Generation ---
SYSTEM_PROMPT = """You are an AI data assistant for Dataflow, a data pipeline platform powered by DuckDB.

You help users with:
- Writing SQL queries (DuckDB dialect) for their data files and pipelines
- Data analysis, exploration, and troubleshooting
- Explaining query results and suggesting optimizations
- General questions about data engineering, SQL patterns, and best practices

DuckDB SQL specifics:
- Query files directly: read_parquet('path'), read_json('path'), read_csv('path')
- Glob patterns work: 'data/**/*.parquet'
- Supports CTEs, window functions, UNNEST, PIVOT, and modern SQL features
- Files are typically under the data/ directory but can be anywhere accessible

When generating SQL:
- Wrap SQL in a ```sql code block
- Use correct column names from the schema context when available
- If no schema is provided, write reasonable SQL based on the user's description
- Keep explanations concise

You are not limited to just query writing — answer any data-related question the user asks.

{schema_context}"""


def build_schema_context(schema_info: Optional[str] = None) -> str:
    """Build the schema context section of the system prompt."""
    if schema_info:
        return f"\nAvailable table schema:\n{schema_info}"
    return ""


def chat_completion(
    messages: List[Dict[str, str]],
    schema_info: Optional[str] = None,
    temperature: float = DEFAULT_TEMPERATURE,
    max_tokens: int = DEFAULT_MAX_TOKENS,
) -> Dict[str, Any]:
    """
    Run chat completion on the loaded model.
    
    Args:
        messages: List of {"role": "user"|"assistant", "content": "..."} dicts
        schema_info: Optional schema context to inject into system prompt
        temperature: Sampling temperature (lower = more deterministic)
        max_tokens: Maximum tokens to generate
    
    Returns:
        {"reply": str, "sql": str|None, "model": str}
    """
    if _loaded_model is None:
        raise RuntimeError("No model loaded. Load a model first via the model settings.")

    # Build system message with schema context
    schema_context = build_schema_context(schema_info)
    system_message = SYSTEM_PROMPT.format(schema_context=schema_context)

    # Construct full message list
    full_messages = [{"role": "system", "content": system_message}]
    full_messages.extend(messages)

    with _model_lock:
        response = _loaded_model.create_chat_completion(
            messages=full_messages,
            temperature=temperature,
            max_tokens=max_tokens,
            stop=["```\n\n", "</s>"],
        )

    # Extract the reply text
    reply_text = response["choices"][0]["message"]["content"].strip()

    # Try to extract SQL from the response
    import re
    sql_match = re.search(r"```sql\s*\n(.*?)```", reply_text, re.DOTALL)
    extracted_sql = sql_match.group(1).strip() if sql_match else None

    # If no fenced SQL but the entire response looks like SQL, treat it as such
    if not extracted_sql and reply_text.upper().startswith(("SELECT", "WITH", "COPY")):
        extracted_sql = reply_text.strip().rstrip(";") + ";"

    return {
        "reply": reply_text,
        "sql": extracted_sql,
        "model": _loaded_model_id,
    }


def get_schema_from_duckdb(file_path: str) -> Optional[str]:
    """
    Introspect a file's schema using DuckDB.
    Returns a formatted string of column names and types.
    """
    try:
        import duckdb
        conn = duckdb.connect()
        
        # Determine reader function
        lower = file_path.lower()
        if any(lower.endswith(ext) for ext in (".json", ".ndjson", ".jsonl")):
            reader = "read_json"
        elif any(lower.endswith(ext) for ext in (".csv", ".tsv")):
            reader = "read_csv"
        else:
            reader = "read_parquet"

        result = conn.execute(f"DESCRIBE SELECT * FROM {reader}('{file_path}')").fetchall()
        conn.close()

        if not result:
            return None

        lines = [f"  {row[0]} ({row[1]})" for row in result]
        return f"File: {file_path}\nColumns:\n" + "\n".join(lines)
    except Exception:
        return None


def get_all_schemas(data_dir: str = "data") -> Optional[str]:
    """
    Scan the data directory for files and return schemas of discovered datasets.
    Limited to first 5 files to keep context manageable.
    """
    import glob

    patterns = [
        f"{data_dir}/**/*.parquet",
        f"{data_dir}/**/*.csv",
        f"{data_dir}/**/*.json",
    ]

    files_found = []
    for pattern in patterns:
        files_found.extend(glob.glob(pattern, recursive=True))

    if not files_found:
        return None

    # Limit to 5 files to keep context window small
    schemas = []
    for fp in files_found[:5]:
        schema = get_schema_from_duckdb(fp)
        if schema:
            schemas.append(schema)

    return "\n\n".join(schemas) if schemas else None
