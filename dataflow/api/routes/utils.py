import os
import json
from typing import List, Dict, Any

CONFIG_DIR = os.environ.get("DATAFLOW_CONFIG_DIR", "./configs")

def get_config_path(filename: str) -> str:
    return os.path.join(CONFIG_DIR, filename)

def save_json_list(filename: str, items: List[Dict[str, Any]]):
    path = get_config_path(filename)
    with open(path, "w") as f:
        json.dump(items, f, indent=4)

def load_json_list(filename: str) -> List[Dict[str, Any]]:
    path = get_config_path(filename)
    if not os.path.exists(path):
        return []
    with open(path, "r") as f:
        return json.load(f)
