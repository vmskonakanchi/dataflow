import sys
import os
import importlib.util

# Add src to python path for internal imports inside src/ modules
src_path = os.path.join(os.path.dirname(__file__), "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

# Load src/main.py as a module under a distinct name to avoid name collision with this file
spec = importlib.util.spec_from_file_location("src_main", os.path.join(src_path, "main.py"))
src_main = importlib.util.module_from_spec(spec)
sys.modules["src_main"] = src_main
spec.loader.exec_module(src_main)

cli = src_main.cli
app = src_main.app

if __name__ == "__main__":
    cli()
