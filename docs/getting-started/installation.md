# Installation

Dataflow requires Python 3.13 or later. The recommended local workflow uses
[uv](https://docs.astral.sh/uv/).

## Docker

```bash
docker build -t dataflow:latest .
docker run --rm \
  -p 8000:8000 \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/db:/app/db" \
  dataflow:latest
```

Open `http://localhost:8000`.

## Local development

```bash
git clone https://github.com/vmskonakanchi/dataflow.git
cd dataflow
uv sync
uv run python main.py server --reload
```

Open `http://localhost:8000` and complete the initial administrator setup.

For all optional dependencies, run `uv sync --all-extras`.
