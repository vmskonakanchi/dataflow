# Stage 1 — Build the React frontend
FROM node:20-alpine AS frontend-builder

WORKDIR /app/ui

COPY ui/package.json ui/package-lock.json ./
RUN npm ci

COPY ui/ ./
RUN npm run build


# Stage 2 — Python backend + serve built UI
FROM python:3.13-slim

WORKDIR /app

# Install system dependencies
# gcc: needed for some Python package builds
# libpq-dev: needed for psycopg2-binary
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install uv

# Copy all source before syncing
# main.py is the CLI entry point declared in pyproject.toml [project.scripts]
# dataflow/ is the local package declared in [tool.setuptools]
COPY pyproject.toml .
COPY uv.lock .
COPY main.py .
COPY dataflow/ ./dataflow/

# Install dependencies
RUN uv sync --frozen --no-dev

# Copy built frontend from Stage 1
COPY --from=frontend-builder /app/ui/dist ./ui/dist

EXPOSE 8000

# Run FastAPI directly — not the CLI
CMD ["uv", "run", "fastapi", "run", "dataflow/api/main.py"]