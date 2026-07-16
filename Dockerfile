FROM python:3.13-slim

WORKDIR /app

# Copy uv binary from official image for fast dependency resolution
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Copy dependency definition and lock file
COPY pyproject.toml uv.lock ./

# Sync dependencies in a frozen state, skipping development dependencies
RUN uv sync --frozen --no-dev

# Copy application code
COPY src/ ./src/
COPY main.py ./

# Expose the virtualenv's binaries to the PATH
ENV PATH="/app/.venv/bin:$PATH"

# Expose the FastAPI server port
EXPOSE 8000

# Set the entrypoint to our CLI
ENTRYPOINT ["python", "main.py"]

# Default command: start the web server (runs migrations, worker, and scheduler)
CMD ["server", "--host", "0.0.0.0", "--port", "8000"]

