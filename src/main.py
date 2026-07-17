import importlib.metadata

import click
from api import app  # Expose app so `main:app` / `api:app` is a valid ASGI entrypoint


def _get_version() -> str:
    try:
        return importlib.metadata.version("dataflow")
    except importlib.metadata.PackageNotFoundError:
        try:
            import tomllib
            from pathlib import Path
            pyproject_path = Path(__file__).resolve().parent.parent / "pyproject.toml"
            with open(pyproject_path, "rb") as f:
                data = tomllib.load(f)
                return data["project"]["version"]
        except Exception:
            return "0.2.0"  # fallback fallback


__version__ = _get_version()


@click.group()
@click.version_option(version=__version__, prog_name="dataflow")
def cli():
    """Dataflow — self-hosted data pipeline engine."""
    pass


@cli.command()
@click.option("--host", default="127.0.0.1", help="Bind host")
@click.option("--port", default=8000, help="Bind port")
@click.option("--reload", is_flag=True, help="Enable auto-reload")
def server(host: str, port: int, reload: bool):
    """Start the web server.

    Database migrations, the job worker, and the cron scheduler all run
    automatically inside this process — this is the only command you need.
    """
    import uvicorn
    click.echo(f"Starting Dataflow at http://{host}:{port} ...")
    uvicorn.run("api:app", host=host, port=port, reload=reload)


if __name__ == "__main__":
    cli()
