import click
from api import app  # Expose app so `main:app` / `api:app` is a valid ASGI entrypoint


@click.group()
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
