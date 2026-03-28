import click
import sys
import os
import time
from dataflow.config.loader import load_configs, ConfigError
from dataflow.scheduler.runner import run_now, start_scheduler
from dataflow.logger.run_log import get_run_history
from dataflow.config.database import init_db as db_init

@click.group()
def cli():
    """Dataflow - Config-driven data pipeline platform."""
    pass

@cli.command()
def init_db():
    """Initialize the database tables."""
    db_init()
    click.echo("Database initialized.")

@cli.command()
def validate():
    """Load and validate all configuration from the database."""
    try:
        resolved = load_configs()
        click.echo(f"Sources:    {len(resolved.sources)} loaded  [{', '.join(resolved.sources.keys())}]")
        click.echo(f"Sinks:      {len(resolved.sinks)} loaded  [{', '.join(resolved.sinks.keys())}]")
        click.echo(f"Pipelines:  {len(resolved.pipelines)} loaded  [{', '.join(resolved.pipelines.keys())}]")
        click.echo(f"Cronjobs:   {len(resolved.cronjobs)} loaded  [{', '.join(resolved.cronjobs.keys())}]")
        click.echo("Cross-references: all valid")
        click.echo("Config is ready to run.")
    except ConfigError as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)

@cli.command()
@click.option("-p", "--pipeline", required=True, help="Name of the pipeline to run")
def run(pipeline: str):
    """Run a pipeline immediately."""
    try:
        resolved = load_configs()
        if pipeline not in resolved.pipelines:
            click.echo(f"Error: Pipeline '{pipeline}' not found in database.", err=True)
            sys.exit(1)
            
        pipeline_config = resolved.pipelines[pipeline]
        source_config = resolved.sources[pipeline_config.source]
        sink_config = resolved.sinks[pipeline_config.sink]
        
        click.echo(f"Running pipeline: {pipeline}")
        click.echo(f"Extracting from source: {pipeline_config.source} ({source_config.type})")
        
        start_time = time.time()
        result = run_now(pipeline, resolved)
        duration = time.time() - start_time
        
        click.echo(f"Rows extracted: {result.rows_extracted}")
        click.echo(f"Applying {len(pipeline_config.transforms)} transforms...")
        click.echo(f"Loading to sink: {pipeline_config.sink} ({sink_config.type})")
        click.echo(f"Done. Rows written: {result.rows_written}  Duration: {duration:.1f}s")
        
    except Exception as e:
        click.echo(f"Error running pipeline '{pipeline}': {str(e)}", err=True)
        sys.exit(1)

@cli.command()
def schedule():
    """Start the scheduler."""
    try:
        resolved = load_configs()
        click.echo("Starting scheduler...")
        start_scheduler(resolved)
    except ConfigError as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)

@cli.command()
@click.option("-p", "--pipeline", required=True, help="Name of the pipeline")
@click.option("-n", "--limit", default=10, help="Number of runs to show")
def history(pipeline: str, limit: int):
    """Show run history for a pipeline."""
    runs = get_run_history(pipeline, limit)
    if not runs:
        click.echo(f"No run history found for pipeline: {pipeline}")
        return
        
    click.echo(f"{'ID':<4} | {'Status':<7} | {'Started':<20} | {'Duration':<8} | {'In':<6} | {'Out':<6} | {'Error'}")
    click.echo("-" * 80)
    for run in runs:
        run_id = run["id"]
        status = run["status"]
        started = run["started_at"].split(".")[0].replace("T", " ")
        
        duration = "—"
        if run["finished_at"] and run["started_at"]:
            try:
                from datetime import datetime
                s = datetime.fromisoformat(run["started_at"])
                f = datetime.fromisoformat(run["finished_at"])
                duration = f"{ (f - s).total_seconds():.1f}s"
            except:
                pass
                
        extracted = run["rows_extracted"] if run["rows_extracted"] is not None else 0
        written = run["rows_written"] if run["rows_written"] is not None else 0
        error = run["error_message"] if run["error_message"] else "—"
        
        click.echo(f"{run_id:<4} | {status:<7} | {started:<20} | {duration:<8} | {extracted:<6} | {written:<6} | {error}")

@cli.command()
def list_pipelines():
    """List all pipelines and their cronjobs."""
    try:
        resolved = load_configs()
        for p_name, p in resolved.pipelines.items():
            source = resolved.sources[p.source]
            sink = resolved.sinks[p.sink]
            click.echo(f"Pipeline: {p_name}")
            click.echo(f"  Source:  {p.source} ({source.type})")
            click.echo(f"  Sink:    {p.sink} ({sink.type})")
            
            # Find associated cronjobs
            cronjobs = [c for c in resolved.cronjobs.values() if c.pipeline == p_name]
            if cronjobs:
                for c in cronjobs:
                    enabled_str = "enabled" if c.enabled else "disabled"
                    click.echo(f"  Cron:    {c.name} — {c.schedule} {c.timezone} ({enabled_str})")
            else:
                click.echo("  Cron:    <none>")
            click.echo("")
    except ConfigError as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)

if __name__ == "__main__":
    cli()
