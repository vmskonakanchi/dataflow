import os
import json
from sqlmodel import Session, select
from .database import engine, init_db
from .db_models import Source, Sink, Pipeline, CronJob

def migrate_json_to_db(config_dir: str = "./configs"):
    """Migrate JSON configuration files to the SQLite database."""
    print(f"Starting migration from {config_dir}...")
    init_db()
    
    with Session(engine) as session:
        # 1. Migrate Sources
        sources_path = os.path.join(config_dir, "sources.json")
        if os.path.exists(sources_path):
            with open(sources_path, "r") as f:
                sources_data = json.load(f)
                for s in sources_data:
                    # Check if already exists
                    statement = select(Source).where(Source.name == s["name"])
                    if session.exec(statement).first():
                        continue
                    
                    name = s.pop("name")
                    type_str = s.pop("type")
                    session.add(Source(name=name, type=type_str, config=s))
            print("Sources migrated.")

        # 2. Migrate Sinks
        sinks_path = os.path.join(config_dir, "sinks.json")
        if os.path.exists(sinks_path):
            with open(sinks_path, "r") as f:
                sinks_data = json.load(f)
                for s in sinks_data:
                    statement = select(Sink).where(Sink.name == s["name"])
                    if session.exec(statement).first():
                        continue
                    
                    name = s.pop("name")
                    type_str = s.pop("type")
                    session.add(Sink(name=name, type=type_str, config=s))
            print("Sinks migrated.")

        # 3. Migrate Pipelines
        pipelines_path = os.path.join(config_dir, "pipelines.json")
        if os.path.exists(pipelines_path):
            with open(pipelines_path, "r") as f:
                pipelines_data = json.load(f)
                for p in pipelines_data:
                    statement = select(Pipeline).where(Pipeline.name == p["name"])
                    if session.exec(statement).first():
                        continue
                    
                    session.add(Pipeline(
                        name=p["name"],
                        description=p.get("description"),
                        source=p["source"],
                        source_query=p["source_query"],
                        sink=p["sink"],
                        sink_table=p["sink_table"],
                        sink_mode=p["sink_mode"],
                        sink_key=p.get("sink_key"),
                        transforms=p.get("transforms", []),
                        alerts=p.get("alerts", {}),
                        batch_size=p.get("batch_size")
                    ))
            print("Pipelines migrated.")

        # 4. Migrate CronJobs
        cronjobs_path = os.path.join(config_dir, "cronjobs.json")
        if os.path.exists(cronjobs_path):
            with open(cronjobs_path, "r") as f:
                cronjobs_data = json.load(f)
                for c in cronjobs_data:
                    statement = select(CronJob).where(CronJob.name == c["name"])
                    if session.exec(statement).first():
                        continue
                    
                    session.add(CronJob(
                        name=c["name"],
                        pipeline=c["pipeline"],
                        schedule=c["schedule"],
                        timezone=c.get("timezone", "UTC"),
                        enabled=c.get("enabled", True),
                        retry=c.get("retry", {})
                    ))
            print("CronJobs migrated.")

        session.commit()
    print("Migration complete.")

if __name__ == "__main__":
    migrate_json_to_db()
