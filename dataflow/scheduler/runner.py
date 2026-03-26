import time
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from ..config.loader import ResolvedConfig
from ..executor.pipeline_runner import run_pipeline

def run_job_with_retry(pipeline_name: str, resolved_config: ResolvedConfig, max_attempts: int, delay_seconds: int):
    attempts = 0
    while attempts < max_attempts:
        try:
            print(f"[{datetime.now().isoformat()}] Starting job: {pipeline_name} (Attempt {attempts + 1}/{max_attempts})")
            run_pipeline(pipeline_name, resolved_config)
            print(f"[{datetime.now().isoformat()}] Job success: {pipeline_name}")
            break
        except Exception as e:
            attempts += 1
            print(f"[{datetime.now().isoformat()}] Job failed: {pipeline_name}. Error: {str(e)}")
            if attempts < max_attempts:
                print(f"[{datetime.now().isoformat()}] Retrying in {delay_seconds}s...")
                time.sleep(delay_seconds)
            else:
                print(f"[{datetime.now().isoformat()}] Max attempts reached for job: {pipeline_name}")

# Need datetime for logging
from datetime import datetime

def start_scheduler(resolved_config: ResolvedConfig):
    scheduler = BackgroundScheduler()
    
    for cronjob in resolved_config.cronjobs.values():
        if not cronjob.enabled:
            continue
            
        print(f"Scheduling job: {cronjob.name} for pipeline: {cronjob.pipeline} ({cronjob.schedule} {cronjob.timezone})")
        
        scheduler.add_job(
            run_job_with_retry,
            CronTrigger.from_crontab(cronjob.schedule, timezone=pytz.timezone(cronjob.timezone)),
            args=[cronjob.pipeline, resolved_config, cronjob.retry.max_attempts, cronjob.retry.delay_seconds],
            id=cronjob.name,
            replace_existing=True
        )

    print(f"Scheduler started with {len(scheduler.get_jobs())} jobs.")
    scheduler.start()
    
    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        print("Scheduler stopped.")

def run_now(pipeline_name: str, resolved_config: ResolvedConfig):
    """Used by CLI to run a pipeline immediately."""
    return run_pipeline(pipeline_name, resolved_config)
