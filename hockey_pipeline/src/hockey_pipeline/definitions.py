from pathlib import Path

from dagster import definitions, load_from_defs_folder, define_asset_job, ScheduleDefinition

# A job that materializes all our assets
nhl_job = define_asset_job(name="nhl_update_job", selection="*")

# A schedule to rucdn the job daily at 2 AM
daily_schedule = ScheduleDefinition(
    job=nhl_job,
    cron_schedule="0 2 * * *", # Daily at 2:00 AM
)


@definitions
def defs():
    return load_from_defs_folder(project_root=Path(__file__).parent.parent.parent)
