from dagster import define_asset_job, ScheduleDefinition
from ..assets import (
    hello_world,
    dbt_setup,
    dbt_snapshots,
    dbt_seed,
    dbt_run,
    dbt_clean,
)

# Placeholder (hello_world) then DBT pipeline: deps, parse, snapshot, seed, run, clean
main_data_sync_pipeline = define_asset_job(
    name="main_data_sync_pipeline",
    selection=[
        "hello_world",  # Placeholder - replace with actual assets in real implementation
        "dbt_setup",
        "dbt_snapshots",
        "dbt_seed",
        "dbt_run",
        "dbt_clean",
    ],
    description="Job to run placeholder asset then DBT pipeline: deps, parse, snapshot, seed, run, clean"
)

main_data_sync_pipeline_schedule = ScheduleDefinition(
    job=main_data_sync_pipeline,
    cron_schedule="0 * * * *",
)
