from dagster import Definitions
from dagster_dbt import DbtCliResource

# Import assets and jobs
from .assets import (
    hello_world,
    dbt_setup,
    dbt_snapshots,
    dbt_seed,
    dbt_run,
    dbt_clean,
)
from .jobs import main_data_sync_pipeline, main_data_sync_pipeline_schedule

defs = Definitions(
    assets=[
        hello_world,
        dbt_setup,
        dbt_snapshots,
        dbt_seed,
        dbt_run,
        dbt_clean,
    ],
    jobs=[main_data_sync_pipeline],
    schedules=[main_data_sync_pipeline_schedule],
    resources={
        "dbt": DbtCliResource(
            project_dir="/app/dbt_models",
            profiles_dir="/app/dbt_models",
        )
    },
)
