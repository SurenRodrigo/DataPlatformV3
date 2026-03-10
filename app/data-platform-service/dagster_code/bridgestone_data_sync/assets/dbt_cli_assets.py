from dagster import asset, AssetExecutionContext
from dagster_dbt import DbtCliResource

"""
1. Check & install dbt Dependencies and parse dbt project
"""


@asset(
    name="dbt_setup",
    group_name="bridgestone_data_sync",
    deps=["hello_world"],
)
def dbt_setup(context: AssetExecutionContext, dbt: DbtCliResource):
    context.log.info("Installing DBT package dependencies (dbt deps)...")
    deps_result = dbt.cli(["deps"]).wait()
    if not deps_result.is_successful():
        raise Exception("DBT dependencies installation failed bridgestone_data_sync")
    context.log.info("Generating DBT manifest (dbt parse)...")
    parse_result = dbt.cli(["parse"]).wait()
    if not parse_result.is_successful():
        raise Exception("DBT parse failed bridgestone_data_sync")
    return {"status": "dbt_initialization_complete"}


"""
2. DBT snapshots
"""


@asset(
    name="dbt_snapshots",
    group_name="bridgestone_data_sync",
    deps=["dbt_setup"],
)
def dbt_snapshots(context: AssetExecutionContext, dbt: DbtCliResource):
    context.log.info("Executing DBT snapshots...")
    try:
        snapshots_result = dbt.cli(["snapshot", "--threads", "6"]).wait()

        if not snapshots_result.is_successful():
            error_message = getattr(snapshots_result.failure_event.raw, "message", "Unknown error")
            context.log.error(f"DBT Snapshot failed: {error_message}")
            raise Exception(f"DBT snapshots execution failed bridgestone_data_sync: {error_message}")

        context.log.info("DBT Snapshots completed successfully")
        return {"status": "dbt_snapshots_execution_completed"}
    except Exception as e:
        context.log.error(f"Error during DBT snapshots: {str(e)}")
        raise


"""
3. DBT seed
"""


@asset(
    name="dbt_seed",
    group_name="bridgestone_data_sync",
    deps=["dbt_snapshots"],
)
def dbt_seed(context: AssetExecutionContext, dbt: DbtCliResource):
    context.log.info("Executing DBT seed...")
    seed_result = dbt.cli(["seed", "-f"]).wait()
    if not seed_result.is_successful():
        raise Exception("DBT seed execution failed bridgestone_data_sync")
    return {"status": "dbt_seed_execution_completed"}


"""
4. DBT run
"""


@asset(
    name="dbt_run",
    group_name="bridgestone_data_sync",
    deps=["dbt_seed"],
)
def dbt_run(context: AssetExecutionContext, dbt: DbtCliResource):
    context.log.info("Executing DBT run...")
    execution_result = dbt.cli(["run", "--threads", "8"]).wait()
    if not execution_result.is_successful():
        raise Exception("DBT run failed for bridgestone_data_sync")
    return {"status": "dbt_run_execution_completed"}


"""
5. DBT clean
"""


@asset(
    name="dbt_clean",
    group_name="bridgestone_data_sync",
    deps=["dbt_run"],
)
def dbt_clean(context: AssetExecutionContext, dbt: DbtCliResource):
    context.log.info("Executing DBT clean...")
    result = dbt.cli(["clean"]).wait()
    if not result.is_successful():
        raise Exception("DBT clean execution failed bridgestone_data_sync")
    return {"status": "dbt_clean_execution_completed"}
