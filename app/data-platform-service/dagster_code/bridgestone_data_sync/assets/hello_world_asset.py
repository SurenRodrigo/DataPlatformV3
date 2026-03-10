from dagster import asset, AssetExecutionContext

"""
Placeholder asset that runs before any DBT asset.
Replace with actual assets in the real implementation.
"""


@asset(
    name="hello_world",
    group_name="bridgestone_data_sync",
    deps=[],
)
def hello_world(context: AssetExecutionContext):
    """Placeholder asset - will be replaced by actual implementation assets."""
    context.log.info("Hello world - placeholder asset executed.")
    return {"status": "hello_world_complete"}
