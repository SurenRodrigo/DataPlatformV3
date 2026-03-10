# PyAirbyte + Dagster Integration Guide

## Table of Contents
1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Step-by-Step Setup](#step-by-step-setup)
4. [Code Location Structure](#code-location-structure)
5. [Assets vs Utils Pattern](#assets-vs-utils-pattern)
6. [Creating PyAirbyte Assets](#creating-pyairbyte-assets)
7. [Creating DBT Assets](#creating-dbt-assets)
8. [Creating Jobs](#creating-jobs)
9. [Testing and Validation](#testing-and-validation)
10. [Troubleshooting](#troubleshooting)
11. [Best Practices](#best-practices)
12. [Example: Complete Connector Setup](#example-complete-connector-setup)

---

## Overview

This guide provides step-by-step instructions for creating new Dagster code locations that integrate PyAirbyte for data extraction and DBT for data transformation. The integration follows the platform's zero-coupling architecture principles and best practices for scalable data pipeline development.

### What You'll Learn

- How to create a new Dagster code location
- How to set up PyAirbyte custom connectors
- How to create DBT assets for data transformation
- How to orchestrate the complete pipeline with Dagster jobs
- How to test and validate your setup

---

## Prerequisites

Before starting, ensure you have:

1. **Platform Running**: Both platform and app services are running
   ```bash
   cd platform && ./start.sh
   cd app && ./start.sh
   ```

2. **Dagster UI Access**: Access to Dagster webserver (typically http://localhost:3030)

3. **Database Access**: PostgreSQL database is running and accessible

4. **Required Dependencies**: All Python packages are installed in the data-platform-service

---

## Step-by-Step Setup

### Step 1: Create Code Location Directory Structure

Create a new code location following the established pattern:

```bash
# Create the main code location directory
mkdir -p app/dagster_code/your_connector_name

# Create the required subdirectories
mkdir -p app/dagster_code/your_connector_name/assets
mkdir -p app/dagster_code/your_connector_name/jobs

# Create __init__.py files
touch app/dagster_code/your_connector_name/__init__.py
touch app/dagster_code/your_connector_name/assets/__init__.py
touch app/dagster_code/your_connector_name/jobs/__init__.py
```

### Step 2: Create Custom Connector Configuration

Create a YAML connector configuration in the data-platform-service:

```bash
# Create the connector configuration. NB: This should be already available, create only if not already there
mkdir -p app/data-manager/external-connectors
```

Create `app/data-manager/external-connectors/your-connector.yaml`:

```yaml
version: 1
name: source-your-connector
connector_type: source
image: airbyte/source-your-connector:latest
config:
  # Add your connector-specific configuration here
  api_key: "your_api_key"
  base_url: "https://api.example.com"
  # ... other configuration parameters
```

### Step 3: Create PyAirbyte Assets (Direct PostgreSQL Cache)

Create `app/dagster_code/your_connector_name/assets/sync_assets.py`:

```python
import sys
from dagster import asset, MetadataValue

# Add the data-manager path to sys.path for imports
sys.path.append('/app/data-manager')

from pyairbyte.utils.pyairbyte_sync import sync_connector

@asset(
    name="sync_your_connector",
    group_name="your_connector_name"
)
def sync_your_connector(context):
    """
    Sync data from your connector with PyAirbyte, writing directly to PostgreSQL
    cache schema 'pyairbyte_cache' (no DuckDB intermediary, no copy step).
    Optionally select specific streams via the second parameter.
    """
    result = sync_connector("source-your-connector", ["your_stream"])  # streams optional
    context.log.info(f"Sync result: {result}")

    if result.get('status') == 'success':
        context.add_output_metadata({
            "status": MetadataValue.text("success"),
            "cache_type": MetadataValue.text(result.get('cache_type', 'PostgresCache')),
            "cache_schema": MetadataValue.text(result.get('cache_schema', 'pyairbyte_cache')),
            "details": MetadataValue.json(result.get('result', {}))
        })
        context.log.info("Data successfully synced to PostgreSQL cache - ready for DBT")
    else:
        context.add_output_metadata({
            "status": MetadataValue.text("error"),
            "error": MetadataValue.text(result.get('error', 'Unknown error')),
            "cache_type": MetadataValue.text(result.get('cache_type', 'PostgresCache')),
            "cache_schema": MetadataValue.text(result.get('cache_schema', 'pyairbyte_cache'))
        })
        raise Exception(f"PyAirbyte sync failed: {result.get('error', 'Unknown error')}")

    return result
```

### Step 4: Create DBT Assets

Create `app/dagster_code/your_connector_name/assets/dbt_assets.py`:

```python
from dagster import asset, MetadataValue, AssetExecutionContext
from dagster_dbt import DbtCliResource

@asset(
    name="transform_your_connector_data",
    group_name="your_connector_name",
    deps=["sync_your_connector"]
)
def transform_your_connector_data(context: AssetExecutionContext, dbt: DbtCliResource):
    """
    Asset to transform your connector data using DBT models.
    """
    context.log.info("Starting DBT transformation for your connector data...")
    
    try:
        # Step 1: Generate DBT manifest
        context.log.info("Generating DBT manifest...")
        parse_result = dbt.cli(["parse"])
        for event in parse_result.stream():
            context.log.info(f"DBT Parse: {event}")
        
        # Step 2: Run DBT models for your connector
        context.log.info("Running DBT models: staging_your_table and your_table_reporting")
        
        run_result = dbt.cli(["run", "--select", "staging_your_table your_table_reporting"])
        
        # Check if the run was successful
        if run_result.is_successful():
            context.log.info("DBT transformation completed successfully")
            
            context.add_output_metadata({
                "status": MetadataValue.text("success"),
                "models_run": MetadataValue.text("staging_your_table, your_table_reporting"),
                "message": MetadataValue.text("DBT transformation completed")
            })
            
            return {
                "status": "success",
                "models_run": ["staging_your_table", "your_table_reporting"]
            }
        else:
            error_msg = "DBT transformation failed"
            context.log.error(error_msg)
            
            context.add_output_metadata({
                "status": MetadataValue.text("error"),
                "error": MetadataValue.text(error_msg)
            })
            
            raise Exception(error_msg)
        
    except Exception as e:
        context.log.error(f"Error during DBT transformation: {str(e)}")
        context.add_output_metadata({
            "status": MetadataValue.text("error"),
            "error": MetadataValue.text(str(e))
        })
        raise
```

### Step 5: Export Assets

Update `app/dagster_code/your_connector_name/assets/__init__.py`:

```python
# Assets package for your_connector_name code location
from .sync_assets import sync_your_connector
from .dbt_assets import transform_your_connector_data

__all__ = [
    "sync_your_connector",
    "transform_your_connector_data"
]
```

### Step 6: Create Jobs

Create `app/dagster_code/your_connector_name/jobs/pipeline_jobs.py`:

```python
from dagster import define_asset_job
from ..assets import sync_your_connector, transform_your_connector_data

# Define the sequential job for the complete pipeline
your_connector_pipeline_job = define_asset_job(
    name="your_connector_pipeline",
    selection=[
        "sync_your_connector",
        "transform_your_connector_data"
    ],
    description="Pipeline to sync your connector data directly to PostgreSQL cache and transform via DBT"
)
```

Update `app/dagster_code/your_connector_name/jobs/__init__.py`:

```python
# Jobs package for your_connector_name code location
from .pipeline_jobs import your_connector_pipeline_job

__all__ = [
    "your_connector_pipeline_job"
]
```

### Step 7: Create Main Definitions

Update `app/dagster_code/your_connector_name/__init__.py`:

```python
from dagster import Definitions
from dagster_dbt import DbtCliResource

# Import assets and jobs from the new package structure
from .assets import sync_your_connector, transform_your_connector_data
from .jobs import your_connector_pipeline_job

# DBT configuration
DBT_PROJECT_DIR = "/app/dbt_models"
DBT_PROFILES_DIR = "/root/.dbt"

# Define the dbt CLI resource
resources = {
    "dbt": DbtCliResource(
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
    )
}

defs = Definitions(
    assets=[sync_your_connector, transform_your_connector_data],
    resources=resources,
    schedules=[],
    jobs=[your_connector_pipeline_job],
)
```

### Step 8: Create DBT Models

Create the necessary DBT models for your connector:

1. **Source Definition**: `app/dbt_models/models/your_connector_sources.yml`
2. **Staging Model**: `app/dbt_models/models/staging/staging_your_table.sql`
3. **Reporting Model**: `app/dbt_models/models/your_table_reporting.sql`

#### 8.1: Source Definition Configuration

**Important**: When creating your source definition, you must specify the schema where your data is stored:

```yaml
version: 2

sources:
  - name: your_connector_name
    description: "Your connector data from external API via PyAirbyte"
    schema: pyairbyte_cache  # ⚠️ CRITICAL: Specify the schema
    tables:
      - name: your_table_name
        description: "Raw data from your connector"
        columns:
          # Define your columns here
          - name: id
            description: "Primary key"
            tests:
              - not_null
              - unique
          # ... other columns
```

**Key Points**:
- **Schema Specification**: Always add `schema: pyairbyte_cache` to your source definition
- **Column Mapping**: Ensure column names match the actual database table structure
- **Data Types**: Consider how your data is stored (text vs JSON, etc.)

#### 8.2: Staging Model Considerations

When creating your staging model, be aware of common issues:

**Column Name Mapping**: Database column names may be lowercase:
```sql
-- Instead of:
discountPercentage as discount_percentage,
availabilityStatus as availability_status,

-- Use:
discountpercentage as discount_percentage,
availabilitystatus as availability_status,
```

**JSON Text Handling**: If JSON data is stored as text, use proper casting:
```sql
-- Instead of:
meta->>'createdAt' as created_at,

-- Use:
(meta::json)->>'createdAt' as created_at,
```

**Example Staging Model Structure**:
```sql
{{
  config(
    materialized='table',
    schema='staging'
  )
}}

with source as (
    select * from {{ source('your_connector_name', 'your_table_name') }}
),

staged as (
    select
        -- Map columns to match actual database structure
        id as primary_key,
        title as item_title,
        -- Handle JSON text fields properly
        (metadata::json)->>'createdAt' as created_at,
        (metadata::json)->>'updatedAt' as updated_at,
        current_timestamp as dbt_loaded_at
    from source
)

select * from staged
```

### Step 9: Update Code Location Configuration

Add your new code location to the Dagster configuration. **This step requires updates to multiple files to ensure proper port configuration:**

#### 9.1: Update Code Locations Configuration

**Update `app/data-manager/resources/dagster/code-locations.json`**:

```json
{
  "code_locations": [
    {
      "name": "your_connector_name",
      "enabled": true,
      "description": "Your connector sync orchestration code location",
      "module": "dagster_code.your_connector_name",
      "port": 4268,
      "host": "data-platform-service",
      "metadata": {
        "team": "data",
        "domain": "your_connector_name",
        "version": "1.0.0"
      }
    }
  ],
  "config": {
    "workspace_name": "appbase-dagster-workspace",
    "description": "AppBase Data Platform Dagster Workspace",
    "version": "1.0.0"
  }
}
```

#### 9.2: Update Platform Workspace Configuration

**Update `platform/workspace.yaml`**:

```yaml
# Dagster Workspace Configuration - Multiple gRPC Servers
# Workspace: appbase-dagster-workspace
# Description: AppBase Data Platform Dagster Workspace

load_from:
  - grpc_server:
      host: data-platform-service
      port: 4274
      location_name: your_connector_name
      # Your connector sync orchestration code location
```

#### 9.3: Update Docker Port Exposures

**Update `app/docker-compose.yaml`** - Ensure your gRPC port is exposed on the data-platform-service:

```yaml
  data-platform-service:
    # ... existing configuration ...
    ports:
      - "4273:4273"  # bridgestone_data_sync gRPC server port
      - "4274:4274"  # your_connector_name gRPC server port (new)
```

#### 9.4: Verify Dockerfile Port Configuration

**Check `app/Dockerfile.data-platform-service`** - Ensure the EXPOSE directive includes your port:

```dockerfile
# Expose gRPC ports for all code locations
EXPOSE 4273 4274
```

**Note**: The Dockerfile already exposes multiple ports, so you may not need to change it unless you're using a port outside the current range.

#### 9.5: Port Assignment Guidelines

When choosing a port for your new code location:

- **Current Ports**: 4273 (bridgestone_data_sync)
- **Available Range**: 4274-4299 (reserved for new code locations)
- **Best Practice**: Use sequential ports (4274, 4275, 4276, etc.)
- **Avoid Conflicts**: Don't use ports already assigned to other services

#### 9.6: Platform Service Considerations

**No changes needed in platform services** because:

- **Zero-Coupling Architecture**: Platform services don't need to know about new code locations
- **Dynamic Discovery**: Dagster webserver discovers gRPC servers via workspace.yaml
- **Network Communication**: All communication happens via Docker network
- **Shared Storage**: All services use the same `dagster_shared_storage` volume

The platform services (Dagster webserver, daemon) will automatically discover your new code location once the workspace.yaml is updated and the gRPC server is running.

### Step 10: Rebuild and Test

```bash
# Rebuild the data-platform-service
docker compose -f app/docker-compose.yaml up -d --build data-platform-service

# Verify the code location loads
docker exec data-platform-service python3 -c "from dagster_code.your_connector_name import defs; print('✅ Code location loaded successfully')"
```

---

## Removing a Dagster Code Location

This section provides step-by-step instructions for completely removing a Dagster code location from the platform, including all associated assets, jobs, and configurations.

### Prerequisites

Before removing a code location, ensure:
1. **No Active Runs**: No jobs from the code location are currently running
2. **Backup Data**: Consider backing up any important data or configurations
3. **Platform Access**: You have access to all configuration files

### Step-by-Step Removal Process

#### Step 1: Stop Platform Services

```bash
# Stop the platform services to prevent conflicts
cd platform && ./stop.sh

# Stop the app services
cd app && ./stop.sh
```

#### Step 2: Remove Code Location Directory

Remove the entire code location directory:

```bash
# Remove the code location directory
rm -rf app/dagster_code/your_connector_name

# Verify removal
ls -la app/dagster_code/
```

#### Step 2.5: Clear Python Cache (CRITICAL)

**⚠️ IMPORTANT**: Clear Python cache files to prevent import errors and stale references:

```bash
# Clear all Python cache files from dagster_code directory
find app/dagster_code -name "*.pyc" -delete
find app/dagster_code -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true

# Verify cache is cleared
find app/dagster_code -name "*.pyc" -o -name "__pycache__" | wc -l
# Should return 0 if cache is properly cleared
```

**Why this is critical**: Python cache files can contain references to removed modules, causing import errors and preventing the data-platform-service from starting properly.

#### Step 3: Remove Connector Configuration

Remove the connector YAML file:

```bash
# Remove the connector configuration
rm app/data-manager/external-connectors/your-connector.yaml

# Verify removal
ls -la app/data-manager/external-connectors/
```

#### Step 4: Remove DBT Models

Remove all DBT models associated with the code location:

```bash
# Remove source definition
rm app/dbt_models/models/your_connector_sources.yml

# Remove staging model
rm app/dbt_models/models/staging/staging_your_table.sql

# Remove reporting model
rm app/dbt_models/models/your_table_reporting.sql

# Verify removal
ls -la app/dbt_models/models/
ls -la app/dbt_models/models/staging/
```

#### Step 5: Update Code Location Configuration

**Update `app/data-manager/resources/dagster/code-locations.json`**:

Remove the entry for your code location:

```json
{
  "code_locations": [
    {
      "name": "bridgestone_data_sync",
      "enabled": true,
      "description": "Bridgestone data sync code location for Bridgestone data pipeline",
      "module": "dagster_code.bridgestone_data_sync",
      "port": 4273,
      "host": "data-platform-service",
      "metadata": {
        "team": "data",
        "domain": "bridgestone_data_sync",
        "version": "1.0.0"
      }
    }
    // ⚠️ REMOVED: your_connector_name entry
  ],
  "config": {
    "workspace_name": "appbase-dagster-workspace",
    "description": "AppBase Data Platform Dagster Workspace (bridgestone_data_sync only)",
    "version": "1.0.0"
  }
}
```

#### Step 6: Update Platform Workspace Configuration

**Update `platform/workspace.yaml`**:

Remove the gRPC server entry for your code location:

```yaml
# Dagster Workspace Configuration - Multiple gRPC Servers
# Description: AppBase Data Platform Dagster Workspace (bridgestone_data_sync only)

load_from:
  - grpc_server:
      host: data-platform-service
      port: 4273
      location_name: bridgestone_data_sync
      # Bridgestone data sync code location
  # ⚠️ REMOVED: your_connector_name gRPC server entry
```

#### Step 7: Update Docker Port Exposures (Optional)

**Update `app/docker-compose.yaml`** - Remove the port for your code location:

```yaml
  data-platform-service:
    # ... existing configuration ...
    ports:
      - "4273:4273"  # bridgestone_data_sync gRPC server port
      # ⚠️ REMOVED: your_connector_name port
```

**Note**: If you're removing the last code location that uses a specific port, you can remove the port exposure. If other code locations still use ports in the same range, keep the port exposures.

#### Step 8: Update Dockerfile (Optional)

**Update `app/Dockerfile.data-platform-service`** - Remove the port from EXPOSE directive:

```dockerfile
# Expose gRPC ports for all code locations
EXPOSE 4273  # Only expose ports that are actually used
```

**Note**: Only remove ports if no other code locations use them.

#### Step 9: Clean Up Database Data (Optional)

If you want to remove all data associated with the code location:

```bash
# Connect to database and remove cache tables
docker exec -e PGPASSWORD=dataplatpassword data-platform-service psql -h db -U dataplatuser -d dataplatform -c "DROP SCHEMA IF EXISTS pyairbyte_cache CASCADE;"

# Remove DBT-generated tables (if they exist)
docker exec -e PGPASSWORD=dataplatpassword data-platform-service psql -h db -U dataplatuser -d dataplatform -c "DROP TABLE IF EXISTS staging.staging_your_table CASCADE;"
docker exec -e PGPASSWORD=dataplatpassword data-platform-service psql -h db -U dataplatuser -d dataplatform -c "DROP TABLE IF EXISTS reporting.your_table_reporting CASCADE;"
```

**⚠️ Warning**: This will permanently delete all data associated with the code location. Only do this if you're sure you want to remove all data.

#### Step 10: Rebuild and Verify Removal

```bash
# Rebuild the data-platform-service
docker compose -f app/docker-compose.yaml up -d --build data-platform-service

# Start platform services
cd platform && ./start.sh

# Verify the code location is no longer loaded
docker exec data-platform-service python3 -c "import sys; sys.path.append('/app'); from dagster_code import your_connector_name; print('❌ Code location still exists')" 2>/dev/null || echo "✅ Code location successfully removed"

# Check Dagster UI to verify the code location is gone
# Access http://localhost:3030 and verify:
# - No assets from your_connector_name group
# - No jobs from your_connector_name
# - No code location in the workspace
```

### Verification Checklist

After removal, verify:

- [ ] **Code Location Directory**: `app/dagster_code/your_connector_name` is deleted
- [ ] **Python Cache Cleared**: No `.pyc` files or `__pycache__` directories remain
- [ ] **Connector Configuration**: `app/data-manager/external-connectors/your-connector.yaml` is deleted
- [ ] **DBT Models**: All related DBT models are deleted
- [ ] **Code Location Config**: Entry removed from `code-locations.json`
- [ ] **Workspace Config**: gRPC server entry removed from `workspace.yaml`
- [ ] **Docker Ports**: Port removed from `docker-compose.yaml` (if applicable)
- [ ] **Dockerfile**: Port removed from EXPOSE directive (if applicable)
- [ ] **Database Data**: Cache tables and DBT tables removed (if applicable)
- [ ] **Dagster UI**: Code location no longer appears in the UI
- [ ] **Service Logs**: No errors related to the removed code location
- [ ] **Import Tests**: No import errors when testing remaining code locations

### Troubleshooting Removal Issues

#### Issue 1: Code Location Still Appears in Dagster UI

**Symptoms**: Code location still shows up in Dagster UI after removal

**Solutions**:
```bash
# Restart platform services to refresh workspace
cd platform && ./stop.sh && ./start.sh

# Check if gRPC server is still running
docker exec data-platform-service ps aux | grep dagster

# Force restart data-platform-service
docker compose -f app/docker-compose.yaml restart data-platform-service
```

#### Issue 2: Import Errors After Removal

**Symptoms**: Python import errors when trying to import removed code location

**Solutions**:
```bash
# Clear Python cache (both locally and in container)
find app/data-platform-service/dagster_code -name "*.pyc" -delete
find app/data-platform-service/dagster_code -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
docker exec data-platform-service find /app -name "*.pyc" -delete
docker exec data-platform-service find /app -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true

# Rebuild data-platform-service
docker compose -f app/docker-compose.yaml up -d --build data-platform-service
```

**Prevention**: Always clear Python cache when removing code locations to avoid stale references.

#### Issue 3: Port Conflicts

**Symptoms**: Port already in use errors when adding new code locations

**Solutions**:
```bash
# Check which processes are using the port
docker exec data-platform-service netstat -tlnp | grep :4273

# Kill any remaining processes
docker exec data-platform-service pkill -f "dagster.*4273" || true
```

#### Issue 4: Stale Python Cache After Removal

**Symptoms**: 
- `ModuleNotFoundError` for removed code locations
- `ImportError` when trying to import removed modules
- gRPC server fails to start due to import issues

**Solutions**:
```bash
# Clear Python cache locally
find app/dagster_code -name "*.pyc" -delete
find app/dagster_code -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true

# Clear Python cache in container
docker exec data-platform-service find /app -name "*.pyc" -delete
docker exec data-platform-service find /app -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true

# Verify cache is cleared
find app/data-platform-service/dagster_code -name "*.pyc" -o -name "__pycache__" | wc -l
# Should return 0

# Rebuild and restart services
docker compose -f app/docker-compose.yaml up -d --build data-platform-service
```

**Prevention**: Always clear Python cache immediately after removing code locations.

### Best Practices for Removal

1. **Document Before Removal**: Keep a record of what was removed for future reference
2. **Test After Removal**: Verify that remaining code locations still work correctly
3. **Clean Up Thoroughly**: Remove all associated files, configurations, and data
4. **Update Documentation**: Update any documentation that referenced the removed code location
5. **Notify Team**: Inform team members about the removal to avoid confusion

### Example: Removing the `airbyte_sync` Code Location

Here's a complete example of removing the `airbyte_sync` code location:

```bashkl
# Step 1: Stop services
cd platform && ./stop.sh
cd app && ./stop.sh

# Step 2: Remove code location directory
rm -rf app/dagster_code/airbyte_sync

# Step 2.5: Clear Python cache (CRITICAL)
find app/dagster_code -name "*.pyc" -delete
find app/dagster_code -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true

# Step 3: Remove connector configuration
rm app/data-manager/external-connectors/example-faker.yaml

# Step 4: Remove DBT models
rm app/dbt_models/models/pyairbyte_cache_sources.yml
rm app/dbt_models/models/staging/staging_faker_users.sql
rm app/dbt_models/models/faker_users_reporting.sql

# Step 5: Update code-locations.json (remove airbyte_sync entry)
# Step 6: Update workspace.yaml (remove airbyte_sync gRPC server)
# Step 7: Update docker-compose.yaml (remove port)
# Step 8: Update Dockerfile.data-platform-service (remove port from EXPOSE)

# Step 9: Clean up database (optional)
docker exec -e PGPASSWORD=dataplatpassword data-platform-service psql -h db -U dataplatuser -d dataplatform -c "DROP SCHEMA IF EXISTS pyairbyte_cache CASCADE;"

# Step 10: Rebuild and verify
docker compose -f app/docker-compose.yaml up -d --build data-platform-service
cd platform && ./start.sh
```

---

## Code Location Structure

Your final structure should look like this:

```
app/dagster_code/your_connector_name/
├── __init__.py              # Main Definitions
├── assets/
│   ├── __init__.py         # Assets package exports
│   ├── sync_assets.py      # PyAirbyte sync assets
│   └── dbt_assets.py       # DBT transformation assets
└── jobs/
    ├── __init__.py         # Jobs package exports
    └── pipeline_jobs.py    # Pipeline job definitions
```

---

## Assets vs Utils Pattern

### Core Principle

The platform follows a **clear separation of concerns** pattern:

- **Assets** = Business Logic (WHAT to do)
- **Utils** = Supporting Methods (HOW to do it)

This pattern ensures maintainable, testable, and reusable code by separating business decisions from technical implementation.

### Pattern Structure

#### Asset Responsibilities (Business Logic)

Assets define **what needs to be done** and handle **Dagster-specific concerns**:

- Define business goals (sync connector X, process events Y, transform data Z)
- Handle Dagster context and metadata
- Manage asset dependencies (`deps=[]`)
- Provide error handling and logging
- Return values and status reporting
- Orchestrate utility functions to achieve business goals

#### Utils Responsibilities (Supporting Methods)

Utils provide **reusable, technical implementations**:

- Handle low-level operations (API calls, database operations, data transformations)
- Manage connections, caching, and error handling
- Are framework-agnostic (no Dagster dependencies)
- Can be tested independently
- Can be reused across multiple assets

### Example Patterns

#### Pattern 1: PyAirbyte Sync Pattern

**Asset (Business Logic)**:
```python
import sys
from dagster import asset, MetadataValue, AssetExecutionContext

sys.path.append('/app/data-manager')
from pyairbyte.utils.pyairbyte_sync import sync_connector
from pyairbyte.utils.common_cache import get_cache

@asset(
    name="sync_api_data",
    group_name="my_data_sync",
    deps=["cleanup_cache"]
)
def sync_api_data(context: AssetExecutionContext):
    """
    Business logic: Sync data from API connector using cache configuration.
    """
    # Business decision: Use default cache
    cache = get_cache("default", "api_connector")
    
    # Business decision: Sync this specific connector
    result = sync_connector("api_connector", cache=cache)
    
    # Dagster-specific: Handle metadata, logging, errors
    if result.get('status') == 'success':
        context.add_output_metadata({
            "status": MetadataValue.text("success"),
            "cache_schema": MetadataValue.text(result.get('cache_schema')),
            "details": MetadataValue.json(result['result'])
        })
        context.log.info("Data successfully synced to PostgreSQL cache")
        return result
    else:
        context.add_output_metadata({
            "status": MetadataValue.text("error"),
            "error": MetadataValue.text(result.get('error', 'Unknown error'))
        })
        context.log.error(f"Sync failed: {result.get('error')}")
        raise Exception(f"PyAirbyte sync failed: {result.get('error')}")
```

**Utils (Supporting Methods)**:
```python
# pyairbyte/utils/pyairbyte_sync.py
def sync_connector(connector_name: str, streams_to_sync: Optional[List[str]] = None, cache: Optional[PostgresCache] = None) -> dict:
    """
    Technical implementation: Load connector, validate, sync data to cache.
    """
    config = get_connector_by_name(connector_name)
    source = ab.get_source(config['name'], config=config['config'])
    source.check()
    result = source.read(cache=cache)
    return {'status': 'success', 'result': {...}}

# pyairbyte/utils/common_cache.py
def get_cache(cache_name: str, connector_name: str = None) -> PostgresCache:
    """
    Technical implementation: Create PostgresCache with proper configuration.
    """
    cache_config = CACHE_CONFIGS[cache_name].copy()
    if connector_name:
        cache_config['table_prefix'] = f"{connector_name.replace('-', '_')}_"
    return PostgresCache(**cache_config)
```

#### Pattern 2: Event Processing Pattern

**Asset (Business Logic)**:
```python
import sys
from dagster import asset, AssetExecutionContext

sys.path.append('/app/data-manager')
from pyairbyte.utils.graphql_util import query_graphql_api
from pyairbyte.utils.event_store import bulk_write_events

@asset(name="data_event_gen", group_name="data_integrations")
def data_event_gen(context: AssetExecutionContext):
    """
    Business logic: Query data and create DATA_SYNC_EVENT events.
    """
    # Business decision: Query data from GraphQL
    query = """
    query {
      records {
        id
        name
        status
      }
    }
    """
    result = query_graphql_api(query)
    records = result.get('data', {}).get('records', [])
    
    # Business logic: Transform records into events
    events_to_insert = []
    for record in records:
        events_to_insert.append({
            "event_type": "DATA_SYNC_EVENT",
            "event_data": {
                "record_id": record.get('id'),
                "name": record.get('name'),
                "status": record.get('status')
            }
        })
    
    # Business decision: Bulk insert events
    bulk_result = bulk_write_events(events_to_insert)
    
    # Dagster-specific: Logging and return
    context.log.info(f"Created {bulk_result['events_created']} events")
    return bulk_result
```

**Utils (Supporting Methods)**:
```python
# pyairbyte/utils/graphql_util.py
def query_graphql_api(query: str, variables: Optional[Dict] = None, ...) -> Dict:
    """
    Technical implementation: Make HTTP request to Hasura GraphQL API.
    """
    response = requests.post(hasura_url, json={'query': query, 'variables': variables}, ...)
    return response.json()

# pyairbyte/utils/event_store.py
def bulk_write_events(events_to_insert: List[Dict]) -> Dict:
    """
    Technical implementation: Hash events, check duplicates, batch insert.
    """
    for event in events_to_insert:
        event_hash = _create_event_hash(event['event_type'], event['event_data'])
        if not _check_hash_exists(event_hash):
            # Insert event via GraphQL mutation
    return {'status': 'success', 'events_created': count, ...}
```

#### Pattern 3: Database Sync Pattern

**Asset (Business Logic)**:
```python
import sys
from dagster import asset, AssetExecutionContext

sys.path.append('/app/data-manager')
from pyairbyte.utils.mssql_sync import sync_mssql_tables

@asset(name="mssql_sync_assets", group_name="data_sync")
def mssql_sync_assets(context: AssetExecutionContext):
    """
    Business logic: Sync specific MSSQL tables to PostgreSQL cache.
    """
    # Business decision: Sync these specific tables
    result = sync_mssql_tables(
        server="mssql-server.example.com",
        database="source_db",
        tables=["table1", "table2"],
        cache_schema="pyairbyte_cache"
    )
    
    # Dagster-specific: Handle result
    if result.get('status') == 'success':
        context.log.info(f"Synced {result['tables_synced']} tables")
    else:
        raise Exception(f"Sync failed: {result.get('error')}")
    
    return result
```

**Utils (Supporting Methods)**:
```python
# pyairbyte/utils/mssql_sync.py
def sync_mssql_tables(server: str, database: str, tables: List[str], cache_schema: str) -> Dict:
    """
    Technical implementation: Connect to MSSQL, read data, map types, write to PostgreSQL.
    """
    conn = pyodbc.connect(f"DRIVER={{ODBC Driver}};SERVER={server};DATABASE={database};...")
    for table in tables:
        df = pd.read_sql(f"SELECT * FROM {table}", conn)
        # Map MSSQL types to PostgreSQL types
        # Write to PostgreSQL cache
    return {'status': 'success', 'tables_synced': len(tables), ...}
```

### Available Utility Modules

The platform provides several utility modules in `app/data-manager/pyairbyte/utils/`:

#### 1. **`pyairbyte_sync.py`** - PyAirbyte Connector Syncing
- `sync_connector(connector_name, streams_to_sync=None, cache=None)` - Main sync function
- Handles connector loading, validation, stream selection, and data sync
- **Used by**: Sync assets that use PyAirbyte connectors

#### 2. **`common_cache.py`** - Cache Management
- `get_cache(cache_name, connector_name=None)` - Get configured PostgresCache
- `CACHE_CONFIGS` - Cache configurations (default)
- **Used by**: Assets requiring cache configurations

#### 3. **`event_store.py`** - Event Processing
- `bulk_write_events(events_to_insert)` - Bulk event insertion with duplicate detection
- `get_unprocessed_or_failed_events(event_type)` - Retrieve events for processing
- `write_event(event_type, event_data)` - Single event write
- **Used by**: Event-driven processing assets

#### 4. **`graphql_util.py`** - GraphQL Operations
- `query_graphql_api(query, variables=None, ...)` - Execute GraphQL queries against Hasura
- Handles authentication, error handling, and response parsing
- **Used by**: Event generation assets, cleanup assets

#### 5. **`api_call.py`** - API Operations
- `call_api_for_event_processing(event_id, method, url, body)` - Make API calls with event logging
- Automatically logs API call results to event_store
- **Used by**: Event handler assets

#### 6. **`mssql_sync.py`** - MSSQL Database Sync
- `sync_mssql_tables(server, database, tables, cache_schema)` - Sync MSSQL tables to PostgreSQL
- Handles type mapping, data transformation, and batch insertion
- **Used by**: MSSQL sync assets

#### 7. **`mysql_sync.py`** - MySQL Database Sync
- `sync_mysql_tables(server, database, tables, cache_schema)` - Sync MySQL tables to PostgreSQL
- Similar to MSSQL sync but for MySQL databases
- **Used by**: MySQL sync assets

#### 8. **`cache_db_manager.py`** - Cache Database Management
- `PyAirbyteCacheDBManager` - Class for managing cache schemas and tables
- Provides methods for cleanup, schema management, and table operations
- **Used by**: Cleanup assets

### Standard Asset Template

When creating new assets, follow this template:

```python
import sys
from dagster import asset, MetadataValue, AssetExecutionContext

# Add the data-manager path to sys.path for imports
sys.path.append('/app/data-manager')

# Import utility functions needed for this asset
from pyairbyte.utils.{utility_module} import {utility_function}

@asset(
    name="{asset_name}",
    group_name="{code_location}",
    deps=["{dependency_asset}"]  # Optional: List dependent assets
)
def {asset_name}(context: AssetExecutionContext):
    """
    Business logic description: What this asset does and why.
    """
    try:
        # Step 1: Business decision - Configure what to do
        config = {...}
        cache = get_cache("cache_name", "connector_name")  # If needed
        
        # Step 2: Call utility function(s) to accomplish goal
        result = utility_function(
            param1=config,
            param2=cache,  # If needed
            ...
        )
        
        # Step 3: Dagster-specific - Handle success
        if result.get('status') == 'success':
            context.add_output_metadata({
                "status": MetadataValue.text("success"),
                "details": MetadataValue.json(result.get('result', {})),
                # Add other relevant metadata
            })
            context.log.info("Success message describing what was accomplished")
            return result
        else:
            # Step 4: Dagster-specific - Handle error
            context.add_output_metadata({
                "status": MetadataValue.text("error"),
                "error": MetadataValue.text(result.get('error', 'Unknown error'))
            })
            context.log.error(f"Error message: {result.get('error')}")
            raise Exception(f"Operation failed: {result.get('error')}")
            
    except Exception as e:
        # Step 5: Dagster-specific - Handle exceptions
        context.log.error(f"Unexpected error: {str(e)}")
        raise
```

### Pattern Benefits

1. **Separation of Concerns**: Business logic is separate from technical implementation
2. **Reusability**: Utils can be used across multiple assets, reducing code duplication
3. **Testability**: Utils can be tested independently; assets can be tested with mocked utils
4. **Maintainability**: Changes to technical implementation only affect utils; business logic changes only affect assets
5. **Consistency**: Standardized utility functions ensure consistent behavior across all assets

### Best Practices

1. **Always use utils for technical operations**: Don't implement low-level operations (API calls, database operations) directly in assets
2. **Keep assets focused on business logic**: Assets should orchestrate utils, not implement technical details
3. **Use descriptive asset names**: Asset names should clearly indicate what business goal they accomplish
4. **Add comprehensive metadata**: Use `context.add_output_metadata()` to provide useful information for monitoring and debugging
5. **Handle errors gracefully**: Always check result status and provide meaningful error messages
6. **Document business decisions**: Use docstrings to explain why the asset does what it does, not just what it does
7. **Leverage existing utils**: Before creating new utility functions, check if existing utils can be reused or extended

### When to Create New Utils

Create new utility functions when:

- You need to perform a technical operation that doesn't fit existing utils
- You have reusable logic that multiple assets will use
- You need to abstract complex operations for better testability
- You want to standardize how certain operations are performed

**Example**: If you need to sync data from a new source type (e.g., MongoDB), create `mongodb_sync.py` in utils rather than implementing the sync logic directly in assets.

---

## Creating PyAirbyte Assets

### Key Components

1. **Sync Asset**: Connects to external source and syncs data directly to PostgreSQL `pyairbyte_cache`
2. **No Copy Step**: DuckDB intermediary not used; DBT reads from PostgreSQL cache
3. **Error Handling**: Comprehensive error handling and metadata exposure
4. **Dependencies**: DBT transform depends on sync asset

### Best Practices

- Use descriptive asset names that include the connector name
- Add comprehensive logging and metadata
- Handle errors gracefully with proper exception handling
- Use asset dependencies to ensure proper execution order

---

## Creating DBT Assets

### Key Components

1. **DBT Resource**: Configured with project and profiles directories
2. **Manifest Generation**: Always parse before running models
3. **Model Selection**: Use specific model selection for targeted runs
4. **Success Validation**: Check run results and handle failures

### Best Practices

- Always generate the DBT manifest before running models
- Use specific model selection to avoid running unnecessary models
- Validate run success and provide detailed error messages
- Add comprehensive metadata for monitoring and debugging

---

## Creating Jobs

### Key Components

1. **Asset Selection**: Define which assets to include in the job
2. **Execution Order**: Assets run in dependency order automatically
3. **Description**: Clear description of what the job does
4. **Sequential Execution**: Ensures proper data flow

### Best Practices

- Include all related assets in the job
- Use descriptive job names
- Add clear descriptions for documentation
- Test job execution thoroughly

---

## Testing and Validation

### 1. Code Location Loading

```bash
docker exec data-platform-service python3 -c "from dagster_code.your_connector_name import defs; print(f'Assets: {len(defs.assets)}'); print(f'Jobs: {len(defs.jobs)}')"
```

### 2. Individual Asset Testing

Test each asset individually in the Dagster UI:
1. Go to Assets tab
2. Find your assets
3. Click "Materialize" to test each one

### 3. Job Testing

Test the complete pipeline:
1. Go to Jobs tab
2. Find your pipeline job
3. Click "Launch Run" to execute the complete pipeline

### 4. Data Validation

Verify data flows correctly:
```bash
# Check PostgreSQL cache
docker exec -e PGPASSWORD=dataplatpassword data-platform-service psql -h db -U dataplatuser -d dataplatform -c "SELECT COUNT(*) FROM pyairbyte_cache.your_table;"

# Check final reporting table
docker exec -e PGPASSWORD=dataplatpassword data-platform-service psql -h db -U dataplatuser -d dataplatform -c "SELECT COUNT(*) FROM reporting.your_table_reporting;"
```

### 5. DBT Model Testing

Test DBT models individually before running the complete pipeline:
```bash
# Test DBT parse
docker exec data-platform-service bash -c "cd /app/dbt_models && dbt parse"

# Test staging model
docker exec data-platform-service bash -c "cd /app/dbt_models && dbt run --select staging_your_table"

# Test reporting model
docker exec data-platform-service bash -c "cd /app/dbt_models && dbt run --select your_table_reporting"
```

### 6. Database Schema Verification

Check the actual database structure to ensure your models match:
```bash
# Check table structure
docker exec -e PGPASSWORD=dataplatpassword data-platform-service psql -h db -U dataplatuser -d dataplatform -c "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'pyairbyte_cache' AND table_name = 'your_table' ORDER BY ordinal_position;"

# Check sample data
docker exec -e PGPASSWORD=dataplatpassword data-platform-service psql -h db -U dataplatuser -d dataplatform -c "SELECT * FROM pyairbyte_cache.your_table LIMIT 3;"
```

---

## Port Management and Configuration

### Port Assignment Strategy

The platform uses a **configuration-driven port assignment** system:

- **Port Range**: 4273-4299 (reserved for Dagster code locations)
- **Current Assignments**:
  - `4273`: bridgestone_data_sync
- **Available Range**: 4274-4299 for new code locations
- **Best Practice**: Use sequential ports to avoid conflicts

### Configuration Files Overview

When adding a new code location, you must update **4 key files**:

1. **`app/data-manager/resources/dagster/code-locations.json`**
   - Defines the code location with port, module, and metadata
   - Used by `dagster-init.sh` to start gRPC servers

2. **`platform/workspace.yaml`**
   - Tells Dagster webserver where to find gRPC servers
   - Must match the ports defined in code-locations.json

3. **`app/docker-compose.yaml`**
   - Exposes ports from container to host
   - Enables network communication between platform and app

4. **`app/Dockerfile.data-platform-service`**
   - Documents which ports the container exposes
   - Helps with container orchestration

### Zero-Coupling Architecture Benefits

**No platform service changes required** because:

- **Dynamic Discovery**: Dagster webserver discovers gRPC servers via workspace.yaml
- **Network Isolation**: All communication via Docker network
- **Shared Storage**: All services use same `dagster_shared_storage` volume
- **Configuration-Driven**: Platform services don't need to know about new code locations

## Troubleshooting

### Common Issues

1. **Import Errors**
   - Ensure `sys.path.append('/app/data-manager')` is in sync assets (this is the path inside the container)
   - Verify all required packages are installed

2. **DBT Errors**
   - Check DBT project and profiles configuration
   - Verify models exist and are properly configured
   - Check database connectivity

3. **PyAirbyte Errors**
   - Verify connector configuration in YAML file
   - Check connector installation and dependencies
   - Validate cache file creation

4. **Code Location Loading Errors**
   - Check all `__init__.py` files exist
   - Verify import statements are correct
   - Check for syntax errors in Python files

5. **Port Configuration Errors**
   - **Port Already in Use**: Check if port is already assigned in `code-locations.json`
   - **gRPC Server Not Starting**: Verify port is exposed in `docker-compose.yaml`
   - **Code Location Not Discovered**: Check `workspace.yaml` has correct port and location_name
   - **Port Range Conflicts**: Ensure port is within 4273-4299 range
   - **Docker Port Not Exposed**: Verify port is listed in `EXPOSE` directive in Dockerfile

6. **DB Bridge Method Errors**
   - **AttributeError: 'PyAirbyteDBBridge' object has no attribute 'copy_connector_data'**
     - **Solution**: Use `copy_duckdb_to_postgres(cache_path, 'table_name', 'pyairbyte_cache')` instead
     - **Correct Method**: The method is `copy_duckdb_to_postgres`, not `copy_connector_data`

7. **DBT Source Configuration Errors**
   - **Error**: `relation "your_connector.products" does not exist`
   - **Solution**: Add `schema: pyairbyte_cache` to your source definition in `your_connector_sources.yml`
   - **Example**:
     ```yaml
     sources:
       - name: your_connector_name
         schema: pyairbyte_cache  # ⚠️ CRITICAL
         tables:
           - name: your_table_name
     ```

8. **DBT Column Mapping Errors**
   - **Error**: `operator does not exist: text ->> unknown`
   - **Solution**: Use proper JSON casting: `(meta::json)->>'createdAt'` instead of `meta->>'createdAt'`
   - **Column Names**: Database columns may be lowercase (e.g., `discountpercentage` not `discountPercentage`)

9. **Declarative Source Configuration Errors**
   - **Error**: `AirbyteConnectorConfigurationMissingError`
   - **Solution**: For declarative sources (No Code connector builder), ensure proper YAML structure and use `source_manifest` parameter
   - **Note**: Declarative sources require special handling in `pyairbyte_sync.py`

### Debugging Steps

1. **Check Logs**: Review container logs for detailed error messages
2. **Test Imports**: Test individual imports in the container
3. **Verify Configuration**: Check all configuration files
4. **Test Components**: Test each component individually

---

## Best Practices

### Code Organization

1. **Consistent Naming**: Use consistent naming conventions across all files
2. **Separation of Concerns**: Keep sync, copy, and transform logic separate
3. **Modular Design**: Design for reusability and maintainability
4. **Documentation**: Add comprehensive docstrings and comments

### Error Handling

1. **Graceful Failures**: Handle errors gracefully with proper logging
2. **Metadata Exposure**: Expose relevant metadata for monitoring
3. **Retry Logic**: Implement retry logic where appropriate
4. **Validation**: Validate inputs and outputs at each step

### Performance

1. **Efficient Data Transfer**: Use efficient methods for data copying
2. **Resource Management**: Properly manage database connections
3. **Monitoring**: Add performance metrics and monitoring
4. **Scaling**: Design for horizontal scaling

---

## Example: Complete Connector Setup

See the existing `sample_product_sync` code location for a complete working example:

- **Location**: `app/data-platform-service/dagster_code/bridgestone_data_sync/`
- **Assets**: `hello_world_asset`, `sync_invoice_data`, `sync_credit_data`, `sync_wwi_invoices`
- **Jobs**: `bridgestone_data_sync_job`, `sync_data_job`
- **Port/Workspace**:
  - `app/data-platform-service/data-manager/resources/dagster/code-locations.json` entry with `port: 4273`, `module: dagster_code.bridgestone_data_sync`
  - `platform/workspace.yaml` gRPC entry: host `data-platform-service`, port `4273`, location `bridgestone_data_sync`

This example demonstrates the current best-practice: ExcelToDbWriter writes directly to PostgreSQL cache, and DBT can read from `pyairbyte_cache` schema.

---

## Common Fixes and Solutions

Based on real-world implementation experience, here are the most common fixes needed:

### Fix 1: DB Bridge Method Call

**Problem**: `AttributeError: 'PyAirbyteDBBridge' object has no attribute 'copy_connector_data'`

**Solution**: Use the correct method name:
```python
# ❌ Incorrect
copy_success = bridge.copy_connector_data(cache_path, 'table_name')

# ✅ Correct
copy_success = bridge.copy_duckdb_to_postgres(cache_path, 'table_name', 'pyairbyte_cache')
```

### Fix 2: DBT Source Schema Configuration

**Problem**: `relation "your_connector.products" does not exist`

**Solution**: Add schema specification to your source definition:
```yaml
# ❌ Missing schema
sources:
  - name: your_connector_name
    tables:
      - name: your_table_name

# ✅ With schema specification
sources:
  - name: your_connector_name
    schema: pyairbyte_cache  # ⚠️ CRITICAL
    tables:
      - name: your_table_name
```

### Fix 3: Database Column Name Mapping

**Problem**: Column name mismatches between expected and actual database structure

**Solution**: Use actual database column names (often lowercase):
```sql
-- ❌ Expected camelCase
discountPercentage as discount_percentage,
availabilityStatus as availability_status,

-- ✅ Actual lowercase
discountpercentage as discount_percentage,
availabilitystatus as availability_status,
```

### Fix 4: JSON Text Field Handling

**Problem**: `operator does not exist: text ->> unknown`

**Solution**: Use proper JSON casting for text fields:
```sql
-- ❌ Direct JSON operator on text
meta->>'createdAt' as created_at,

-- ✅ Proper JSON casting
(meta::json)->>'createdAt' as created_at,
```

### Fix 5: Declarative Source Configuration

**Problem**: `AirbyteConnectorConfigurationMissingError` with declarative sources

**Solution**: Ensure proper YAML structure and use `source_manifest` parameter in `pyairbyte_sync.py`:
```python
# For declarative sources (No Code connector builder)
source = ab.get_source(
    config['name'],
    source_manifest=source_manifest_dict,  # Pass YAML content as dict
    config={},  # Provide empty config
    install_if_missing=True
)
```

---

## References

- [Dagster Documentation](https://docs.dagster.io/)
- [PyAirbyte Documentation](https://docs.airbyte.com/connector-development/cdk-python/)
- [DBT Documentation](https://docs.getdbt.com/)
- [Platform Architecture](./2-platform-architecture.md)
- [Dagster Architecture](./11-platform-architecture-dagster.md)

---

**For additional support, consult the platform documentation and Dagster community resources.** 