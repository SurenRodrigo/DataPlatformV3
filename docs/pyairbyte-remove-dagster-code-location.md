# Removing Dagster Code Locations - Complete Guide

## Table of Contents
1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Step-by-Step Removal Process](#step-by-step-removal-process)
4. [Verification Commands](#verification-commands)
5. [Troubleshooting](#troubleshooting)
6. [Example: Removing a Code Location](#example-removing-a-code-location)
7. [Best Practices](#best-practices)

---

## Overview

This guide provides step-by-step instructions for completely removing a Dagster code location from the platform, including all associated assets, jobs, configurations, and data. The process follows the platform's zero-coupling architecture principles and ensures clean removal without affecting other code locations.

### What This Guide Covers

- Complete removal of Dagster code location directories
- Removal of associated connector configurations
- Cleanup of DBT models and source definitions
- Updates to all configuration files
- Database cleanup and data removal
- Verification of successful removal
- Troubleshooting common issues

---

## Prerequisites

Before starting the removal process, ensure you have:

1. **Platform Access**: Access to all configuration files and services
2. **No Active Runs**: No jobs from the code location are currently running
3. **Backup Consideration**: Consider backing up any important data or configurations
4. **Understanding**: Know which code location you want to remove and its dependencies

---

## Step-by-Step Removal Process

### Step 1: Stop Platform Services

Stop all platform and app services to prevent conflicts during removal:

```bash
# Stop platform services
cd platform && ./stop.sh

# Stop app services
cd app && ./stop.sh
```

### Step 2: Remove Code Location Directory

Remove the entire code location directory:

```bash
# Remove the code location directory
rm -rf app/dagster_code/YOUR_CODE_LOCATION_NAME

# Verify removal
ls -la app/dagster_code/
```

### Step 2.5: Clear Python Cache (CRITICAL)

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

### Step 3: Remove Connector Configuration

Remove the connector YAML file associated with the code location:

```bash
# Remove the connector configuration
rm app/data-platform-service/data-manager/external-connectors/YOUR_CONNECTOR_NAME.yaml

# Verify removal
ls -la app/data-platform-service/data-manager/external-connectors/
```

### Step 4: Remove DBT Models

Remove all DBT models associated with the code location:

```bash
# Remove source definition
rm app/dbt_models/models/YOUR_CONNECTOR_sources.yml

# Remove staging model
rm app/dbt_models/models/staging/staging_YOUR_TABLE.sql

# Remove reporting model
rm app/dbt_models/models/YOUR_TABLE_reporting.sql

# Verify removal
ls -la app/dbt_models/models/
ls -la app/dbt_models/models/staging/
```

### Step 5: Update Code Location Configuration

**Update `app/data-platform-service/data-manager/resources/dagster/code-locations.json`**:

Remove the entry for your code location and update the description:

```json
{
  "code_locations": [
    {
      "name": "remaining_code_location",
      "enabled": true,
      "description": "Remaining code location description",
      "module": "dagster_code.remaining_code_location",
      "port": 4268,
      "host": "data-platform-service",
      "metadata": {
        "team": "data",
        "domain": "remaining_code_location",
        "version": "1.0.0"
      }
    }
    // ⚠️ REMOVED: your_code_location entry
  ],
  "config": {
    "workspace_name": "appbase-dagster-workspace",
    "description": "AppBase Data Platform Dagster Workspace (remaining_code_location only)",
    "version": "1.0.0"
  }
}
```

### Step 6: Update Platform Workspace Configuration

**Update `platform/workspace.yaml`**:

Remove the gRPC server entry for your code location:

```yaml
# Dagster Workspace Configuration - Multiple gRPC Servers
# Description: AppBase Data Platform Dagster Workspace (remaining_code_location only)

load_from:
  - grpc_server:
      host: data-platform-service
      port: 4273
      location_name: remaining_code_location
      # Remaining code location description
  # ⚠️ REMOVED: your_code_location gRPC server entry
```

### Step 7: Update Docker Port Exposures

**Update `app/docker-compose.yaml`** - Remove the port for your code location:

```yaml
  data-platform-service:
    # ... existing configuration ...
    ports:
      - "4273:4273"  # bridgestone_data_sync gRPC server port
      # ⚠️ REMOVED: your_code_location port
```

**Note**: If you're removing the last code location that uses a specific port, you can remove the port exposure. If other code locations still use ports in the same range, keep the port exposures.

### Step 8: Update Dockerfile (Optional)

**Update `app/Dockerfile.data-platform-service`** - Remove the port from EXPOSE directive:

```dockerfile
# Expose gRPC ports for all code locations
EXPOSE 4273  # Only expose ports that are actually used
```

**Note**: Only remove ports if no other code locations use them.

### Step 9: Clean Up Database Data (Optional)

If you want to remove all data associated with the code location:

```bash
# Connect to database and remove cache tables
docker exec -e PGPASSWORD=dataplatpassword data-platform-service psql -h db -U dataplatuser -d dataplatform -c "DROP SCHEMA IF EXISTS pyairbyte_cache CASCADE;"

# Remove DBT-generated tables (if they exist)
docker exec -e PGPASSWORD=dataplatpassword data-platform-service psql -h db -U dataplatuser -d dataplatform -c "DROP TABLE IF EXISTS staging.staging_YOUR_TABLE CASCADE;"
docker exec -e PGPASSWORD=dataplatpassword data-platform-service psql -h db -U dataplatuser -d dataplatform -c "DROP TABLE IF EXISTS reporting.YOUR_TABLE_reporting CASCADE;"
```

**⚠️ Warning**: This will permanently delete all data associated with the code location. Only do this if you're sure you want to remove all data.

### Step 10: Rebuild and Verify Removal

```bash
# Rebuild the data-platform-service
docker compose -f app/docker-compose.yaml up -d --build data-platform-service

# Start platform services
cd platform && ./start.sh

# Verify the code location is no longer loaded
docker exec data-platform-service python3 -c "import sys; sys.path.append('/app'); from dagster_code import YOUR_CODE_LOCATION; print('❌ Code location still exists')" 2>/dev/null || echo "✅ Code location successfully removed"
```

---

## Verification Commands

### 1. Code Location Loading Test

Test if the removed code location can still be imported:

```bash
# This should fail (return non-zero exit code)
docker exec data-platform-service python3 -c "import sys; sys.path.append('/app'); from dagster_code import YOUR_CODE_LOCATION; print('❌ Code location still exists')" 2>/dev/null || echo "✅ Code location successfully removed"
```

### 2. Remaining Code Locations Test

Verify that remaining code locations still work:

```bash
# Test remaining code locations
docker exec data-platform-service python3 -c "from dagster_code.bridgestone_data_sync import defs; print(f'✅ Remaining code location loaded - Assets: {len(defs.assets)}, Jobs: {len(defs.jobs)}')"
```

### 3. gRPC Server Verification

Check which gRPC servers are running:

```bash
# Check running gRPC servers
docker exec data-platform-service ps aux | grep dagster

# Check data-platform-service logs
docker logs data-platform-service --tail 20
```

### 4. Dagster UI Verification

Check if the code location appears in Dagster UI:

```bash
# Check Dagster webserver health
curl -f http://localhost:3030/health 2>/dev/null && echo "✅ Dagster webserver is accessible" || echo "⚠️ Dagster webserver not accessible"
```

### 5. Database Cleanup Verification

Verify database cleanup:

```bash
# Check if cache schema exists
docker exec -e PGPASSWORD=dataplatpassword data-platform-service psql -h db -U dataplatuser -d dataplatform -c "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'pyairbyte_cache';" 2>/dev/null || echo "Cache schema removed"

# Check if DBT tables exist
docker exec -e PGPASSWORD=dataplatpassword data-platform-service psql -h db -U dataplatuser -d dataplatform -c "SELECT table_name FROM information_schema.tables WHERE table_schema = 'staging' AND table_name = 'staging_YOUR_TABLE';" 2>/dev/null || echo "DBT staging table removed"
```

### 6. Configuration File Verification

Verify configuration files are updated:

```bash
# Check code-locations.json
cat app/data-platform-service/data-manager/resources/dagster/code-locations.json | jq '.code_locations[].name'

# Check workspace.yaml
grep -A 5 "load_from:" platform/workspace.yaml

# Check docker-compose.yaml ports
grep -A 10 "ports:" app/docker-compose.yaml
```

### 7. File System Verification

Verify files are removed:

```bash
# Check code location directory
ls -la app/dagster_code/

# Check connector configurations
ls -la app/data-platform-service/data-manager/external-connectors/

# Check DBT models
ls -la app/dbt_models/models/
ls -la app/dbt_models/models/staging/
```

---

## Troubleshooting

### Issue 1: Code Location Still Appears in Dagster UI

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

### Issue 2: Import Errors After Removal

**Symptoms**: Python import errors when trying to import removed code location

**Solutions**:
```bash
# Clear Python cache (both locally and in container)
find app/dagster_code -name "*.pyc" -delete
find app/dagster_code -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
docker exec data-platform-service find /app -name "*.pyc" -delete
docker exec data-platform-service find /app -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true

# Rebuild data-platform-service
docker compose -f app/docker-compose.yaml up -d --build data-platform-service
```

### Issue 3: Port Conflicts

**Symptoms**: Port already in use errors when adding new code locations

**Solutions**:
```bash
# Check which processes are using the port
docker exec data-platform-service netstat -tlnp | grep :4273

# Kill any remaining processes
docker exec data-platform-service pkill -f "dagster.*4273" || true
```

### Issue 4: Stale Python Cache After Removal

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

### Issue 5: gRPC Connection Errors After Removal

**Symptoms**: 
- `DagsterUserCodeUnreachableError: Could not reach user code server`
- `gRPC Error code: UNAVAILABLE`
- `Failed to connect to remote host: connect: Connection refused`

**Root Cause**: The Dagster webserver is trying to connect to gRPC servers before they've fully initialized.

**Solutions**:
```bash
# Add delay before checking logs or testing connections
sleep 30 && docker logs data-platform-service --tail 20

# Wait before testing gRPC server connectivity
sleep 30 && docker exec data-platform-service ps aux | grep dagster

# Restart platform services with delay
cd platform && ./stop.sh && sleep 10 && ./start.sh && sleep 45
```

### Issue 6: Database Connection Errors After Removal

**Symptoms**: 
- `FATAL: database "pyairbyte_cache" does not exist`
- Data-platform-service fails to start due to database connection issues
- PyAirbyte cache initialization errors

**Root Cause**: The system is trying to connect to a database called `pyairbyte_cache` instead of using the `pyairbyte_cache` schema within the main `dataplatform` database.

**Solutions**:
```bash
# Fix environment variable in app/.env
# Change PYAIRBYTE_CACHE_DB_NAME from 'pyairbyte_cache' to 'dataplatform'
sed -i 's/PYAIRBYTE_CACHE_DB_NAME=pyairbyte_cache/PYAIRBYTE_CACHE_DB_NAME=dataplatform/' app/.env

# Update cache database manager to create schema instead of database
# Edit app/data-platform-service/data-manager/pyairbyte/utils/cache_db_manager.py
# Change create_cache_database() method to create schema only

# Update initialization script
# Edit app/data-platform-service/data-manager/scripts/init_cache_db.py
# Change wait_for_database() to connect to main database instead of 'postgres'

# Rebuild data-platform-service
docker compose -f app/docker-compose.yaml up -d --build data-platform-service

# Verify schema creation
docker exec -e PGPASSWORD=dataplatpassword data-platform-service psql -h db -U dataplatuser -d dataplatform -c "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'pyairbyte_cache';"
```

---

## Example: Removing a Code Location

Here's a complete example of removing a hypothetical `example_sync` code location:

### Files and Directories Removed:

1. **Code Location Directory**: `app/data-platform-service/dagster_code/example_sync/`
2. **Connector Configuration**: `app/data-platform-service/data-manager/external-connectors/example-connector.yaml` (if applicable)
3. **DBT Models**:
   - `app/data-platform-service/dbt_models/models/example_sources.yml`
   - `app/data-platform-service/dbt_models/models/staging/staging_example.sql`
   - `app/data-platform-service/dbt_models/models/marts/example_mart.sql`

### Configuration Files Updated:

1. **Code Locations Config**: `app/data-platform-service/data-manager/resources/dagster/code-locations.json`
   - Removed `example_sync` entry
   - Updated description to reflect remaining code locations

2. **Workspace Config**: `platform/workspace.yaml`
   - Removed `example_sync` gRPC server entry
   - Updated description

3. **Docker Configuration**: `app/docker-compose.yaml`
   - Removed port exposure (if applicable)

4. **Dockerfile**: `app/Dockerfile.data-platform-service`
   - Removed port from EXPOSE directive (if applicable)

### Database Cleanup:

1. **Cache Tables**: Dropped tables with connector prefix from `pyairbyte_cache` schema
2. **DBT Tables**: Dropped staging and marts tables

### Commands Executed:

```bash
# Step 1: Stop services
cd platform && ./stop.sh
cd app && ./stop.sh

# Step 2: Remove code location directory
rm -rf app/data-platform-service/dagster_code/example_sync

# Step 2.5: Clear Python cache (CRITICAL)
find app/data-platform-service/dagster_code -name "*.pyc" -delete
find app/data-platform-service/dagster_code -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true

# Step 3: Remove connector configuration (if applicable)
rm app/data-platform-service/data-manager/external-connectors/example-connector.yaml

# Step 4: Remove DBT models (if applicable)
rm app/data-platform-service/dbt_models/models/example_sources.yml
rm app/data-platform-service/dbt_models/models/staging/staging_example.sql
rm app/data-platform-service/dbt_models/models/marts/example_mart.sql

# Step 5-8: Update configuration files (see above)

# Step 9: Clean up database (optional)
docker exec -e PGPASSWORD=dataplatpassword data-platform-service psql -h db -U dataplatuser -d dataplatform -c "DROP TABLE IF EXISTS pyairbyte_cache.example_connector_table CASCADE;"
docker exec -e PGPASSWORD=dataplatpassword data-platform-service psql -h db -U dataplatuser -d dataplatform -c "DROP TABLE IF EXISTS staging.staging_example CASCADE;"
docker exec -e PGPASSWORD=dataplatpassword data-platform-service psql -h db -U dataplatuser -d dataplatform -c "DROP TABLE IF EXISTS marts.example_mart CASCADE;"

# Step 10: Rebuild and verify
docker compose -f app/docker-compose.yaml up -d --build data-platform-service
cd platform && ./start.sh

# Verification
docker exec data-platform-service python3 -c "import sys; sys.path.append('/app'); from dagster_code import example_sync; print('❌ Code location still exists')" 2>/dev/null || echo "✅ Code location successfully removed"
```

---

## Best Practices

### 1. Document Before Removal

Keep a record of what was removed for future reference:
- List all files and directories removed
- Document configuration changes
- Note any database cleanup performed

### 2. Test After Removal

Verify that remaining code locations still work correctly:
- Test imports of remaining code locations
- Verify assets and jobs are still available
- Check Dagster UI functionality

### 3. Clean Up Thoroughly

Remove all associated files, configurations, and data:
- Code location directories
- Connector configurations
- DBT models and source definitions
- Database tables and schemas
- Configuration file entries

### 4. Update Documentation

Update any documentation that referenced the removed code location:
- Architecture diagrams
- Setup guides
- Configuration documentation

### 5. Notify Team

Inform team members about the removal to avoid confusion:
- Communicate the removal
- Update any shared documentation
- Ensure no dependencies exist

### 6. Monitor After Removal

Watch for any issues after removal:
- Check service logs for errors
- Monitor Dagster UI functionality
- Verify remaining code locations work

### 7. Port Management

When removing code locations, consider port management:
- Only remove ports if no other code locations use them
- Keep port ranges consistent
- Document port assignments

### 8. Python Cache Management

Always clear Python cache when removing code locations:
- Clear `.pyc` files
- Remove `__pycache__` directories
- Clear cache both locally and in containers

---

## Verification Checklist

After removal, verify:

- [ ] **Code Location Directory**: `app/dagster_code/YOUR_CODE_LOCATION` is deleted
- [ ] **Python Cache Cleared**: No `.pyc` files or `__pycache__` directories remain
- [ ] **Connector Configuration**: `app/data-platform-service/data-manager/external-connectors/YOUR_CONNECTOR.yaml` is deleted
- [ ] **DBT Models**: All related DBT models are deleted
- [ ] **Code Location Config**: Entry removed from `code-locations.json`
- [ ] **Workspace Config**: gRPC server entry removed from `workspace.yaml`
- [ ] **Docker Ports**: Port removed from `docker-compose.yaml` (if applicable)
- [ ] **Dockerfile**: Port removed from EXPOSE directive (if applicable)
- [ ] **Database Data**: Cache tables and DBT tables removed (if applicable)
- [ ] **Dagster UI**: Code location no longer appears in the UI
- [ ] **Service Logs**: No errors related to the removed code location
- [ ] **Import Tests**: No import errors when testing remaining code locations
- [ ] **Remaining Functionality**: Other code locations still work correctly

---

## References

- [PyAirbyte + Dagster Integration Guide](./pyairbyte-dagster-integration-guide.md)
- [Platform Architecture](./architecture.md)
- [Brownfield Architecture](./brownfield-architecture.md)

---

**For additional support, consult the platform documentation and Dagster community resources.** 