# Removing Services from the App Layer

## Overview

This guide provides step-by-step instructions for safely removing an existing service from the `app/` layer of the 99x Data Platform. We'll use a NestJS service as a practical example, but the process applies to any app-level service.

**Key Principle**: Remove services systematically to avoid breaking dependencies and ensure a clean removal without impacting other services.

## Table of Contents

1. [Pre-Removal Checklist](#pre-removal-checklist)
2. [Understanding Dependencies](#understanding-dependencies)
3. [Step-by-Step Removal Process](#step-by-step-removal-process)
4. [Service Configuration Cleanup](#service-configuration-cleanup)
5. [Docker Resources Cleanup](#docker-resources-cleanup)
6. [Database Cleanup](#database-cleanup)
7. [File System Cleanup](#file-system-cleanup)
8. [Verification and Testing](#verification-and-testing)
9. [Troubleshooting](#troubleshooting)

---

## Pre-Removal Checklist

Before removing a service, complete this checklist:

### 1. Identify Service Dependencies

**Check for:**
- [ ] Other services that depend on this service
- [ ] Platform services that require this service
- [ ] Database tables or schemas created by this service
- [ ] Shared volumes or networks used by this service
- [ ] Environment variables used by other services
- [ ] References in documentation or configuration files

**Commands to check dependencies:**
```bash
# Search for service references in codebase
grep -r "your-service-name" . --exclude-dir=node_modules --exclude-dir=.git

# Check docker-compose dependencies
grep -A 10 "depends_on:" app/docker-compose.yaml | grep -i "your-service-name"

# Check for database references
grep -r "your_service" app/appbase-init/appbase-schemas/
```

### 2. Backup Important Data

**Backup:**
- [ ] Service-specific database tables or schemas
- [ ] Service configuration files
- [ ] Service logs (if needed for debugging)
- [ ] Service environment variables

**Database Backup Example:**
```bash
# Backup service-specific tables
pg_dump -h ${APPBASE_DB_HOST} -U ${APPBASE_DB_USER} -d ${APPBASE_DB_NAME} \
  -t service_users -t service_config \
  > backup_service_tables_$(date +%Y%m%d).sql
```

### 3. Document Service State

**Document:**
- [ ] Current service configuration in `service.yaml`
- [ ] Service environment variables
- [ ] Service ports and network configuration
- [ ] Service dependencies
- [ ] Database schema created by the service

**Why this matters:**
- Helps with rollback if needed
- Provides reference for future similar services
- Documents what was removed for team knowledge

### 4. Verify Service is Not Critical

**Verify:**
- [ ] Service is not required by production systems
- [ ] No active users or processes depend on this service
- [ ] Service can be safely stopped without impact
- [ ] Removal is approved by team/stakeholders

---

## Understanding Dependencies

### Service Dependency Types

**1. Docker Compose Dependencies**
- Services that have `depends_on: your-service` in docker-compose.yaml
- These services will fail to start if your service is removed

**2. Database Dependencies**
- Foreign key relationships to your service's tables
- Views or functions that reference your service's schema
- Other services querying your service's data

**3. Network Dependencies**
- Services communicating with your service via network
- Services expecting your service to be available

**4. Configuration Dependencies**
- Other services reading your service's configuration
- Shared environment variables
- Service discovery mechanisms

### Finding Dependencies

**Search for service references:**
```bash
# Search in all files (excluding node_modules and .git)
find . -type f -not -path "*/node_modules/*" -not -path "*/.git/*" \
  -exec grep -l "your-service-name" {} \;

# Search in configuration files
grep -r "your-service-name" app/ platform/ service.yaml

# Search in documentation
grep -r "your-service-name" docs/
```

**Check Docker Compose dependencies:**
```bash
# Check if any service depends on your service
cd app
grep -B 5 -A 5 "depends_on:" docker-compose.yaml | grep -A 5 "your-service-name"
```

**Check database dependencies:**
```bash
# Connect to database and check for foreign keys
psql -h ${APPBASE_DB_HOST} -U ${APPBASE_DB_USER} -d ${APPBASE_DB_NAME} <<EOF
SELECT
    tc.table_name,
    kcu.column_name,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage AS ccu
    ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
    AND ccu.table_name = 'your_service_table';
EOF
```

---

## Step-by-Step Removal Process

### Step 1: Stop the Service

**1.1. Disable Service in service.yaml**

Edit [service.yaml](../service.yaml) and set the service to `false`:

```yaml
app:
  # appbase-init is mandatory and always runs (not toggleable)
  data-platform-service: true
  your-service-name: false  # Disable the service
```

**1.2. Stop Running Containers**

```bash
cd app

# Stop the service container
docker compose stop your-service-name

# Remove the container
docker compose rm -f your-service-name

# Verify it's stopped
docker ps -a | grep your-service-name
```

**1.3. Verify Service is Stopped**

```bash
# Check container status
docker ps -a | grep your-service-name

# Check if port is still in use
lsof -i :YOUR_SERVICE_PORT

# Verify service is not responding
curl http://localhost:YOUR_SERVICE_PORT/health
# Should fail or timeout
```

### Step 2: Remove Service Configuration

**2.1. Remove from service.yaml**

Edit [service.yaml](../service.yaml) and remove the service entry:

```yaml
app:
  # appbase-init is mandatory and always runs (not toggleable)
  data-platform-service: true
  # your-service-name: false  # REMOVED
```

**2.2. Remove from docker-compose.yaml**

Edit [app/docker-compose.yaml](../app/docker-compose.yaml) and remove the entire service definition:

```yaml
services:
  # ... other services ...
  
  # REMOVE THIS ENTIRE SERVICE BLOCK
  # your-service-name:
  #   profiles: ["your-service-name"]
  #   platform: linux/amd64
  #   ...
```

**Important**: Remove the entire service block, including:
- Service definition
- Environment variables
- Volumes (if service-specific)
- Port mappings
- Health checks
- Dependencies

**2.3. Remove from start.sh**

Edit [app/start.sh](../app/start.sh) and remove all references to the service:

**Remove service state reading:**
```bash
# REMOVE THIS LINE
# APP_YOUR_SERVICE=$(get_service_state "app.your-service-name")
```

**Remove service build logic:**
```bash
# REMOVE THIS ENTIRE BLOCK
# if [ "$APP_YOUR_SERVICE" = "true" ]; then
#     print_status "Building your-service-name (enabled in service.yaml)..."
#     DOCKER_BUILDKIT=1 docker compose --profile your-service-name build $CACHE_OPTION your-service-name
# else
#     print_status "your-service-name disabled in service.yaml, skipping build"
# fi
```

**Remove profile from compose command:**
```bash
# REMOVE THIS BLOCK
# if [ "$APP_YOUR_SERVICE" = "true" ]; then
#     print_status "Including your-service-name profile"
#     compose_command+=" --profile your-service-name"
# fi
```

**Remove service verification:**
```bash
# REMOVE THIS ENTIRE BLOCK
# if [ "$APP_YOUR_SERVICE" = "true" ]; then
#     if docker ps --format "{{.Names}}" | grep -q "^your-service-name$"; then
#         print_success "  ✓ your-service-name is running"
#     else
#         print_error "  ✗ ERROR: your-service-name should be running but is not"
#     fi
# else
#     if docker ps --format "{{.Names}}" | grep -q "^your-service-name$"; then
#         print_warning "  ⚠ WARNING: your-service-name is running but should be disabled in service.yaml"
#     else
#         print_success "  ✓ your-service-name is correctly disabled"
#     fi
# fi
```

**2.4. Remove from stop.sh (if applicable)**

Edit [app/stop.sh](../app/stop.sh) and remove service-specific stop logic if it exists.

### Step 3: Remove Environment Variables

**3.1. Remove from _env_sample**

Edit [app/_env_sample](../app/_env_sample) and remove service-specific environment variables:

```bash
# REMOVE THIS SECTION
# ################################################################################
# # YOUR SERVICE CONFIGURATION
# ################################################################################
# YOUR_SERVICE_PORT=3000
# YOUR_SERVICE_LOG_LEVEL=info
```

**3.2. Remove from .env**

Edit [app/.env](../app/.env) and remove service-specific environment variables (if not shared with other services).

**Important**: Only remove variables that are:
- Specific to this service only
- Not used by other services
- Not required by platform services

**3.3. Remove from _env_live (if applicable)**

If you have a production environment file, remove service variables from there as well.

### Step 4: Remove Docker Resources

**4.1. Remove Docker Images**

```bash
# List images related to your service
docker images | grep your-service-name

# Remove the image
docker rmi appbase/your-service-name:latest

# Remove all tags/versions
docker images | grep your-service-name | awk '{print $3}' | xargs docker rmi -f
```

**4.2. Remove Docker Volumes (if service-specific)**

```bash
# List volumes
docker volume ls | grep your-service-name

# Remove service-specific volumes (BE CAREFUL - verify first!)
docker volume rm your-service-name-data

# Verify removal
docker volume ls | grep your-service-name
```

**Important**: Only remove volumes that are:
- Service-specific (not shared)
- Not containing important data
- Not used by other services

**4.3. Remove Docker Networks (if service-specific)**

```bash
# List networks
docker network ls | grep your-service-name

# Remove service-specific network (if not shared)
docker network rm your-service-name-network
```

**Note**: Most services use `app-base-network` which is shared and should NOT be removed.

### Step 5: Remove Service Directory

**5.1. Backup Service Code (Optional)**

If you want to keep the code for reference:

```bash
# Create backup
tar -czf backup-your-service-name-$(date +%Y%m%d).tar.gz app/your-service-name/

# Move to archive location
mv backup-your-service-name-*.tar.gz ~/backups/
```

**5.2. Remove Service Directory**

```bash
# Navigate to app directory
cd app

# Remove service directory
rm -rf your-service-name/

# Verify removal
ls -la | grep your-service-name
```

**5.3. Remove Service Dockerfile (if separate)**

If the service has a separate Dockerfile in the app root:

```bash
cd app
rm -f Dockerfile.your-service-name
```

---

## Service Configuration Cleanup

### Complete Removal Checklist

Use this checklist to ensure complete removal:

- [ ] Service disabled in `service.yaml`
- [ ] Service entry removed from `service.yaml`
- [ ] Service definition removed from `app/docker-compose.yaml`
- [ ] Service references removed from `app/start.sh`
- [ ] Service references removed from `app/stop.sh` (if applicable)
- [ ] Service environment variables removed from `app/_env_sample`
- [ ] Service environment variables removed from `app/.env`
- [ ] Service environment variables removed from `app/_env_live` (if applicable)
- [ ] Service directory removed from `app/`
- [ ] Service Dockerfile removed (if separate)
- [ ] Docker images removed
- [ ] Docker volumes removed (if service-specific)
- [ ] Docker networks removed (if service-specific)

---

## Docker Resources Cleanup

### Comprehensive Docker Cleanup

**1. Stop and Remove Containers**

```bash
cd app

# Stop the service
docker compose stop your-service-name

# Remove the container
docker compose rm -f your-service-name

# Verify removal
docker ps -a | grep your-service-name
```

**2. Remove Images**

```bash
# Find all images
docker images | grep your-service-name

# Remove specific image
docker rmi appbase/your-service-name:latest

# Force remove if needed
docker rmi -f appbase/your-service-name:latest

# Remove all versions
docker images | grep your-service-name | awk '{print $3}' | xargs docker rmi -f
```

**3. Remove Volumes**

```bash
# List volumes
docker volume ls

# Inspect volume (verify it's service-specific)
docker volume inspect your-service-name-data

# Remove volume (WARNING: This deletes data!)
docker volume rm your-service-name-data
```

**4. Clean Up Build Cache (Optional)**

```bash
# Remove build cache for the service
docker builder prune -f

# Or remove all unused build cache
docker system prune -a --volumes
```

**Warning**: `docker system prune -a` removes ALL unused resources. Use with caution.

---

## Database Cleanup

### Service Database Cleanup

**Important Considerations:**
- Only remove database objects if you're certain they're not needed
- Consider keeping data for historical purposes
- Backup before deletion
- Check for foreign key dependencies

### Step 1: Identify Service Database Objects

**List service tables:**
```bash
psql -h ${APPBASE_DB_HOST} -U ${APPBASE_DB_USER} -d ${APPBASE_DB_NAME} <<EOF
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public' 
  AND table_name LIKE 'your_service_%';
EOF
```

**List service schemas:**
```bash
psql -h ${APPBASE_DB_HOST} -U ${APPBASE_DB_USER} -d ${APPBASE_DB_NAME} <<EOF
SELECT schema_name 
FROM information_schema.schemata 
WHERE schema_name LIKE 'your_service%';
EOF
```

**Check for foreign key dependencies:**
```bash
psql -h ${APPBASE_DB_HOST} -U ${APPBASE_DB_USER} -d ${APPBASE_DB_NAME} <<EOF
SELECT
    tc.table_name,
    kcu.column_name,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage AS ccu
    ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
    AND (ccu.table_name LIKE 'your_service_%' OR tc.table_name LIKE 'your_service_%');
EOF
```

### Step 2: Backup Service Data

**Backup service tables:**
```bash
# Backup all service tables
pg_dump -h ${APPBASE_DB_HOST} -U ${APPBASE_DB_USER} -d ${APPBASE_DB_NAME} \
  -t your_service_* \
  > backup_your_service_$(date +%Y%m%d).sql
```

**Backup service schema:**
```bash
# Backup entire schema
pg_dump -h ${APPBASE_DB_HOST} -U ${APPBASE_DB_USER} -d ${APPBASE_DB_NAME} \
  -n your_service_schema \
  > backup_your_service_schema_$(date +%Y%m%d).sql
```

### Step 3: Remove Service Database Objects

**Option 1: Remove Tables (if no dependencies)**

```sql
-- Connect to database
psql -h ${APPBASE_DB_HOST} -U ${APPBASE_DB_USER} -d ${APPBASE_DB_NAME}

-- Drop tables (in reverse dependency order)
DROP TABLE IF EXISTS your_service_table2 CASCADE;
DROP TABLE IF EXISTS your_service_table1 CASCADE;

-- Verify removal
\dt your_service_*
```

**Option 2: Remove Schema (if service uses separate schema)**

```sql
-- Drop entire schema
DROP SCHEMA IF EXISTS your_service_schema CASCADE;

-- Verify removal
\dn your_service*
```

**Option 3: Remove Flyway Migration History (if applicable)**

```sql
-- Remove Flyway migration history for service
DELETE FROM flyway_schema_history 
WHERE installed_rank IN (
    SELECT installed_rank 
    FROM flyway_schema_history 
    WHERE script LIKE 'V%__your_service%'
);
```

**Important Notes:**
- Use `CASCADE` carefully - it removes dependent objects
- Always backup before deletion
- Consider keeping data for audit/historical purposes
- Check with team before deleting production data

### Step 4: Clean Up Service Migrations

If the service had migrations in `appbase-init`, you may want to remove them:

```bash
# Review migrations
ls -la app/appbase-init/appbase-schemas/ | grep your_service

# Backup migrations before removal
cp -r app/appbase-init/appbase-schemas/ app/appbase-init/appbase-schemas.backup/

# Remove service migrations (if they exist)
rm app/appbase-init/appbase-schemas/V*__your_service*.sql
```

**Note**: Only remove migrations if:
- They're service-specific (not shared)
- Service is permanently removed
- You have backups

---

## File System Cleanup

### Remove Service Files

**1. Remove Service Directory**

```bash
cd app
rm -rf your-service-name/
```

**2. Remove Service Dockerfile**

```bash
cd app
rm -f Dockerfile.your-service-name
```

**3. Remove Service-Specific Scripts (if any)**

```bash
# Check for service-specific scripts
find app/ -name "*your-service*" -type f

# Remove if found
rm app/scripts/your-service-*.sh
```

**4. Clean Up Documentation**

```bash
# Remove service-specific documentation
rm -f docs/your-service-*.md

# Update main documentation if it references the service
# Edit AGENTS.md or other docs to remove service references
```

### Verify File Removal

```bash
# Search for any remaining references
find . -type f -not -path "*/node_modules/*" -not -path "*/.git/*" \
  -exec grep -l "your-service-name" {} \;

# Should return minimal or no results (only in this removal guide)
```

---

## Verification and Testing

### Post-Removal Verification

**1. Verify Service is Removed from Configuration**

```bash
# Check service.yaml
grep -i "your-service-name" service.yaml
# Should return no results

# Check docker-compose.yaml
grep -i "your-service-name" app/docker-compose.yaml
# Should return no results

# Check start.sh
grep -i "your-service-name" app/start.sh
# Should return no results
```

**2. Verify Docker Resources are Removed**

```bash
# Check containers
docker ps -a | grep your-service-name
# Should return no results

# Check images
docker images | grep your-service-name
# Should return no results

# Check volumes
docker volume ls | grep your-service-name
# Should return no results (unless shared)
```

**3. Verify Service Directory is Removed**

```bash
# Check directory
ls -la app/ | grep your-service-name
# Should return no results

# Check for any remaining files
find app/ -name "*your-service*"
# Should return no results
```

**4. Test Other Services Still Work**

```bash
# Start remaining services
cd app
./start.sh

# Verify other services start correctly
docker compose ps

# Check service logs for errors
docker compose logs appbase-init
docker compose logs data-platform-service

# Test service health endpoints
curl http://localhost:8081/healthz  # Hasura
curl http://localhost:4266/health  # Data Platform Service
```

**5. Verify Database (if applicable)**

```bash
# Check that service tables are removed (if you removed them)
psql -h ${APPBASE_DB_HOST} -U ${APPBASE_DB_USER} -d ${APPBASE_DB_NAME} <<EOF
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public' 
  AND table_name LIKE 'your_service_%';
EOF
# Should return no results
```

### Integration Testing

**1. Test Service Startup**

```bash
cd app
./start.sh

# Verify no errors related to removed service
# Check startup logs
docker compose logs | grep -i "your-service-name"
# Should return no results or only error messages about missing service (which is expected)
```

**2. Test Service Dependencies**

```bash
# Verify services that depended on removed service still work
# (If any services had dependencies, they should be updated before removal)

# Test each remaining service
curl http://localhost:8081/healthz  # Hasura
curl http://localhost:4266/health  # Data Platform Service
```

**3. Test Database Connections**

```bash
# Verify database connections still work
psql -h ${APPBASE_DB_HOST} -U ${APPBASE_DB_USER} -d ${APPBASE_DB_NAME} -c "SELECT 1;"
```

---

## Troubleshooting

### Service Still Appears After Removal

**Symptoms:**
- Service container still running
- Service appears in `docker compose ps`
- Service responds to health checks

**Solutions:**
```bash
# Force stop and remove
docker compose stop your-service-name
docker compose rm -f your-service-name

# Check for orphaned containers
docker ps -a | grep your-service-name

# Remove manually if needed
docker stop your-service-name
docker rm your-service-name
```

### Configuration Errors After Removal

**Symptoms:**
- `start.sh` fails with service-related errors
- Docker Compose errors about missing service
- Service state reading errors

**Solutions:**
```bash
# Verify service.yaml is clean
grep -i "your-service-name" service.yaml
# Remove any remaining references

# Verify docker-compose.yaml is clean
grep -i "your-service-name" app/docker-compose.yaml
# Remove any remaining references

# Verify start.sh is clean
grep -i "your-service-name" app/start.sh
# Remove any remaining references

# Test startup
cd app
./start.sh
```

### Database Cleanup Issues

**Symptoms:**
- Foreign key constraint errors when dropping tables
- Other services fail after database cleanup
- Migration errors

**Solutions:**
```bash
# Check for foreign key dependencies
psql -h ${APPBASE_DB_HOST} -U ${APPBASE_DB_USER} -d ${APPBASE_DB_NAME} <<EOF
SELECT
    tc.table_name,
    kcu.column_name,
    ccu.table_name AS foreign_table_name
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage AS ccu
    ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
    AND ccu.table_name LIKE 'your_service_%';
EOF

# Drop with CASCADE (if safe)
DROP TABLE your_service_table CASCADE;

# Or remove foreign keys first
ALTER TABLE other_table DROP CONSTRAINT fk_your_service;
DROP TABLE your_service_table;
```

### Port Still in Use

**Symptoms:**
- Port conflict when starting other services
- "Port already in use" errors

**Solutions:**
```bash
# Find process using the port
lsof -i :YOUR_SERVICE_PORT

# Kill the process
kill -9 $(lsof -t -i:YOUR_SERVICE_PORT)

# Or find and stop the container
docker ps | grep YOUR_SERVICE_PORT
docker stop <container-id>
```

### Volume Cleanup Issues

**Symptoms:**
- Volume still exists after service removal
- Volume in use errors

**Solutions:**
```bash
# Check volume usage
docker volume inspect your-service-name-data

# Find containers using the volume
docker ps -a --filter volume=your-service-name-data

# Remove containers first
docker rm -f <container-id>

# Then remove volume
docker volume rm your-service-name-data
```

### Rollback Procedure

If you need to rollback the removal:

**1. Restore Configuration**

```bash
# Restore service.yaml (from git or backup)
git checkout HEAD -- service.yaml

# Or manually add back:
# your-service-name: true
```

**2. Restore Files**

```bash
# Restore from git
git checkout HEAD -- app/docker-compose.yaml
git checkout HEAD -- app/start.sh

# Or restore from backup
cp backup/docker-compose.yaml app/
cp backup/start.sh app/
```

**3. Restore Service Directory**

```bash
# Restore from backup
tar -xzf backup-your-service-name-*.tar.gz -C app/

# Or restore from git
git checkout HEAD -- app/your-service-name/
```

**4. Restore Database (if removed)**

```bash
# Restore from backup
psql -h ${APPBASE_DB_HOST} -U ${APPBASE_DB_USER} -d ${APPBASE_DB_NAME} \
  < backup_your_service_*.sql
```

**5. Restart Services**

```bash
cd app
./start.sh
```

---

## Summary Checklist

Use this comprehensive checklist when removing a service:

### Pre-Removal
- [ ] Identified all service dependencies
- [ ] Backed up important data
- [ ] Documented service state
- [ ] Verified service is not critical
- [ ] Obtained approval for removal

### Service Removal
- [ ] Disabled service in `service.yaml`
- [ ] Stopped service containers
- [ ] Removed service from `service.yaml`
- [ ] Removed service from `app/docker-compose.yaml`
- [ ] Removed service from `app/start.sh`
- [ ] Removed service from `app/stop.sh` (if applicable)
- [ ] Removed environment variables from `app/_env_sample`
- [ ] Removed environment variables from `app/.env`
- [ ] Removed environment variables from `app/_env_live` (if applicable)

### Docker Cleanup
- [ ] Removed Docker containers
- [ ] Removed Docker images
- [ ] Removed Docker volumes (if service-specific)
- [ ] Removed Docker networks (if service-specific)

### Database Cleanup (if applicable)
- [ ] Identified service database objects
- [ ] Backed up service data
- [ ] Removed service tables/schemas
- [ ] Removed Flyway migration history (if applicable)
- [ ] Removed service migrations from appbase-init (if applicable)

### File System Cleanup
- [ ] Removed service directory
- [ ] Removed service Dockerfile
- [ ] Removed service-specific scripts
- [ ] Updated documentation

### Verification
- [ ] Verified service removed from configuration
- [ ] Verified Docker resources removed
- [ ] Verified service directory removed
- [ ] Tested other services still work
- [ ] Verified database cleanup (if applicable)
- [ ] Performed integration testing

---

## Additional Resources

- [adding-new-app-services.md](adding-new-app-services.md) - Guide for adding services (reverse reference)
- [AGENTS.md](../AGENTS.md) - Complete platform architecture documentation
- [app/docker-compose.yaml](../app/docker-compose.yaml) - Docker Compose configuration reference
- [app/start.sh](../app/start.sh) - Startup script reference
- [service.yaml](../service.yaml) - Service configuration file

---

## Best Practices

1. **Always Backup First**: Create backups before removing anything
2. **Remove Gradually**: Disable first, then remove configuration, then clean up
3. **Verify Dependencies**: Always check for dependencies before removal
4. **Test After Removal**: Verify other services still work correctly
5. **Document Removal**: Keep notes on what was removed and why
6. **Keep Backups**: Don't delete backups immediately - keep them for a period
7. **Communicate Changes**: Inform team members about service removal

---

This guide ensures safe and complete removal of services without impacting other parts of the system. Follow the steps systematically and verify each stage before proceeding to the next.

