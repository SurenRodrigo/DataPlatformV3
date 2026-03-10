#!/bin/bash
docker network ls|grep app-base-network > /dev/null || docker network create --driver bridge app-base-network
set -eo pipefail

# Parse command line arguments
NO_CACHE=""

for arg in "$@"; do
  if [[ "$arg" == "-n" || "$arg" == "--no-cache" ]]; then
    NO_CACHE="--no-cache"
  fi
done

# Function to verify services are running according to service.yaml configuration
verify_services() {
    local platform_db="$1"
    local platform_hasura="$2"
    local platform_metabase="$3"
    local platform_superset="$4"
    local platform_dagster="$5"
    local platform_nginx="$6"
    local platform_cloudbeaver="$7"
    
    printf "\n🔍 Verifying services against service.yaml configuration...\n"
    
    local errors=0
    local warnings=0
    
    # Wait a few seconds for containers to fully start
    sleep 3
    
    # Check database (only if local)
    if [ "$platform_db" = "local" ]; then
        if docker ps --format "{{.Names}}" | grep -q "^appbase-db$"; then
            printf "  ✓ Database (appbase-db) is running\n"
        else
            printf "  ✗ ERROR: Database (appbase-db) should be running but is not\n"
            errors=$((errors + 1))
        fi
    else
        printf "  ⊘ Database: External mode (not checking container)\n"
    fi
    
    # Check Hasura
    if [ "$platform_hasura" = "true" ]; then
        if docker ps --format "{{.Names}}" | grep -q "^hasura$"; then
            printf "  ✓ Hasura is running\n"
        else
            printf "  ✗ ERROR: Hasura should be running but is not\n"
            errors=$((errors + 1))
        fi
    else
        if docker ps --format "{{.Names}}" | grep -q "^hasura$"; then
            printf "  ⚠ WARNING: Hasura is running but should be disabled in service.yaml\n"
            warnings=$((warnings + 1))
        else
            printf "  ✓ Hasura is correctly disabled\n"
        fi
    fi
    
    # Check Metabase
    if [ "$platform_metabase" = "true" ]; then
        if docker ps --format "{{.Names}}" | grep -q "^metabase$"; then
            printf "  ✓ Metabase is running\n"
        else
            printf "  ✗ ERROR: Metabase should be running but is not\n"
            errors=$((errors + 1))
        fi
    else
        if docker ps --format "{{.Names}}" | grep -q "^metabase$"; then
            printf "  ⚠ WARNING: Metabase is running but should be disabled in service.yaml\n"
            warnings=$((warnings + 1))
        else
            printf "  ✓ Metabase is correctly disabled\n"
        fi
    fi
    
    # Check Superset
    if [ "$platform_superset" = "true" ]; then
        if docker ps --format "{{.Names}}" | grep -q "^superset$"; then
            printf "  ✓ Superset is running\n"
        else
            printf "  ✗ ERROR: Superset should be running but is not\n"
            errors=$((errors + 1))
        fi
    else
        if docker ps --format "{{.Names}}" | grep -q "^superset$"; then
            printf "  ⚠ WARNING: Superset is running but should be disabled in service.yaml\n"
            warnings=$((warnings + 1))
        else
            printf "  ✓ Superset is correctly disabled\n"
        fi
    fi
    
    # Check Dagster (both webserver and daemon)
    if [ "$platform_dagster" = "true" ]; then
        local dagster_webserver_running=false
        local dagster_daemon_running=false
        
        if docker ps --format "{{.Names}}" | grep -q "^dagster-webserver$"; then
            dagster_webserver_running=true
            printf "  ✓ Dagster webserver is running\n"
        else
            printf "  ✗ ERROR: Dagster webserver should be running but is not\n"
            errors=$((errors + 1))
        fi
        
        if docker ps --format "{{.Names}}" | grep -q "^dagster-daemon$"; then
            dagster_daemon_running=true
            printf "  ✓ Dagster daemon is running\n"
        else
            printf "  ✗ ERROR: Dagster daemon should be running but is not\n"
            errors=$((errors + 1))
        fi
    else
        if docker ps --format "{{.Names}}" | grep -qE "^dagster-(webserver|daemon)$"; then
            printf "  ⚠ WARNING: Dagster services are running but should be disabled in service.yaml\n"
            warnings=$((warnings + 1))
        else
            printf "  ✓ Dagster is correctly disabled\n"
        fi
    fi
    
    # Check Nginx
    if [ "$platform_nginx" = "true" ]; then
        if docker ps --format "{{.Names}}" | grep -q "^nginx$"; then
            printf "  ✓ Nginx is running\n"
        else
            printf "  ✗ ERROR: Nginx should be running but is not\n"
            errors=$((errors + 1))
        fi
    else
        if docker ps --format "{{.Names}}" | grep -q "^nginx$"; then
            printf "  ⚠ WARNING: Nginx is running but should be disabled in service.yaml\n"
            warnings=$((warnings + 1))
        else
            printf "  ✓ Nginx is correctly disabled\n"
        fi
    fi
    
    # Check CloudBeaver
    if [ "$platform_cloudbeaver" = "true" ]; then
        if docker ps --format "{{.Names}}" | grep -q "^cloudbeaver$"; then
            printf "  ✓ CloudBeaver is running\n"
        else
            printf "  ✗ ERROR: CloudBeaver should be running but is not\n"
            errors=$((errors + 1))
        fi
    else
        if docker ps --format "{{.Names}}" | grep -q "^cloudbeaver$"; then
            printf "  ⚠ WARNING: CloudBeaver is running but should be disabled in service.yaml\n"
            warnings=$((warnings + 1))
        else
            printf "  ✓ CloudBeaver is correctly disabled\n"
        fi
    fi
    
    # Summary
    printf "\n"
    if [ $errors -eq 0 ] && [ $warnings -eq 0 ]; then
        printf "✅ All services match service.yaml configuration!\n"
    elif [ $errors -eq 0 ]; then
        printf "⚠️  Verification complete with %d warning(s). Services may need adjustment.\n" "$warnings"
    else
        printf "❌ Verification failed with %d error(s) and %d warning(s).\n" "$errors" "$warnings"
        printf "   Please check the service status and service.yaml configuration.\n"
    fi
    printf "\n"
}

# Function to parse service.yaml and get service state
# Returns: "true", "false", or "local"/"external" for db
get_service_state() {
    local service_path="$1"
    local service_yaml="${2:-../service.yaml}"
    
    # Try to use yq if available
    if command -v yq >/dev/null 2>&1; then
        local value=$(yq eval ".${service_path}" "$service_yaml" 2>/dev/null)
        if [ "$value" = "null" ] || [ -z "$value" ]; then
            echo "false"
        elif [ "$value" = "true" ] || [ "$value" = "false" ]; then
            echo "$value"
        else
            echo "$value"
        fi
        return
    fi
    
    # Fallback: simple bash-based YAML parser for nested structure
    if [ ! -f "$service_yaml" ]; then
        printf "Error: service.yaml not found at %s\n" "$service_yaml" >&2
        printf "Please ensure service.yaml exists in the project root.\n" >&2
        exit 1
    fi
    
    # Extract the key name (last part of path, e.g., "hasura" from "platform.hasura")
    local key_name="${service_path##*.}"
    
    # Find the platform section and then the specific key
    local in_platform=false
    local value=""
    
    while IFS= read -r line || [ -n "$line" ]; do
        # Check if we're entering the platform section
        if [[ "$line" =~ ^platform: ]]; then
            in_platform=true
            continue
        fi
        
        # If we hit another top-level key, we're out of platform section
        if [[ "$in_platform" = true ]] && [[ "$line" =~ ^[a-zA-Z_]+: ]] && [[ ! "$line" =~ ^[[:space:]] ]]; then
            break
        fi
        
        # Look for the key we want within platform section
        if [[ "$in_platform" = true ]] && [[ "$line" =~ ^[[:space:]]+${key_name}: ]]; then
            # Extract value (remove key, colon, strip comments, and trim)
            value=$(echo "$line" | sed "s/.*${key_name}:[[:space:]]*//" | sed 's/#.*$//' | tr -d '"' | tr -d "'" | xargs)
            break
        fi
    done < "$service_yaml"
    
    if [ -z "$value" ]; then
        echo "false"
    else
        echo "$value"
    fi
}

main() {
    # Check if service.yaml exists first (needed to determine service states)
    local service_yaml="../service.yaml"
    if [ ! -f "$service_yaml" ]; then
        printf "Error: service.yaml not found at %s\n" "$service_yaml" >&2
        printf "Please ensure service.yaml exists in the project root.\n" >&2
        exit 1
    fi
    
    # Read service states from service.yaml
    local PLATFORM_DB=$(get_service_state "platform.db" "$service_yaml")
    local PLATFORM_HASURA=$(get_service_state "platform.hasura" "$service_yaml")
    local PLATFORM_METABASE=$(get_service_state "platform.metabase" "$service_yaml")
    local PLATFORM_SUPERSET=$(get_service_state "platform.superset" "$service_yaml")
    local PLATFORM_DAGSTER=$(get_service_state "platform.dagster" "$service_yaml")
    local PLATFORM_NGINX=$(get_service_state "platform.nginx" "$service_yaml")
    local PLATFORM_CLOUDBEAVER=$(get_service_state "platform.cloudbeaver" "$service_yaml")
    
    # Check if .env file exists
    if [ ! -f .env ]; then
        printf "Error: .env file not found in platform directory\n" >&2
        exit 1
    fi
    set -a
    source .env
    set +a
    
    # Note: postgres-entrypoint.sh now reads directly from service.yaml
    # No need to translate service.yaml values to env vars anymore
    
    # Check if docker compose is running
    local ps_command="docker compose"
    if [ "$PLATFORM_DB" != "local" ] && [ -f "docker-compose.no-db.yaml" ]; then
        ps_command="docker compose -f docker-compose.yaml -f docker-compose.no-db.yaml"
    else
        # When using local DB, include the local-db profile for the check
        ps_command="docker compose --profile local-db"
    fi
    # Temporarily disable exit on error for this check
    set +e
    RUNNING=$($ps_command ps -q --status=running 2>/dev/null | wc -l)
    set -e
    if [ "$RUNNING" -gt 0 ]; then
        printf "AppBase Engine is still running. \n"
        printf "You can stop it with the ./stop.sh command. \n"
        exit 1
    fi
    export APPBASE_START_SOURCE="unknown"
    
    # Base docker-compose command
    compose_command="docker compose"
    
    # Hasura service configuration
    if [ "$PLATFORM_HASURA" = "true" ]; then
        printf "Hasura GraphQL engine enabled.\n"
        compose_command+=" --profile hasura"
        printf "Including Hasura profile: hasura\n"
    else
        printf "Hasura GraphQL engine disabled.\n"
    fi
    
    # BI tool selection logic (can enable both simultaneously)
    if [ "$PLATFORM_METABASE" = "true" ] && [ "$PLATFORM_SUPERSET" = "true" ]; then
        printf "BI tool integration enabled: Both Metabase and Superset\n"
        compose_command+=" --profile metabase"
        compose_command+=" --profile superset"
        printf "Including BI tool profiles: metabase, superset\n"
    elif [ "$PLATFORM_METABASE" = "true" ]; then
        printf "BI tool integration enabled: Metabase\n"
        compose_command+=" --profile metabase"
        printf "Including BI tool profile: metabase\n"
    elif [ "$PLATFORM_SUPERSET" = "true" ]; then
        printf "BI tool integration enabled: Superset\n"
        compose_command+=" --profile superset"
        printf "Including BI tool profile: superset\n"
    else
        printf "BI tool integration disabled.\n"
    fi
    
    # CloudBeaver web database manager
    if [ "$PLATFORM_CLOUDBEAVER" = "true" ]; then
        printf "CloudBeaver web database manager enabled.\n"
        compose_command+=" --profile cloudbeaver"
        printf "Including CloudBeaver profile: cloudbeaver\n"
    else
        printf "CloudBeaver web database manager disabled.\n"
    fi
    
    # Dagster data orchestration selection logic
    if [ "$PLATFORM_DAGSTER" = "true" ]; then
        printf "Dagster data orchestration enabled.\n"
        compose_command+=" --profile dagster"
        printf "Including Dagster profile: dagster\n"
        
        # Initialize shared Dagster storage if needed
        printf "🔧 Checking shared Dagster storage...\n"
        if ! docker volume inspect dagster_shared_storage >/dev/null 2>&1; then
            printf "📦 Creating shared Dagster storage volume...\n"
            docker volume create dagster_shared_storage
        else
            printf "📦 Shared Dagster storage volume already exists.\n"
        fi
        
        # Copy platform configuration to shared volume
        printf "📋 Copying Dagster configuration to shared volume...\n"
        docker run --rm \
          -v dagster_shared_storage:/opt/dagster/dagster_home \
          -v $(pwd)/dagster.yaml:/dagster.yaml:ro \
          -v $(pwd)/workspace.yaml:/workspace.yaml:ro \
          alpine:latest \
          sh -c "cp /dagster.yaml /opt/dagster/dagster_home/ && cp /workspace.yaml /opt/dagster/dagster_home/ && chmod 644 /opt/dagster/dagster_home/*.yaml" 2>/dev/null || true
        
        printf "✅ Shared Dagster storage ready!\n"
    else
        printf "Dagster data orchestration disabled.\n"
    fi
    
    # Database service configuration
    if [ "$PLATFORM_DB" = "local" ]; then
        printf "Local database service enabled.\n"
        compose_command+=" --profile local-db"
        printf "Including local database profile: local-db\n"
    else
        printf "External database mode enabled (db=%s).\n" "$PLATFORM_DB"
        # Validate external DB configuration
        if [ -z "${APPBASE_CONFIG_DB_HOST}" ]; then
            printf "Error: APPBASE_CONFIG_DB_HOST is not set. Required when using external database.\n" >&2
            exit 1
        fi
        if [ "${APPBASE_CONFIG_DB_HOST}" = "db" ]; then
            printf "Warning: APPBASE_CONFIG_DB_HOST is set to 'db' (Docker service name) but external database mode is enabled.\n" >&2
            printf "Warning: This may cause connection issues. Please set APPBASE_CONFIG_DB_HOST to your external database hostname.\n" >&2
        fi
        # Use override file to remove depends_on: db references
        if [ -f "docker-compose.no-db.yaml" ]; then
            local rest_of_cmd="${compose_command#docker compose}"
            if [[ -z "${rest_of_cmd// }" ]]; then
                rest_of_cmd=""
            fi
            compose_command="docker compose -f docker-compose.yaml -f docker-compose.no-db.yaml${rest_of_cmd}"
            printf "Using override file to remove db dependencies\n"
        else
            printf "Error: docker-compose.no-db.yaml not found. Required when using external database.\n" >&2
            exit 1
        fi
    fi
    
    # Nginx reverse proxy configuration
    if [ "$PLATFORM_NGINX" = "true" ]; then
        printf "Nginx reverse proxy enabled.\n"
        compose_command+=" --profile nginx"
        printf "Including Nginx profile: nginx\n"
    else
        printf "Nginx reverse proxy disabled.\n"
    fi
    
    # Build and up logic for Docker Compose v2+
    # Print final command for debugging
    printf "Final compose command: %s\n" "$compose_command"
    
    if [ -n "$NO_CACHE" ]; then
        printf "Building images with no cache...\n"
        $compose_command build --no-cache
        printf "Starting AppBase Platform with command: %s up --remove-orphans --detach\n" "$compose_command"
        $compose_command up --remove-orphans --detach
    else
        printf "Starting AppBase Platform with command: %s up --build --remove-orphans --detach\n" "$compose_command"
        $compose_command up --build --remove-orphans --detach
    fi
    printf "Platform services started successfully!\n"
    
    # Verify services are running according to service.yaml
    verify_services "$PLATFORM_DB" "$PLATFORM_HASURA" "$PLATFORM_METABASE" "$PLATFORM_SUPERSET" "$PLATFORM_DAGSTER" "$PLATFORM_NGINX" "$PLATFORM_CLOUDBEAVER"
    
    # Display database connection info
    if [ "$PLATFORM_DB" = "local" ]; then
        printf "Database: http://localhost:5432\n"
    else
        printf "Database: ${APPBASE_CONFIG_DB_HOST}:${APPBASE_CONFIG_DB_PORT}\n"
    fi
    
    # Display service URLs based on service.yaml configuration
    if [ "$PLATFORM_HASURA" = "true" ]; then
        printf "Hasura Console: http://localhost:8081\n"
    fi
    
    if [ "$PLATFORM_METABASE" = "true" ]; then
        printf "Metabase BI: http://localhost:3000\n"
    fi
    
    if [ "$PLATFORM_SUPERSET" = "true" ]; then
        printf "Superset BI: http://localhost:8088\n"
    fi
    
    if [ "$PLATFORM_DAGSTER" = "true" ]; then
        printf "Dagster Webserver: http://localhost:3030\n"
    fi
    
    if [ "$PLATFORM_CLOUDBEAVER" = "true" ]; then
        printf "CloudBeaver: http://localhost:${CLOUDBEAVER_PORT:-8978}\n"
    fi
}

main "$@"
exit
