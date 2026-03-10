#!/bin/bash

# ============================================================================
# Service Initialization Script
# ============================================================================
# This script initializes platform services based on service.yaml configuration.
# 
# IMPORTANT: service.yaml is the SINGLE SOURCE OF TRUTH for which services
# should be initialized. Environment variables are only used for connection
# details (URLs, credentials, etc.), NOT for service enablement decisions.
#
# Service enablement logic:
# - All service enablement decisions come from service.yaml
# - Env vars are validated only when services are enabled in service.yaml
# - Future services can be added by extending service.yaml and this script
# ============================================================================

# Function to get service state from service.yaml
# This is the ONLY way to determine if a service should be initialized
get_service_state() {
    local service_path="$1"
    node parse-service-yaml.js "$service_path" 2>/dev/null || echo "false"
}

# Validate that required environment variables are set when a service is enabled
# This ensures proper error messages if env vars are missing
validate_service_env() {
    local service_name="$1"
    local required_vars=("${@:2}")  # All remaining args are required vars
    
    local missing_vars=()
    for var in "${required_vars[@]}"; do
        if [ -z "${!var}" ]; then
            missing_vars+=("$var")
        fi
    done
    
    if [ ${#missing_vars[@]} -gt 0 ]; then
        echo "ERROR: Service '$service_name' is enabled in service.yaml but required environment variables are missing:"
        for var in "${missing_vars[@]}"; do
            echo "  - $var"
        done
        exit 1
    fi
}

# Read connection details from environment variables
# These are used for connecting to services, NOT for determining if services should run
hasura_admin_secret=$HASURA_GRAPHQL_ADMIN_SECRET
hasura_database_url=$HASURA_GRAPHQL_DATABASE_URL
hasura_url=$HASURA_URL
metabase_url=$METABASE_URL
superset_url=$SUPERSET_URL
db_host=$APPBASE_DB_HOST
db_port=$APPBASE_DB_PORT

# Prints a failure message and exits if the last command
# executed failed (i.e., its exit code was not 0).
function fail() {
    if [ $? -ne 0 ] ; then
        echo "$1"
        exit 1
    fi
}

./wait-for/wait-for.sh "$db_host":"$db_port"
fail "Timed out waiting for DB."

 ./db-init.sh
 fail "DB initialization failed."


# Hasura initialization (conditional based on service.yaml - SINGLE SOURCE OF TRUTH)
if [ "$(get_service_state platform.hasura)" = "true" ]; then
    echo "Hasura service enabled in service.yaml, initializing..."
    
    # Validate required env vars for Hasura
    validate_service_env "Hasura" "HASURA_URL" "HASURA_GRAPHQL_ADMIN_SECRET"
    
    hasura_optional_args=()
    if [ -n "${hasura_database_url}" ]; then
        hasura_optional_args=("${hasura_optional_args[@]}" --database-url "${hasura_database_url}")
    fi
    
    ./wait-for/wait-for.sh "$hasura_url"/healthz -t 120
    fail "Timed out waiting for Hasura."
    
    node ../lib/hasura/init --hasura-url "$hasura_url" --admin-secret "$hasura_admin_secret" "${hasura_optional_args[@]}"
    fail "Hasura initialization failed."
    
    echo "Hasura initialization completed successfully"
else
    echo "Hasura service disabled in service.yaml, skipping initialization"
fi

# BI tool initialization (read from service.yaml - SINGLE SOURCE OF TRUTH)
# Both Metabase and Superset can be enabled simultaneously
PLATFORM_METABASE=$(get_service_state platform.metabase)
PLATFORM_SUPERSET=$(get_service_state platform.superset)

if [ "$PLATFORM_METABASE" = "true" ] && [ "$PLATFORM_SUPERSET" = "true" ]; then
    echo "BI tool integration enabled: Both Metabase and Superset (from service.yaml)"
    
    # Validate required env vars for Metabase
    validate_service_env "Metabase" "METABASE_URL"
    
    # Initialize Metabase
    echo "Initializing Metabase..."
    ./wait-for/wait-for.sh "$metabase_url"/api/health -t 120
    fail "Timed out waiting for Metabase."
    
    ./metabase-init.sh
    fail "Metabase initialization failed."
    
    echo "Metabase initialization completed successfully"
    
    # Validate required env vars for Superset
    validate_service_env "Superset" "SUPERSET_URL"
    
    # Initialize Superset
    echo "Initializing Superset..."
    ./wait-for/wait-for.sh "$superset_url"/health -t 120
    fail "Timed out waiting for Superset."
    
    ./superset-init.sh
    fail "Superset initialization failed."
    
    echo "Superset initialization completed successfully"
    
    echo "Both BI tools initialized successfully"
elif [ "$PLATFORM_METABASE" = "true" ]; then
    echo "BI tool integration enabled: Metabase (from service.yaml)"
    
    # Validate required env vars for Metabase
    validate_service_env "Metabase" "METABASE_URL"
    
    echo "Initializing Metabase..."
    ./wait-for/wait-for.sh "$metabase_url"/api/health -t 120
    fail "Timed out waiting for Metabase."
    
    ./metabase-init.sh
    fail "Metabase initialization failed."
    
    echo "Metabase initialization completed successfully"
elif [ "$PLATFORM_SUPERSET" = "true" ]; then
    echo "BI tool integration enabled: Superset (from service.yaml)"
    
    # Validate required env vars for Superset
    validate_service_env "Superset" "SUPERSET_URL"
    
    echo "Initializing Superset..."
    ./wait-for/wait-for.sh "$superset_url"/health -t 120
    fail "Timed out waiting for Superset."
    
    ./superset-init.sh
    fail "Superset initialization failed."
    
    echo "Superset initialization completed successfully"
else
    echo "BI tool integration disabled (no BI tools enabled in service.yaml)"
fi

# Dagster initialization
# Note: Dagster initialization is handled by data-platform-service, not appbase-init
# The service state is checked in app/start.sh to conditionally start data-platform-service
PLATFORM_DAGSTER=$(get_service_state platform.dagster)
if [ "$PLATFORM_DAGSTER" = "true" ]; then
    echo "Dagster service enabled in service.yaml (initialization handled by data-platform-service)"
else
    echo "Dagster service disabled in service.yaml"
fi

node ../lib/banner
exit 0
