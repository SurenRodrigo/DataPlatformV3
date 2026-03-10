#!/bin/bash
set -e

# ============================================================================
# PostgreSQL Database Initialization Script
# ============================================================================
# This script creates service databases based on service.yaml configuration.
# 
# IMPORTANT: service.yaml is the SINGLE SOURCE OF TRUTH for which service
# databases should be created. This script reads directly from service.yaml
# (mounted at /app/service.yaml) to determine which databases to create.
# ============================================================================

# Function to get service state from service.yaml
# Returns: "true", "false", or "local"/"external" for db
get_service_state() {
    local service_path="$1"
    local service_yaml="${2:-/app/service.yaml}"
    
    if [ ! -f "$service_yaml" ]; then
        echo "false"
        return
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

# Start PostgreSQL in the background
/usr/local/bin/docker-entrypoint.sh postgres &

# Wait for PostgreSQL to be ready
until pg_isready -U "$POSTGRES_USER"; do
  echo "Waiting for PostgreSQL to be ready..."
  sleep 2
done

echo "PostgreSQL is ready. Starting database creation..."

# Function to create database if it doesn't exist
create_database_if_not_exists() {
    local db_name="$1"
    local db_user="$2"
    
    echo "Checking if database '$db_name' exists..."
    if psql -U "$db_user" -lqt | cut -d \| -f 1 | grep -qw "$db_name"; then
        echo "Database '$db_name' already exists, skipping creation."
    else
        echo "Creating database '$db_name'..."
        createdb -U "$db_user" "$db_name"
        echo "Database '$db_name' created successfully."
    fi
}

# Read service states from service.yaml (SINGLE SOURCE OF TRUTH)
PLATFORM_METABASE=$(get_service_state "platform.metabase")
PLATFORM_SUPERSET=$(get_service_state "platform.superset")
PLATFORM_DAGSTER=$(get_service_state "platform.dagster")

# Create Hasura database (always needed)
create_database_if_not_exists "$HASURA_DB_NAME" "$POSTGRES_USER"

# Create BI tool databases if enabled in service.yaml
if [ "$PLATFORM_METABASE" = "true" ] && [ "$PLATFORM_SUPERSET" = "true" ]; then
    echo "BI tools enabled: Both Metabase and Superset (from service.yaml)"
    create_database_if_not_exists "$METABASE_DB_NAME" "$POSTGRES_USER"
    create_database_if_not_exists "$SUPERSET_DB_NAME" "$POSTGRES_USER"
elif [ "$PLATFORM_METABASE" = "true" ]; then
    echo "BI tool enabled: Metabase (from service.yaml)"
    create_database_if_not_exists "$METABASE_DB_NAME" "$POSTGRES_USER"
elif [ "$PLATFORM_SUPERSET" = "true" ]; then
    echo "BI tool enabled: Superset (from service.yaml)"
    create_database_if_not_exists "$SUPERSET_DB_NAME" "$POSTGRES_USER"
else
    echo "BI tool integration disabled (no BI tools enabled in service.yaml)"
fi

# Create Dagster database if enabled in service.yaml
if [ "$PLATFORM_DAGSTER" = "true" ]; then
    echo "Dagster orchestration enabled (from service.yaml)"
    create_database_if_not_exists "$DAGSTER_DB_NAME" "$POSTGRES_USER"
else
    echo "Dagster orchestration disabled (not enabled in service.yaml)"
fi

echo "Database initialization completed successfully!"
echo "Created databases:"
echo "  - Main database: $POSTGRES_DB"
echo "  - Hasura database: $HASURA_DB_NAME"
if [ "$PLATFORM_METABASE" = "true" ] && [ "$PLATFORM_SUPERSET" = "true" ]; then
    echo "  - Metabase database: $METABASE_DB_NAME"
    echo "  - Superset database: $SUPERSET_DB_NAME"
elif [ "$PLATFORM_METABASE" = "true" ]; then
    echo "  - Metabase database: $METABASE_DB_NAME"
elif [ "$PLATFORM_SUPERSET" = "true" ]; then
    echo "  - Superset database: $SUPERSET_DB_NAME"
fi
if [ "$PLATFORM_DAGSTER" = "true" ]; then
    echo "  - Dagster database: $DAGSTER_DB_NAME"
fi

# Wait for the original postgres process to keep the container running
wait 