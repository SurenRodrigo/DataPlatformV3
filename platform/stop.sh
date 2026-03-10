#!/bin/bash
set -eo pipefail

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

# Check if .env file exists
if [ ! -f .env ]; then
    printf "Error: .env file not found in platform directory\n" >&2
    exit 1
fi
set -a
source .env
set +a

# Check if service.yaml exists
service_yaml="../service.yaml"
if [ ! -f "$service_yaml" ]; then
    printf "Error: service.yaml not found at %s\n" "$service_yaml" >&2
    printf "Please ensure service.yaml exists in the project root.\n" >&2
    exit 1
fi

# Read service states from service.yaml
PLATFORM_DB=$(get_service_state "platform.db" "$service_yaml")
PLATFORM_HASURA=$(get_service_state "platform.hasura" "$service_yaml")
PLATFORM_METABASE=$(get_service_state "platform.metabase" "$service_yaml")
PLATFORM_SUPERSET=$(get_service_state "platform.superset" "$service_yaml")
PLATFORM_DAGSTER=$(get_service_state "platform.dagster" "$service_yaml")
PLATFORM_NGINX=$(get_service_state "platform.nginx" "$service_yaml")
PLATFORM_CLOUDBEAVER=$(get_service_state "platform.cloudbeaver" "$service_yaml")

# Base docker-compose command
compose_command="docker compose"

# Database service configuration
if [ "$PLATFORM_DB" = "local" ]; then
    printf "Local database service enabled, including profile for shutdown.\n"
    compose_command+=" --profile local-db"
else
    printf "External database mode detected (db=%s).\n" "$PLATFORM_DB"
    # Use override file to remove depends_on: db references
    if [ -f "docker-compose.no-db.yaml" ]; then
        compose_command="docker compose -f docker-compose.yaml -f docker-compose.no-db.yaml"
        printf "Using override file to remove db dependencies\n"
    else
        printf "Warning: docker-compose.no-db.yaml not found. Proceeding with standard compose.\n" >&2
    fi
fi

# Hasura service configuration
if [ "$PLATFORM_HASURA" = "true" ]; then
    printf "Hasura GraphQL engine enabled, including profile for shutdown.\n"
    compose_command+=" --profile hasura"
else
    printf "Hasura GraphQL engine disabled.\n"
fi

# BI tool selection logic (can enable both simultaneously)
if [ "$PLATFORM_METABASE" = "true" ] && [ "$PLATFORM_SUPERSET" = "true" ]; then
    printf "Stopping BI tools: Both Metabase and Superset\n"
    compose_command+=" --profile metabase"
    compose_command+=" --profile superset"
elif [ "$PLATFORM_METABASE" = "true" ]; then
    printf "Stopping BI tool: Metabase\n"
    compose_command+=" --profile metabase"
elif [ "$PLATFORM_SUPERSET" = "true" ]; then
    printf "Stopping BI tool: Superset\n"
    compose_command+=" --profile superset"
else
    printf "BI tool integration disabled.\n"
fi

# CloudBeaver web database manager
if [ "$PLATFORM_CLOUDBEAVER" = "true" ]; then
    printf "Stopping CloudBeaver web database manager.\n"
    compose_command+=" --profile cloudbeaver"
else
    printf "CloudBeaver web database manager disabled.\n"
fi

# Dagster data orchestration selection logic
if [ "$PLATFORM_DAGSTER" = "true" ]; then
    printf "Stopping Dagster data orchestration.\n"
    compose_command+=" --profile dagster"
else
    printf "Dagster data orchestration disabled.\n"
fi

# Nginx reverse proxy configuration
if [ "$PLATFORM_NGINX" = "true" ]; then
    printf "Stopping Nginx reverse proxy.\n"
    compose_command+=" --profile nginx"
else
    printf "Nginx reverse proxy disabled.\n"
fi

# Run the composed command
printf "Stopping AppBase Platform with command: %s\n" "$compose_command"
$compose_command down

# (Removed workspace.yaml reset section)

exit