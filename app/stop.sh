#!/bin/bash

# AppBase Application Stop Script
# This script stops the application initialization service

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
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
        else
            echo "$value"
        fi
        return
    fi
    
    # Fallback: simple bash-based YAML parser
    if [ ! -f "$service_yaml" ]; then
        echo "false"
        return
    fi
    
    # Determine which section we're looking for (platform or app)
    local section=""
    if [[ "$service_path" =~ ^platform\. ]]; then
        section="platform"
    elif [[ "$service_path" =~ ^app\. ]]; then
        section="app"
    else
        echo "false"
        return
    fi
    
    local key_name="${service_path##*.}"
    local in_section=false
    local value=""
    
    while IFS= read -r line || [ -n "$line" ]; do
        # Check if we're entering the target section
        if [[ "$line" =~ ^${section}: ]]; then
            in_section=true
            continue
        fi
        
        # If we hit another top-level key, we're out of the target section
        if [[ "$in_section" = true ]] && [[ "$line" =~ ^[a-zA-Z_]+: ]] && [[ ! "$line" =~ ^[[:space:]] ]]; then
            break
        fi
        
        # Look for the key we want within the target section
        if [[ "$in_section" = true ]] && [[ "$line" =~ ^[[:space:]]+${key_name}: ]]; then
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

# Check if service.yaml exists
if [ ! -f ../service.yaml ]; then
    print_error "service.yaml not found in project root"
    print_error "Please ensure service.yaml exists in the project root directory"
    exit 1
fi

# Read app service states from service.yaml
APP_DATA_PLATFORM_SERVICE=$(get_service_state "app.data-platform-service")
PLATFORM_DAGSTER=$(get_service_state "platform.dagster")

# Check if application services are running or exist
RUNNING_SERVICES=$(docker ps -a --format "table {{.Names}}" | grep -E "(appbase-init|data-platform-service)" | wc -l)
if [ "$RUNNING_SERVICES" -eq 0 ]; then
    print_warning "No application services are currently running or present"
    exit 0
fi

print_status "Stopping application services..."

# Build docker compose command with appropriate profiles
compose_command="docker compose"

# Always stop appbase-init (mandatory, no profile needed)
# Add profile for data-platform-service if enabled
if [ "$APP_DATA_PLATFORM_SERVICE" = "true" ]; then
    print_status "Including data-platform-service profile for shutdown"
    compose_command+=" --profile data-platform-service"
fi


# Stop the application compose
print_status "Stopping with command: $compose_command down"
$compose_command down

print_success "Application services stopped successfully!"
print_status ""
print_status "Services stopped:"
print_status "  - appbase-init: Application initialization service (mandatory)"

if [ "$APP_DATA_PLATFORM_SERVICE" = "true" ]; then
    print_status "  - data-platform-service: Data orchestration service"
fi


if [ "$PLATFORM_DAGSTER" = "true" ]; then
    print_status ""
    print_status "Dagster Status:"
    print_status "  - Platform services: Still running (webserver: http://localhost:3030)"
    if [ "$APP_DATA_PLATFORM_SERVICE" = "true" ]; then
        print_status "  - Data Platform Service: Stopped (gRPC code locations unloaded)"
    fi
    print_status "  - Note: To fully stop Dagster, use platform stop script"
fi

print_status ""
print_status "Note: Platform services (database, hasura, bi tools) are still running"
print_status "To stop platform services, use: cd ../platform && ./stop.sh" 