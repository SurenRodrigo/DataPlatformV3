#!/bin/bash

# AppBase Application Startup Script
# This script starts the application initialization service after the platform is running

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default settings
USE_NO_CACHE=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --no-cache|-n)
      USE_NO_CACHE=true
      shift
      ;;
    --help|-h)
      echo "Usage: ./start.sh [options]"
      echo "Options:"
      echo "  --no-cache, -n     Build Docker image without cache"
      echo "  --help, -h         Show this help message"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      echo "Use --help for available options"
      exit 1
      ;;
  esac
done

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

# Function to check if a service is running
check_service() {
    local service_name=$1
    local port=$2
    
    if [ "$service_name" = "database" ]; then
        # Check if database is accessible using configured connection
        # If APPBASE_DB_HOST is 'db' (Docker service name), use localhost instead
        # since we're running from the host machine where 'db' is not resolvable
        local db_host="${APPBASE_DB_HOST:-localhost}"
        if [ "$db_host" = "db" ]; then
            db_host="localhost"
        fi
        local db_port="${APPBASE_DB_PORT:-5432}"
        
        # Use netcat for basic port connectivity check
        if command -v nc > /dev/null 2>&1; then
            if nc -z "$db_host" "$db_port" 2>/dev/null; then
                return 0
            else
                return 1
            fi
        else
            print_warning "nc (netcat) not found. Skipping database connectivity check."
            return 0
        fi
    else
        # Use curl for HTTP services
        # Special handling for different services with their health endpoints
        local health_url="http://localhost:$port"
        if [ "$service_name" = "superset" ]; then
            health_url="http://localhost:$port/health"
        elif [ "$service_name" = "metabase" ]; then
            health_url="http://localhost:$port/api/health"
        elif [ "$service_name" = "hasura" ]; then
            health_url="http://localhost:$port/healthz"
        fi
        
        # Try health endpoint first
        if curl -s -f "$health_url" > /dev/null 2>&1; then
            return 0
        fi
        
        # If health endpoint fails, check if port is open (service might still be initializing)
        # This is more lenient and allows services that are starting up
        if command -v nc > /dev/null 2>&1; then
            if nc -z localhost "$port" 2>/dev/null; then
                return 0
            fi
        fi
        
        return 1
    fi
}

# Function to wait for a service to be ready
wait_for_service() {
    local service_name=$1
    local port=$2
    local max_attempts=30
    local attempt=1
    
    print_status "Waiting for $service_name to be ready on port $port..."
    
    while [ $attempt -le $max_attempts ]; do
        if check_service "$service_name" "$port"; then
            print_success "$service_name is ready!"
            return 0
        fi
        
        print_status "Attempt $attempt/$max_attempts: $service_name not ready yet, waiting..."
        sleep 2
        attempt=$((attempt + 1))
    done
    
    print_error "Timeout waiting for $service_name to be ready"
    return 1
}

# Function to parse service.yaml and get service state
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

# Load environment variables from local .env file
if [ -f .env ]; then
    print_status "Loading environment variables from .env"
    set -a
    source .env
    set +a
else
    print_error ".env file not found in app directory"
    exit 1
fi

# Check if service.yaml exists
if [ ! -f ../service.yaml ]; then
    print_error "service.yaml not found in project root"
    print_error "Please ensure service.yaml exists in the project root directory"
    exit 1
fi
print_success "service.yaml found"

# Check if platform is running
print_status "Checking if platform services are running..."

# Check database
# Determine the actual host for error messages (convert 'db' to 'localhost')
db_check_host="${APPBASE_DB_HOST:-localhost}"
if [ "$db_check_host" = "db" ]; then
    db_check_host="localhost"
fi
if ! check_service "database" "5432"; then
    print_error "Database is not accessible at ${db_check_host}:${APPBASE_DB_PORT:-5432}"
    print_error "Please ensure the database is running and accessible"
    if [ "$db_check_host" = "localhost" ]; then
        print_error "Make sure the platform services are started first using: cd ../platform && ./start.sh"
    else
        print_error "Check your .env file for APPBASE_DB_HOST and APPBASE_DB_PORT settings"
    fi
    exit 1
fi

# Read service states from service.yaml
PLATFORM_HASURA=$(get_service_state "platform.hasura")
PLATFORM_METABASE=$(get_service_state "platform.metabase")
PLATFORM_SUPERSET=$(get_service_state "platform.superset")
PLATFORM_DAGSTER=$(get_service_state "platform.dagster")

# Read app service states from service.yaml
APP_DATA_PLATFORM_SERVICE=$(get_service_state "app.data-platform-service")

# Check Hasura if enabled
if [ "$PLATFORM_HASURA" = "true" ]; then
    if ! check_service "hasura" "8081"; then
        print_error "Hasura is not running on port 8081"
        print_error "Please start the platform first using: cd ../platform && ./start.sh"
        exit 1
    fi
    print_success "Hasura is running"
fi

# Check BI tools if enabled (can enable both simultaneously)
if [ "$PLATFORM_METABASE" = "true" ]; then
    if ! check_service "metabase" "3000"; then
        print_error "Metabase is not running on port 3000"
        print_error "Please ensure platform is started with metabase enabled in service.yaml"
        exit 1
    fi
    print_success "Metabase is running"
fi

if [ "$PLATFORM_SUPERSET" = "true" ]; then
    if ! check_service "superset" "8088"; then
        print_error "Superset is not running on port 8088"
        print_error "Please ensure platform is started with superset enabled in service.yaml"
        exit 1
    fi
    print_success "Superset is running"
fi

# Check Dagster if enabled
if [ "$PLATFORM_DAGSTER" = "true" ]; then
    print_status "Dagster integration enabled, checking Dagster services..."
    DAGSTER_PORT="${DAGSTER_PORT:-3030}"
    if ! check_service "dagster" "${DAGSTER_PORT}"; then
        print_error "Dagster webserver is not running on port ${DAGSTER_PORT}"
        print_error "Please ensure platform is started with dagster enabled in service.yaml"
        exit 1
    fi
    print_success "Dagster webserver is running"
fi

print_success "Platform services are running"

# Wait for services to be fully ready
# Determine the actual host for status messages (convert 'db' to 'localhost')
db_wait_host="${APPBASE_DB_HOST:-localhost}"
if [ "$db_wait_host" = "db" ]; then
    db_wait_host="localhost"
fi
print_status "Waiting for database at ${db_wait_host}:${APPBASE_DB_PORT:-5432}..."
wait_for_service "database" "5432"

# Wait for Hasura if enabled
if [ "$PLATFORM_HASURA" = "true" ]; then
    wait_for_service "hasura" "8081"
fi

# Wait for BI tools if enabled
if [ "$PLATFORM_METABASE" = "true" ]; then
    wait_for_service "metabase" "3000"
fi

if [ "$PLATFORM_SUPERSET" = "true" ]; then
    wait_for_service "superset" "8088"
fi

# Wait for Dagster if enabled
if [ "$PLATFORM_DAGSTER" = "true" ]; then
    DAGSTER_PORT="${DAGSTER_PORT:-3030}"
    wait_for_service "dagster" "${DAGSTER_PORT}"
fi

# Check if appbase-init is already running
if docker ps --format "table {{.Names}}" | grep -q "appbase-init"; then
    print_warning "appbase-init is already running"
    print_status "Stopping existing appbase-init container..."
    docker stop appbase-init
    docker rm appbase-init
fi

# Start the application initialization service
print_status "Starting application initialization service..."

# Build and start the application services
print_status "Building and starting application services..."

# Ensure buildx is available and create builder if needed
if ! docker buildx inspect multiarch-builder >/dev/null 2>&1; then
    print_status "Creating buildx builder for multi-architecture builds..."
    docker buildx create --name multiarch-builder --use --bootstrap --platform linux/amd64,linux/arm64 2>/dev/null || true
fi

# Use buildx for cross-platform builds
CACHE_OPTION=""
if [ "$USE_NO_CACHE" = true ]; then
    print_status "Building without cache (--no-cache flag provided)"
    CACHE_OPTION="--no-cache"
fi

# Build using docker compose with buildx (respects platform in docker-compose.yaml)
# Always build appbase-init (mandatory service)
print_status "Building appbase-init (mandatory service)..."
DOCKER_BUILDKIT=1 docker compose build $CACHE_OPTION appbase-init
APP_BASE_INIT_VERSION=$(docker image ls -q ${APPBASE_INIT_IMAGE:-AppBase/system-init:latest} | head -n 1)
print_status "AppBase init version: $APP_BASE_INIT_VERSION"

# Conditionally build data-platform-service if enabled
if [ "$APP_DATA_PLATFORM_SERVICE" = "true" ]; then
    print_status "Building data-platform-service (enabled in service.yaml)..."
    DOCKER_BUILDKIT=1 docker compose --profile data-platform-service build $CACHE_OPTION data-platform-service
else
    print_status "data-platform-service disabled in service.yaml, skipping build"
fi

# Start the application compose with appropriate profiles
print_status "Starting application services..."
compose_command="docker compose"

# Always start appbase-init (mandatory, no profile needed)
# Add profile for data-platform-service if enabled
if [ "$APP_DATA_PLATFORM_SERVICE" = "true" ]; then
    print_status "Including data-platform-service profile"
    compose_command+=" --profile data-platform-service"
fi


# Start services
$compose_command up -d

# Wait for appbase-init to be ready
print_status "Waiting for application initialization to complete..."
if docker compose logs -f appbase-init | grep -q "Application initialization completed"; then
    print_success "Application initialization completed successfully!"
else
    print_warning "Application initialization may still be in progress"
    print_status "You can check the logs with: docker compose logs -f appbase-init"
fi

# Verify app services are running according to service.yaml
print_status ""
print_status "Verifying app services against service.yaml configuration..."

# Verify appbase-init completion (it's a one-time initialization service that exits after completion)
# Check if container exists and completed successfully (exit code 0)
if docker ps -a --format "{{.Names}}" | grep -q "^appbase-init$"; then
    # Check if container is still running
    if docker ps --format "{{.Names}}" | grep -q "^appbase-init$"; then
        print_warning "  ⚠ appbase-init is still running (initialization in progress)"
    else
        # Container exists but is stopped - check exit code
        exit_code=$(docker inspect appbase-init --format='{{.State.ExitCode}}' 2>/dev/null || echo "-1")
        if [ "$exit_code" = "0" ]; then
            print_success "  ✓ appbase-init completed successfully (one-time initialization service)"
        elif [ "$exit_code" != "-1" ]; then
            print_error "  ✗ ERROR: appbase-init failed with exit code $exit_code"
            print_error "    Check logs with: docker compose logs appbase-init"
        else
            # Couldn't get exit code, check status string
            appbase_init_status=$(docker ps -a --format "{{.Names}} {{.Status}}" | grep "^appbase-init " | awk '{$1=""; print $0}' | xargs)
            if echo "$appbase_init_status" | grep -q "Exited (0)"; then
                print_success "  ✓ appbase-init completed successfully (one-time initialization service)"
            elif echo "$appbase_init_status" | grep -q "Exited"; then
                exit_code=$(echo "$appbase_init_status" | sed -n 's/.*Exited (\([0-9]*\)).*/\1/p')
                print_error "  ✗ ERROR: appbase-init failed with exit code $exit_code"
                print_error "    Check logs with: docker compose logs appbase-init"
            else
                print_warning "  ⚠ appbase-init status: $appbase_init_status"
            fi
        fi
    fi
else
    print_error "  ✗ ERROR: appbase-init container not found"
    print_error "    Check if initialization started: docker compose ps appbase-init"
fi

# Verify data-platform-service if enabled (this is a long-running service)
if [ "$APP_DATA_PLATFORM_SERVICE" = "true" ]; then
    if docker ps --format "{{.Names}}" | grep -q "^data-platform-service$"; then
        print_success "  ✓ data-platform-service is running"
    else
        print_error "  ✗ ERROR: data-platform-service should be running but is not"
    fi
else
    if docker ps --format "{{.Names}}" | grep -q "^data-platform-service$"; then
        print_warning "  ⚠ WARNING: data-platform-service is running but should be disabled in service.yaml"
    else
        print_success "  ✓ data-platform-service is correctly disabled"
    fi
fi


print_success "Application startup completed!"
print_status ""
print_status "Application services:"
print_status "  - appbase-init: Application initialization service (mandatory)"

if [ "$APP_DATA_PLATFORM_SERVICE" = "true" ]; then
    print_status "  - data-platform-service: Data orchestration service (enabled)"
    print_status "    - gRPC servers: ports 4266, 4269, 4270, 4271, 4272"
fi


if [ "$PLATFORM_DAGSTER" = "true" ]; then
    print_status ""
    print_status "Dagster Integration:"
    DAGSTER_PORT="${DAGSTER_PORT:-3030}"
    print_status "  - Platform Webserver: http://localhost:${DAGSTER_PORT}"
    if [ "$APP_DATA_PLATFORM_SERVICE" = "true" ]; then
        print_status "  - Data Platform Service: Running (gRPC code locations active)"
    fi
fi

print_status ""
print_status "To view application logs:"
print_status "  docker compose logs -f appbase-init"
print_status ""
print_status "To stop application services:"
print_status "  ./stop.sh" 
