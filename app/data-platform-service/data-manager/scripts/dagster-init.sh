#!/usr/bin/env bash

set -eo pipefail

# Function to start a gRPC server
start_grpc_server() {
    local name=$1
    local module=$2
    local port=$3
    local host=$4
    local description=$5
    
    echo "🔍 [DEBUG] ENTERING start_grpc_server function for '$name'"
    
    echo "Starting gRPC server for '$name' (module: $module, port: $port)..."
    
    # Ensure we're in the right directory for module loading
    echo "🔍 [DEBUG] Changing to /app directory"
    cd /app
    echo "🔍 [DEBUG] Current directory: $(pwd)"
    
    # Start the gRPC server in background with proper environment variables
    echo "🔍 [DEBUG] Starting gRPC server: dagster api grpc --module-name $module --host 0.0.0.0 --port $port"
    DAGSTER_HOME=/opt/dagster/dagster_home dagster api grpc --module-name "$module" --host "0.0.0.0" --port "$port" &
    local grpc_pid=$!
    
    echo "✅ gRPC server '$name' started with PID: $grpc_pid"
    echo "🔍 [DEBUG] Background process started, PID captured: $grpc_pid"
    
    # Wait for gRPC server to be ready
    local max_grpc_retries=10
    local grpc_retry_count=0
    local grpc_ready=false
    
    echo "🔍 [DEBUG] Starting health check loop for '$name' (PID: $grpc_pid, Port: $port)"
    
    while [[ $grpc_retry_count -lt $max_grpc_retries && "$grpc_ready" == "false" ]]; do
        grpc_retry_count=$((grpc_retry_count + 1))
        echo "🔍 [DEBUG] Health check attempt $grpc_retry_count/$max_grpc_retries for '$name'"
        
        # Check if the gRPC server process is still running
        if ! kill -0 $grpc_pid 2>/dev/null; then
            echo "❌ gRPC server '$name' process died (PID: $grpc_pid)"
            return 1
        fi
        echo "✅ gRPC server '$name' process is still running (PID: $grpc_pid)"
        
        # Check if port is listening
        echo "🔍 [DEBUG] Checking if port $port is listening..."
        if nc -z localhost "$port" 2>/dev/null; then
            echo "✅ Port $port is listening"
        else
            echo "❌ Port $port is not listening yet"
        fi
        
        # Use Dagster's grpc-health-check for readiness with timeout
        echo "🔍 [DEBUG] Running health check: dagster api grpc-health-check --port $port --host localhost"
        if timeout 10 dagster api grpc-health-check --port "$port" --host "localhost" 2>/dev/null; then
            grpc_ready=true
            echo "✅ gRPC server '$name' is healthy and ready on port $port"
            echo "🔍 [DEBUG] Health check SUCCESS for '$name' - exiting loop"
        else
            echo "❌ Health check failed for gRPC server '$name' on port $port (timeout or error)"
            if [[ $grpc_retry_count -lt $max_grpc_retries ]]; then
                echo "⏳ gRPC server '$name' not ready yet, waiting 3 seconds..."
                sleep 3
            fi
        fi
    done
    
    if [[ "$grpc_ready" == "false" ]]; then
        echo "❌ gRPC server '$name' failed to become healthy after $max_grpc_retries attempts"
        kill $grpc_pid 2>/dev/null || true
        return 1
    fi
    
    # Store PID for cleanup
    echo "$grpc_pid" > "/tmp/grpc_${name}.pid"
    echo "🔍 [DEBUG] EXITING start_grpc_server function for '$name' - returning PID: $grpc_pid"
    echo "$grpc_pid"
}

# Export the function to make it available in subprocesses
export -f start_grpc_server

# Function to generate workspace.yaml content
generate_workspace_yaml() {
    local config=$1
    local workspace_name=$2
    local workspace_description=$3
    
    echo "# Dagster Workspace Configuration - Multiple gRPC Servers"
    echo "# Generated from configuration at: $(date)"
    echo "# Workspace: $workspace_name"
    echo "# Description: $workspace_description"
    echo ""
    echo "load_from:"
    
    # Extract enabled code locations
    local enabled_locations=$(echo "$config" | jq -r '.code_locations[] | select(.enabled == true) | @base64')
    
    if [[ -z "$enabled_locations" ]]; then
        echo "  # No enabled code locations found"
        return
    fi
    
    # Process each enabled location
    while IFS= read -r location_base64; do
        if [[ -n "$location_base64" ]]; then
            local location_json=$(echo "$location_base64" | base64 -d)
            local name=$(echo "$location_json" | jq -r '.name')
            local host=$(echo "$location_json" | jq -r '.host')
            local port=$(echo "$location_json" | jq -r '.port')
            local description=$(echo "$location_json" | jq -r '.description')
            
            echo "  - grpc_server:"
            echo "      host: $host"
            echo "      port: $port"
            echo "      location_name: $name"
            echo "      # $description"
            echo ""
        fi
    done <<< "$enabled_locations"
}

# Export the generate_workspace_yaml function as well
export -f generate_workspace_yaml

# Main execution starts here
echo "🔍 [DEBUG] SCRIPT START: dagster-init.sh execution begins"
echo "=================================================="
echo "Starting Dagster Multi-gRPC Server initialization..."
echo "=================================================="

# Debug: Verify function availability
echo "🔍 [DEBUG] Verifying function availability..."
if type start_grpc_server >/dev/null 2>&1; then
    echo "✅ start_grpc_server function is available"
else
    echo "❌ start_grpc_server function is NOT available"
    exit 1
fi

# Environment variable validation
[[ -z "$DAGSTER_URL" ]] && echo "DAGSTER_URL not set" && exit 1

dagster_url=$DAGSTER_URL
admin_user=${DAGSTER_ADMIN_USER:-admin}
admin_password=${DAGSTER_ADMIN_PASSWORD:-admin}

# Configuration file paths
config_dir="/app/data-manager/resources/dagster"
code_locations_config="$config_dir/code-locations.json"
schema_file="$config_dir/schema.json"

echo "Dagster URL: $dagster_url"
echo "Configuration Directory: $config_dir"
echo "Code Locations Config: $code_locations_config"
echo "Architecture: Multiple gRPC Servers with Configuration-Driven Setup"

# Validate configuration file exists
if [[ ! -f "$code_locations_config" ]]; then
    echo "❌ Configuration file not found: $code_locations_config"
    exit 1
fi

# Validate JSON configuration
echo "Validating configuration file..."
if ! jq empty "$code_locations_config" 2>/dev/null; then
    echo "❌ Invalid JSON in configuration file: $code_locations_config"
    exit 1
fi

# Load configuration
echo "Loading code locations configuration..."
config=$(cat "$code_locations_config")
workspace_name=$(echo "$config" | jq -r '.config.workspace_name // "appbase-dagster-workspace"')
workspace_description=$(echo "$config" | jq -r '.config.description // "AppBase Data Platform Dagster Workspace"')

echo "Workspace Name: $workspace_name"
echo "Workspace Description: $workspace_description"

# Check if Dagster webserver is running
echo "Checking Dagster webserver health..."
max_health_retries=5
health_retry_count=0
webserver_ready=false

while [[ $health_retry_count -lt $max_health_retries && "$webserver_ready" == "false" ]]; do
    health_retry_count=$((health_retry_count + 1))
    echo "Health check attempt $health_retry_count/$max_health_retries..."
    
    if [[ "$(curl -o /dev/null -s -w "%{http_code}\n" "$dagster_url/server_info")" == "200" ]]; then
        webserver_ready=true
        echo "✅ Dagster webserver is healthy"
    else
        if [[ $health_retry_count -lt $max_health_retries ]]; then
            echo "⏳ Dagster webserver not ready, waiting 5 seconds..."
            sleep 5
        fi
    fi
done

if [[ "$webserver_ready" == "false" ]]; then
    echo "❌ Dagster webserver health check failed after $max_health_retries attempts"
    echo "URL: $dagster_url/server_info"
    exit 1
fi

# Start all enabled gRPC servers
echo "Starting enabled gRPC servers..."
enabled_locations=$(echo "$config" | jq -r '.code_locations[] | select(.enabled == true) | @base64')
started_servers=()
failed_servers=()

if [[ -z "$enabled_locations" ]]; then
    echo "⚠️  No enabled code locations found in configuration"
else
    while IFS= read -r location_base64; do
        if [[ -n "$location_base64" ]]; then
            location_json=$(echo "$location_base64" | base64 -d)
            name=$(echo "$location_json" | jq -r '.name')
            module=$(echo "$location_json" | jq -r '.module')
            port=$(echo "$location_json" | jq -r '.port')
            host=$(echo "$location_json" | jq -r '.host')
            description=$(echo "$location_json" | jq -r '.description')
            
            echo "🚀 Processing code location: $name"
            echo "  Module: $module"
            echo "  Port: $port"
            echo "  Host: $host"
            echo "  Description: $description"
            echo "🔍 [DEBUG] About to call start_grpc_server function for '$name'"
            echo "🔍 [DEBUG] Function call: start_grpc_server '$name' '$module' '$port' '$host' '$description'"
            echo "🔍 [DEBUG] Parameter values: name='$name', module='$module', port='$port', host='$host', description='$description'"
            
            # Inline gRPC server startup logic instead of function call
            echo "🔍 [DEBUG] ENTERING inline gRPC server startup for '$name'"
            echo "Starting gRPC server for '$name' (module: $module, port: $port)..."
            
            # Ensure we're in the right directory for module loading
            echo "🔍 [DEBUG] Changing to /app directory"
            cd /app
            echo "🔍 [DEBUG] Current directory: $(pwd)"
            
            # Start the gRPC server in background with proper environment variables
            echo "🔍 [DEBUG] Starting gRPC server: dagster api grpc --module-name $module --host 0.0.0.0 --port $port"
            DAGSTER_HOME=/opt/dagster/dagster_home dagster api grpc --module-name "$module" --host "0.0.0.0" --port "$port" &
            grpc_pid=$!
            
            echo "✅ gRPC server '$name' started with PID: $grpc_pid"
            echo "🔍 [DEBUG] Background process started, PID captured: $grpc_pid"
            
            # Wait for gRPC server to be ready
            max_grpc_retries=10
            grpc_retry_count=0
            grpc_ready=false
            
            echo "🔍 [DEBUG] Starting health check loop for '$name' (PID: $grpc_pid, Port: $port)"
            
            while [[ $grpc_retry_count -lt $max_grpc_retries && "$grpc_ready" == "false" ]]; do
                grpc_retry_count=$((grpc_retry_count + 1))
                echo "🔍 [DEBUG] Health check attempt $grpc_retry_count/$max_grpc_retries for '$name'"
                
                # Check if the gRPC server process is still running
                if ! kill -0 $grpc_pid 2>/dev/null; then
                    echo "❌ gRPC server '$name' process died (PID: $grpc_pid)"
                    failed_servers+=("$name")
                    echo "❌ Failed to start gRPC server for '$name'"
                    echo "🔍 [DEBUG] Inline logic FAILED for '$name'"
                    continue 2
                fi
                echo "✅ gRPC server '$name' process is still running (PID: $grpc_pid)"
                
                # Check if port is listening
                echo "🔍 [DEBUG] Checking if port $port is listening..."
                if nc -z localhost "$port" 2>/dev/null; then
                    echo "✅ Port $port is listening"
                else
                    echo "❌ Port $port is not listening yet"
                fi
                
                # Use Dagster's grpc-health-check for readiness with timeout
                echo "🔍 [DEBUG] Running health check: dagster api grpc-health-check --port $port --host localhost"
                if timeout 10 dagster api grpc-health-check --port "$port" --host "localhost" 2>/dev/null; then
                    grpc_ready=true
                    echo "✅ gRPC server '$name' is healthy and ready on port $port"
                    echo "🔍 [DEBUG] Health check SUCCESS for '$name' - exiting loop"
                else
                    echo "❌ Health check failed for gRPC server '$name' on port $port (timeout or error)"
                    if [[ $grpc_retry_count -lt $max_grpc_retries ]]; then
                        echo "⏳ gRPC server '$name' not ready yet, waiting 3 seconds..."
                        sleep 3
                    fi
                fi
            done
            
            if [[ "$grpc_ready" == "false" ]]; then
                echo "❌ gRPC server '$name' failed to become healthy after $max_grpc_retries attempts"
                kill $grpc_pid 2>/dev/null || true
                failed_servers+=("$name")
                echo "❌ Failed to start gRPC server for '$name'"
                echo "🔍 [DEBUG] Inline logic FAILED for '$name'"
            else
                # Store PID for cleanup
                echo "$grpc_pid" > "/tmp/grpc_${name}.pid"
                echo "🔍 [DEBUG] EXITING inline logic for '$name' - returning PID: $grpc_pid"
                started_servers+=("$name:$grpc_pid")
                echo "✅ Successfully started gRPC server for '$name' (PID: $grpc_pid)"
                echo "🔍 [DEBUG] Inline logic SUCCESS for '$name' - returning to main loop"
            fi
        fi
    done <<< "$enabled_locations"
fi

# Check if any servers failed to start
if [[ ${#failed_servers[@]} -gt 0 ]]; then
    echo "❌ Some gRPC servers failed to start:"
    printf '  - %s\n' "${failed_servers[@]}"
    
    # Clean up started servers
    for server_info in "${started_servers[@]}"; do
        name=$(echo "$server_info" | cut -d: -f1)
        pid=$(echo "$server_info" | cut -d: -f2)
        echo "Stopping gRPC server '$name' (PID: $pid)..."
        kill $pid 2>/dev/null || true
    done
    
    exit 1
fi

if [[ ${#started_servers[@]} -eq 0 ]]; then
    echo "⚠️  No gRPC servers were started"
else
    echo "✅ Successfully started ${#started_servers[@]} gRPC server(s):"
    for server_info in "${started_servers[@]}"; do
        name=$(echo "$server_info" | cut -d: -f1)
        pid=$(echo "$server_info" | cut -d: -f2)
        echo "  - $name (PID: $pid)"
    done
fi

# Note: No workspace.yaml generation needed - using pure API-based registration
echo "Using pure API-based registration (zero file coupling)..."
echo "✅ gRPC servers are ready for platform registration"

# Trigger workspace reload to pick up all gRPC server locations
echo "Triggering workspace reload to register all gRPC servers..."
reload_response=$(curl -s \
  -H "Content-Type: application/json" \
  "$dagster_url/graphql" \
  -d '{"query": "mutation { reloadWorkspace { __typename ... on WorkspaceLocationEntry { name loadStatus } ... on PythonError { message } } }"}' \
  2>/dev/null || echo '{"data": {"reloadWorkspace": []}}')

echo "Reload response:"
echo "$reload_response" | jq '.' || echo "$reload_response"

# Wait for workspace reload to complete
echo "Waiting for workspace reload to complete..."
sleep 10

# Verify all gRPC code location registrations
echo "Verifying gRPC code location registrations..."
max_verification_retries=8
verification_retry_count=0
all_locations_loaded=false

while [[ $verification_retry_count -lt $max_verification_retries && "$all_locations_loaded" == "false" ]]; do
    verification_retry_count=$((verification_retry_count + 1))
    echo "Verification attempt $verification_retry_count/$max_verification_retries..."
    
    workspace_query='{"query": "{ workspaceOrError { __typename ... on Workspace { locationEntries { __typename id name loadStatus } } } }"}'
    
    workspace_status=$(curl -s \
      -H "Content-Type: application/json" \
      "$dagster_url/graphql" \
      -d "$workspace_query" \
      2>/dev/null || echo '{"data": {"workspaceOrError": {"locationEntries": []}}}')
    
    # Check each expected location
    expected_locations=()
    while IFS= read -r location_base64; do
        if [[ -n "$location_base64" ]]; then
            location_json=$(echo "$location_base64" | base64 -d)
            name=$(echo "$location_json" | jq -r '.name')
            expected_locations+=("$name")
        fi
    done <<< "$enabled_locations"
    
    all_loaded=true
    for expected_location in "${expected_locations[@]}"; do
        location_status=$(echo "$workspace_status" | jq -r ".data.workspaceOrError.locationEntries[] | select(.name == \"$expected_location\") | .loadStatus" 2>/dev/null || echo "")
        
        if [[ "$location_status" == "LOADED" ]]; then
            echo "✅ gRPC code location '$expected_location' is loaded and ready!"
        else
            echo "⏳ gRPC code location '$expected_location' status: ${location_status:-NOT_FOUND}"
            all_loaded=false
        fi
    done
    
    if [[ "$all_loaded" == "true" ]]; then
        echo "✅ All gRPC code locations are loaded and ready!"
        all_locations_loaded=true
    else
        if [[ $verification_retry_count -lt $max_verification_retries ]]; then
            echo "Waiting 10 seconds before retry..."
            sleep 10
        fi
    fi
done

if [[ "$all_locations_loaded" == "false" ]]; then
    echo "❌ Failed to verify all gRPC code locations after $max_verification_retries attempts"
    echo "Current workspace status:"
    echo "$workspace_status" | jq '.' || echo "$workspace_status"
    echo ""
    echo "🔍 Troubleshooting suggestions:"
    echo "1. Check gRPC server logs for errors"
    echo "2. Verify network connectivity between platform and app containers"
    echo "3. Check that all modules load correctly"
    echo "4. Ensure Dagster daemon can connect to all gRPC servers"
    
    # Clean up started servers
    for server_info in "${started_servers[@]}"; do
        name=$(echo "$server_info" | cut -d: -f1)
        pid=$(echo "$server_info" | cut -d: -f2)
        echo "Stopping gRPC server '$name' (PID: $pid)..."
        kill $pid 2>/dev/null || true
    done
    
    exit 1
fi

# Verify pipeline and asset definitions are available via gRPC
echo "Verifying pipeline and asset definitions via gRPC..."

# Count total pipelines
pipelines_query='{"query": "{ pipelinesOrError { __typename ... on PipelineConnection { nodes { name } } } }"}'
pipelines_status=$(curl -s \
  -H "Content-Type: application/json" \
  "$dagster_url/graphql" \
  -d "$pipelines_query" \
  2>/dev/null || echo '{"data": {"pipelinesOrError": {"nodes": []}}}')

pipeline_count=$(echo "$pipelines_status" | jq -r '.data.pipelinesOrError.nodes | length' 2>/dev/null || echo "0")
pipeline_names=$(echo "$pipelines_status" | jq -r '.data.pipelinesOrError.nodes[]?.name // empty' 2>/dev/null || echo "")

echo "Available pipelines ($pipeline_count): ${pipeline_names:-none}"

# Count total assets
assets_query='{"query": "{ assetsOrError { __typename ... on AssetConnection { nodes { key { path } } } } }"}'
assets_status=$(curl -s \
  -H "Content-Type: application/json" \
  "$dagster_url/graphql" \
  -d "$assets_query" \
  2>/dev/null || echo '{"data": {"assetsOrError": {"nodes": []}}}')

asset_count=$(echo "$assets_status" | jq -r '.data.assetsOrError.nodes | length' 2>/dev/null || echo "0")
asset_names=$(echo "$assets_status" | jq -r '.data.assetsOrError.nodes[]?.key.path | join(".")' 2>/dev/null | tr '\n' ' ' || echo "")

echo "Available assets ($asset_count): ${asset_names:-none}"

# Keep all gRPC servers running
echo "All gRPC servers will continue running in background..."
for server_info in "${started_servers[@]}"; do
    name=$(echo "$server_info" | cut -d: -f1)
    pid=$(echo "$server_info" | cut -d: -f2)
    echo "  - $name (PID: $pid)"
done

# Success summary
echo "=================================================="
echo "✅ Dagster Multi-gRPC Server initialization completed!"
echo "=================================================="
echo "📊 Summary:"
echo "   - Architecture: Multiple gRPC Servers with Configuration-Driven Setup"
echo "   - Webserver: $dagster_url"
echo "   - Code Locations Started: ${#started_servers[@]}"
echo "   - Workspace: $workspace_name"
echo "   - Business Logic: 100% in app container"
echo "   - Platform Coupling: ZERO - only network communication"
echo "   - Pipelines: $pipeline_count"
echo "   - Assets: $asset_count"
echo "   - Status: Ready for data orchestration"
echo "=================================================="

# Wait for all gRPC servers to keep running
echo "Waiting for gRPC servers to continue running..."
wait 