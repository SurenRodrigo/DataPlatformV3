#!/usr/bin/env bash

set -eo pipefail

[[ -z "$SUPERSET_URL" ]] && echo "SUPERSET_URL not set" && exit 1
[[ -z "$SUPERSET_ADMIN_USER" ]] && echo "SUPERSET_ADMIN_USER not set" && exit 1
[[ -z "$SUPERSET_ADMIN_PASSWORD" ]] && echo "SUPERSET_ADMIN_PASSWORD not set" && exit 1

db_host=$SUPERSET_APPBASE_DB_HOST
db_port=$APPBASE_DB_PORT
db_name=$APPBASE_DB_NAME
db_user=$APPBASE_DB_USER
db_password=$APPBASE_DB_PASSWORD

superset_url=$SUPERSET_URL
superset_user=$SUPERSET_ADMIN_USER
superset_password=$SUPERSET_ADMIN_PASSWORD

# Function to check database connectivity
check_database_connectivity() {
    local host="$1"
    local port="$2"
    
    if command -v nc > /dev/null 2>&1; then
        if nc -z "$host" "$port" 2>/dev/null; then
            return 0
        else
            return 1
        fi
    else
        echo "Warning: nc (netcat) not found. Cannot test database connectivity."
        return 0
    fi
}

# Function to test Superset database connection via API
test_superset_connection() {
    local url="$1"
    local token="$2"
    local db_config="$3"
    local max_retries=3
    local retry_count=0
    local retry_delays=(2 5 10)
    
    while [ $retry_count -lt $max_retries ]; do
        local response=$(curl -s -L -w "\n%{http_code}" \
            -X POST \
            -H "Authorization: Bearer $token" \
            -H "Content-Type: application/json" \
            -d "$db_config" \
            "$url/api/v1/database/test_connection/")
    
        # Extract HTTP status code (last line)
        local http_code=$(echo "$response" | tail -n1)
        # Extract response body (all but last line)
        local response_body=$(echo "$response" | sed '$d')
        
        # Check if response is valid JSON
        if ! echo "$response_body" | jq . > /dev/null 2>&1; then
            echo "Warning: Superset API returned non-JSON response (HTTP $http_code)"
            # Truncate long HTML responses for readability
            if [ ${#response_body} -gt 500 ]; then
                echo "Response (truncated): ${response_body:0:500}..."
            else
                echo "Response: $response_body"
            fi
            
            # Check for permission system errors in HTML response
            if echo "$response_body" | grep -qi "ResourceClosedError\|permission\|Internal server error"; then
                echo "Detected permission system error, will retry..."
                if [ $retry_count -lt $((max_retries - 1)) ]; then
                    local delay=${retry_delays[$retry_count]}
                    echo "Retrying in $delay seconds... (attempt $((retry_count + 1))/$max_retries)"
                    sleep $delay
                    retry_count=$((retry_count + 1))
                    continue
                fi
            fi
            
            # If HTTP code is 200, assume connection is OK even if response format is unexpected
            if [ "$http_code" = "200" ]; then
                echo "HTTP 200 received, assuming connection test passed"
                return 0
            fi
            
            # Retry on 500 errors
            if [ "$http_code" = "500" ] && [ $retry_count -lt $((max_retries - 1)) ]; then
                local delay=${retry_delays[$retry_count]}
                echo "HTTP 500 error detected, retrying in $delay seconds... (attempt $((retry_count + 1))/$max_retries)"
                sleep $delay
                retry_count=$((retry_count + 1))
                continue
            fi
            
            return 1
        fi
    
        # Parse JSON response
        local message=$(echo "$response_body" | jq -r '.message // empty' 2>/dev/null)
        local error=$(echo "$response_body" | jq -r '.errors // empty' 2>/dev/null)
        
        # Check HTTP status code
        if [ "$http_code" != "200" ]; then
            echo "Database connection test failed with HTTP $http_code"
            if [[ -n "$error" && "$error" != "null" ]]; then
                echo "Error details: $error"
            fi
            if [[ -n "$message" && "$message" != "null" && "$message" != "" ]]; then
                echo "Message: $message"
            fi
            
            # Retry on 500 errors
            if [ "$http_code" = "500" ] && [ $retry_count -lt $((max_retries - 1)) ]; then
                local delay=${retry_delays[$retry_count]}
                echo "Retrying in $delay seconds... (attempt $((retry_count + 1))/$max_retries)"
                sleep $delay
                retry_count=$((retry_count + 1))
                continue
            fi
            
            return 1
        fi
        
        # Check for errors in response
        if [[ -n "$error" && "$error" != "null" && "$error" != "" ]]; then
            echo "Database connection test failed: $error"
            return 1
        fi
        
        # Check for success message
        if [[ "$message" == "OK" || "$message" == "Connection tested successfully" ]]; then
            return 0
        fi
        
        # If we get here with HTTP 200 and no error, assume success
        if [ "$http_code" = "200" ]; then
            echo "Database connection test returned: ${message:-'Success (no message)'}"
            return 0
        fi
        
        return 1
    done
    
    echo "Database connection test failed after $max_retries attempts"
    return 1
}

# Function to check if Superset is ready
check_superset_health() {
    local url="$1"
    local max_attempts=30
    local attempt=1
    
    echo "Checking Superset health at $url"
    
    while [ $attempt -le $max_attempts ]; do
        if curl -f -s "$url/health" > /dev/null 2>&1; then
            echo "Superset health endpoint is responding"
            
            # Additional check: verify API endpoint is ready (test login endpoint)
            echo "Verifying Superset API readiness..."
            local api_test_response=$(curl -s -L -o /dev/null -w "%{http_code}" \
                -X POST \
                -H "Content-Type: application/json" \
                -d '{"username":"test","password":"test","provider":"db"}' \
                "$url/api/v1/security/login" 2>/dev/null || echo "000")
            
            # API should return 401 (unauthorized) or 400 (bad request) if it's working
            # 500 means permission system isn't ready yet
            if [ "$api_test_response" = "401" ] || [ "$api_test_response" = "400" ] || [ "$api_test_response" = "422" ]; then
                echo "Superset API is ready! (HTTP $api_test_response)"
                return 0
            elif [ "$api_test_response" = "500" ]; then
                echo "Superset API returned 500 (permission system may not be ready), waiting..."
            else
                echo "Superset API test returned HTTP $api_test_response, waiting..."
            fi
        fi
        
        echo "Attempt $attempt/$max_attempts: Superset not ready yet, waiting..."
        sleep 10
        ((attempt++))
    done
    
    echo "Superset failed to become ready after $max_attempts attempts"
    return 1
}

# Function to authenticate and get access token
get_access_token() {
    local url="$1"
    local username="$2"
    local password="$3"
    local max_retries=3
    local retry_count=0
    local retry_delays=(2 5 10)
    
    while [ $retry_count -lt $max_retries ]; do
        local full_response=$(curl -s -L -w "\n%{http_code}" \
            -X POST \
            -H "Content-Type: application/json" \
            -d "{\"username\": \"$username\", \"password\": \"$password\", \"provider\": \"db\", \"refresh\": true}" \
            "$url/api/v1/security/login")
        
        # Extract HTTP status code (last line)
        local http_code=$(echo "$full_response" | tail -n1)
        # Extract response body (all but last line)
        local response=$(echo "$full_response" | sed '$d')
        
        # Check HTTP status code first
        if [ "$http_code" = "200" ]; then
            local access_token=$(echo "$response" | jq -r '.access_token // empty' 2>/dev/null)
            
            if [[ -n "$access_token" && "$access_token" != "null" ]]; then
                echo "$access_token"
                return 0
            fi
        elif [ "$http_code" = "401" ] || [ "$http_code" = "400" ] || [ "$http_code" = "422" ]; then
            # Authentication failed with valid credentials error - don't retry
            echo "Authentication failed: Invalid credentials (HTTP $http_code)"
            echo "Response: $response"
            exit 1
        elif [ "$http_code" = "500" ]; then
            # Server error - retry (likely database connection issue)
            if [ $retry_count -lt $((max_retries - 1)) ]; then
                local delay=${retry_delays[$retry_count]}
                echo "Authentication failed with HTTP 500 (database connection issue), retrying in $delay seconds... (attempt $((retry_count + 1))/$max_retries)"
                sleep $delay
                retry_count=$((retry_count + 1))
                continue
            fi
        fi
        
        # Check if response indicates permission system error (even if HTTP code is not 500)
        if echo "$response" | grep -qi "ResourceClosedError\|Internal server error\|DatabaseError" || \
           ! echo "$response" | jq . > /dev/null 2>&1; then
            if [ $retry_count -lt $((max_retries - 1)) ]; then
                local delay=${retry_delays[$retry_count]}
                echo "Authentication failed (permission system may not be ready), retrying in $delay seconds... (attempt $((retry_count + 1))/$max_retries)"
                sleep $delay
                retry_count=$((retry_count + 1))
                continue
            fi
        fi
        
        echo "Failed to authenticate with Superset API (HTTP $http_code)"
        echo "Response: $response"
        if [ $retry_count -lt $((max_retries - 1)) ]; then
            local delay=${retry_delays[$retry_count]}
            echo "Retrying in $delay seconds... (attempt $((retry_count + 1))/$max_retries)"
            sleep $delay
            retry_count=$((retry_count + 1))
            continue
        fi
        
        exit 1
    done
    
    echo "Failed to authenticate after $max_retries attempts"
    exit 1
}

# Function to check if database exists
database_exists() {
    local url="$1"
    local token="$2"
    local db_name="$3"
    
    local response=$(curl -s -L \
        -H "Authorization: Bearer $token" \
        -H "Content-Type: application/json" \
        "$url/api/v1/database/")
    
    local db_exists=$(echo "$response" | jq -r ".result[] | select(.database_name == \"$db_name\") | .database_name")
    
    if [[ "$db_exists" == "$db_name" ]]; then
        return 0
    else
        return 1
    fi
}

# Function to create database connection
create_database() {
    local url="$1"
    local token="$2"
    local db_config="$3"
    local max_retries=3
    local retry_count=0
    local retry_delays=(2 5 10)
    
    # Extract connection details for logging (mask password)
    local masked_uri=$(echo "$db_config" | jq -r '.sqlalchemy_uri' | sed 's/:[^:@]*@/:****@/')
    echo "Creating database connection with URI: $masked_uri"
    
    while [ $retry_count -lt $max_retries ]; do
        local response=$(curl -s -L -w "\n%{http_code}" \
            -X POST \
            -H "Authorization: Bearer $token" \
            -H "Content-Type: application/json" \
            -d "$db_config" \
            "$url/api/v1/database/")
    
        # Extract HTTP status code (last line)
        local http_code=$(echo "$response" | tail -n1)
        # Extract response body (all but last line)
        local response_body=$(echo "$response" | sed '$d')
        
        # Check if response is valid JSON
        if ! echo "$response_body" | jq . > /dev/null 2>&1; then
            echo "Error: Superset API returned non-JSON response (HTTP $http_code)"
            # Truncate long HTML responses for readability
            if [ ${#response_body} -gt 500 ]; then
                echo "Response (truncated): ${response_body:0:500}..."
            else
                echo "Response: $response_body"
            fi
            
            # Check for permission system errors
            if echo "$response_body" | grep -qi "ResourceClosedError\|permission\|Internal server error"; then
                if [ $retry_count -lt $((max_retries - 1)) ]; then
                    local delay=${retry_delays[$retry_count]}
                    echo "Detected permission system error, retrying in $delay seconds... (attempt $((retry_count + 1))/$max_retries)"
                    sleep $delay
                    retry_count=$((retry_count + 1))
                    continue
                fi
            fi
            
            if [ "$http_code" = "200" ] || [ "$http_code" = "201" ]; then
                echo "HTTP $http_code received, assuming database connection created successfully"
                return 0
            fi
            
            # Retry on 500 errors
            if [ "$http_code" = "500" ] && [ $retry_count -lt $((max_retries - 1)) ]; then
                local delay=${retry_delays[$retry_count]}
                echo "HTTP 500 error detected, retrying in $delay seconds... (attempt $((retry_count + 1))/$max_retries)"
                sleep $delay
                retry_count=$((retry_count + 1))
                continue
            fi
            
            echo "Failed to create database connection (HTTP $http_code)"
            exit 1
        fi
    
        # Parse JSON response
        local error=$(echo "$response_body" | jq -r '.message // empty' 2>/dev/null)
        local errors=$(echo "$response_body" | jq -r '.errors // empty' 2>/dev/null)
        local result_id=$(echo "$response_body" | jq -r '.result.id // empty' 2>/dev/null)
        
        # Check HTTP status code first
        if [ "$http_code" != "200" ] && [ "$http_code" != "201" ]; then
            echo "Failed to create database connection (HTTP $http_code)"
            if [[ -n "$error" && "$error" != "null" && "$error" != "" ]]; then
                echo "Error message: $error"
            fi
            if [[ -n "$errors" && "$errors" != "null" && "$errors" != "" ]]; then
                echo "Errors: $errors"
            fi
            
            # Retry on 500 errors
            if [ "$http_code" = "500" ] && [ $retry_count -lt $((max_retries - 1)) ]; then
                local delay=${retry_delays[$retry_count]}
                echo "Retrying in $delay seconds... (attempt $((retry_count + 1))/$max_retries)"
                sleep $delay
                retry_count=$((retry_count + 1))
                continue
            fi
            
            echo "Full response: $response_body"
            exit 1
        fi
    
        # Check for error messages in response (even with HTTP 200/201)
        if [[ -n "$error" && "$error" != "null" && "$error" != "OK" && "$error" != "" ]]; then
            # Some Superset versions return error messages even with HTTP 200
            if [[ "$error" == *"already exists"* ]] || [[ "$error" == *"duplicate"* ]]; then
                echo "Database connection already exists (or duplicate): $error"
                return 0
            fi
            echo "Failed to create database connection: $error"
            echo "Connection URI (masked): $masked_uri"
            if [[ -n "$errors" && "$errors" != "null" ]]; then
                echo "Errors: $errors"
            fi
            echo "Full response: $response_body"
            exit 1
        fi
        
        # Check for errors array
        if [[ -n "$errors" && "$errors" != "null" && "$errors" != "" ]]; then
            echo "Failed to create database connection. Errors: $errors"
            echo "Connection URI (masked): $masked_uri"
            echo "Full response: $response_body"
            exit 1
        fi
        
        # Success - check if we got a result ID or just HTTP success
        if [[ -n "$result_id" && "$result_id" != "null" && "$result_id" != "" ]]; then
            echo "Database connection created successfully (ID: $result_id)"
        else
            echo "Database connection created successfully (HTTP $http_code)"
        fi
        return 0
    done
    
    echo "Failed to create database connection after $max_retries attempts"
    exit 1
}

# Main execution
echo "Starting Superset initialization..."

# Validate required variables
if [[ -z "$db_host" || -z "$db_port" || -z "$db_name" || -z "$db_user" || -z "$db_password" ]]; then
    echo "Error: Missing required database configuration variables"
    echo "Required: SUPERSET_APPBASE_DB_HOST, APPBASE_DB_PORT, APPBASE_DB_NAME, APPBASE_DB_USER, APPBASE_DB_PASSWORD"
    exit 1
fi

# Validate db_port is a number
if ! [[ "$db_port" =~ ^[0-9]+$ ]]; then
    echo "Error: APPBASE_DB_PORT must be a valid number, got: $db_port"
    exit 1
fi

# Wait for external database to be reachable (if not local DB)
if [ "$db_host" != "db" ]; then
    echo "External database detected: $db_host:$db_port"
    echo "Waiting for external database to be reachable..."
    max_attempts=30
    attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if check_database_connectivity "$db_host" "$db_port"; then
            echo "External database is reachable!"
            break
        fi
        
        echo "Attempt $attempt/$max_attempts: Database not reachable at $db_host:$db_port, waiting..."
        sleep 2
        attempt=$((attempt + 1))
    done
    
    if [ $attempt -gt $max_attempts ]; then
        echo "Error: External database at $db_host:$db_port is not reachable after $max_attempts attempts"
        echo "Please check:"
        echo "  1. Database hostname is correct and resolvable"
        echo "  2. Database port $db_port is open and accessible"
        echo "  3. Network connectivity from appbase-init container to database"
        exit 1
    fi
else
    echo "Local database detected (db service)"
fi

# Check if Superset is running and healthy
if ! check_superset_health "$superset_url"; then
    echo "Superset instance is not ready: $superset_url/health"
    exit 1
fi

# Add delay after health check to allow Superset permission system to stabilize
echo "Waiting 10 seconds for Superset permission system to fully initialize..."
sleep 10

# Get access token
echo "Authenticating with Superset..."
access_token=$(get_access_token "$superset_url" "$superset_user" "$superset_password")

# Check if Platform database already exists
if database_exists "$superset_url" "$access_token" "Platform Data"; then
    echo "Platform database connection already exists in Superset"
else
    echo "Creating Platform database connection in Superset..."
    
    # Create database configuration JSON
    db_config=$(jq -n \
        --arg db_host "$db_host" \
        --arg db_port "$db_port" \
        --arg db_name "$db_name" \
        --arg db_user "$db_user" \
        --arg db_password "$db_password" \
        '{
            "database_name": "Platform Data",
            "sqlalchemy_uri": ("postgresql://" + $db_user + ":" + $db_password + "@" + $db_host + ":" + $db_port + "/" + $db_name),
            "engine": "postgresql",
            "expose_in_sqllab": true,
            "allow_run_async": false,
            "allow_ctas": true,
            "allow_cvas": true,
            "allow_dml": true,
            "allow_file_upload": false
        }')
    
    # Test database connection via Superset API before creating (optional)
    # Note: This test may fail due to API differences, but we'll proceed with creation anyway
    echo "Testing database connection via Superset API..."
    if test_superset_connection "$superset_url" "$access_token" "$db_config"; then
        echo "Database connection test passed!"
    else
        echo "Warning: Database connection test failed or returned unexpected response"
        echo "Proceeding with database connection creation anyway..."
        echo "If creation fails, please verify:"
        echo "  1. Database credentials are correct"
        echo "  2. Database '$db_name' exists"
        echo "  3. User '$db_user' has proper permissions"
        echo "  4. Superset container can reach database at $db_host:$db_port"
    fi
    
    # Create the database connection
    create_database "$superset_url" "$access_token" "$db_config"
fi

echo "Superset Platform database setup completed successfully"

echo "Syncing database schema"
node ../lib/superset/init.js --superset-url "$superset_url" --username "$superset_user" --password "$superset_password" --database "$db_name" --sync-schema

echo "Superset initialization completed successfully" 