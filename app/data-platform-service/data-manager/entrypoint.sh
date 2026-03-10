#!/bin/bash

set -eo pipefail

echo "=================================================="
echo "Starting Data Platform Service (Dagster gRPC Servers)"
echo "=================================================="

# Change to app directory where dagster_code is mounted
cd /app

# Initialize PyAirbyte cache schema
echo "Initializing PyAirbyte cache schema..."
python3 /app/data-manager/scripts/init_cache_db.py

# Make dagster-init.sh executable
chmod +x /app/data-manager/scripts/dagster-init.sh

# Execute dagster-init.sh to start all gRPC servers
echo "Executing dagster-init.sh to start gRPC servers..."
/app/data-manager/scripts/dagster-init.sh /app/data-manager/resources/dagster/code-locations.json

# Keep the container running
echo "Data Platform Service is running..."
echo "gRPC servers are active and ready for Dagster platform connection"
echo "=================================================="

# Wait indefinitely to keep the container running
tail -f /dev/null 