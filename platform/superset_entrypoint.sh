#!/bin/bash
set -eo pipefail

echo "🚀 Starting Superset initialization..."

# Wait for the database to be ready (only for local DB)
if [ "${APPBASE_CONFIG_DB_HOST}" = "db" ]; then
    echo "⏳ Waiting for database to be ready..."
    while ! nc -z db 5432; do
        echo "Database not ready, waiting..."
        sleep 2
    done
    echo "✅ Database is ready!"
else
    echo "Using external database (${APPBASE_CONFIG_DB_HOST}), skipping wait..."
fi

# Initialize Superset database
echo "🔧 Initializing Superset database schema..."
superset db upgrade

# Create admin user (only if it doesn't exist)
echo "👤 Creating Superset admin user..."
superset fab create-admin \
    --username "$SUPERSET_ADMIN_USER" \
    --firstname Admin \
    --lastname User \
    --email "$SUPERSET_ADMIN_EMAIL" \
    --password "$SUPERSET_ADMIN_PASSWORD" || true

# Initialize Superset (roles, permissions, etc.)
echo "🔐 Initializing Superset roles and permissions..."
# Check if roles already exist (quick check to see if init is needed)
# This helps avoid unnecessary long-running init operations
ROLES_EXIST=false
if command -v psql > /dev/null 2>&1; then
    DB_URI="${SUPERSET_DATABASE_URI}"
    if [ -n "$DB_URI" ]; then
        ROLE_COUNT=$(psql "$DB_URI" -t -c "SELECT COUNT(*) FROM ab_role;" 2>/dev/null | tr -d ' ' || echo "0")
        if [ "$ROLE_COUNT" -gt "0" ] 2>/dev/null; then
            ROLES_EXIST=true
            echo "ℹ️  Roles already exist ($ROLE_COUNT roles found), running init in background to sync..."
        else
            echo "ℹ️  No roles found, running init to create roles and permissions..."
        fi
    fi
fi

# Run init in background to prevent blocking webserver startup
# This is safe because init can complete while webserver is running
# The init process syncs roles and permissions which can take time, especially with external databases
echo "Running superset init in background (this may take a while)..."
(
    superset init 2>&1 | while IFS= read -r line; do 
        echo "[init] $line"
    done
    INIT_EXIT_CODE=$?
    if [ $INIT_EXIT_CODE -eq 0 ]; then
        echo "✅ Superset init completed successfully"
    else
        echo "⚠️  Superset init exited with code $INIT_EXIT_CODE, but continuing..."
    fi
) &
INIT_PID=$!

echo "✅ Core Superset initialization complete!"
echo "ℹ️  Roles and permissions initialization running in background (PID: $INIT_PID)"
echo "ℹ️  Webserver will start immediately - init will complete in background"

# Start the Superset webserver immediately
# The init process will continue in background
echo "🌐 Starting Superset webserver with Gunicorn (production-ready)..."
# Use Gunicorn for production WSGI server (replaces development server)
# Configuration:
#   - bind: Listen on all interfaces on port 8088
#   - workers: Number of worker processes (configurable via SUPERSET_GUNICORN_WORKERS, default: 2)
#   - threads: 2 threads per worker for handling concurrent requests
#   - timeout: 120 seconds (recommended for Superset queries)
#   - access-logfile: Log access requests to stdout
#   - error-logfile: Log errors to stderr
#   - log-level: info level logging
#   - limit-request-line: Maximum size of HTTP request line (0 = unlimited)
#   - limit-request-field_size: Maximum size of HTTP request header field (0 = unlimited)
#   - preload: Load application code before forking workers (faster startup, lower memory)
#   - max-requests: Restart workers after this many requests (prevents memory leaks)
#   - max-requests-jitter: Random jitter to prevent all workers restarting simultaneously

# Calculate worker count (default: 2, can be overridden via environment variable)
WORKERS=${SUPERSET_GUNICORN_WORKERS:-2}
echo "📊 Gunicorn configuration: ${WORKERS} workers, 2 threads per worker"

exec gunicorn \
    --bind 0.0.0.0:8088 \
    --workers "${WORKERS}" \
    --threads 2 \
    --timeout 120 \
    --access-logfile - \
    --error-logfile - \
    --log-level info \
    --limit-request-line 0 \
    --limit-request-field_size 0 \
    --preload \
    --max-requests 1000 \
    --max-requests-jitter 50 \
    "superset.app:create_app()" 
