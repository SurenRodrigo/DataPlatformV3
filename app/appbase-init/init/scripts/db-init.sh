#!/bin/bash

# ============================================================================
# Database Initialization Script
# ============================================================================
# This script initializes and verifies service databases based on service.yaml.
# 
# IMPORTANT: service.yaml is the SINGLE SOURCE OF TRUTH for which service
# databases should be verified. Environment variables are only used for
# connection details (host, port, credentials, database names), NOT for
# determining which services to verify.
#
# Service database verification logic:
# - All service enablement decisions come from service.yaml
# - Only databases for services enabled in service.yaml are verified
# - Future services can be added by extending service.yaml and this script
# ============================================================================

set -eo pipefail

echo "DEBUG: APPBASE_DB_NAME=$APPBASE_DB_NAME"
echo "DEBUG: APPBASE_DB_HOST=$APPBASE_DB_HOST"
echo "DEBUG: APPBASE_DB_PORT=$APPBASE_DB_PORT"
echo "DEBUG: APPBASE_DB_USER=$APPBASE_DB_USER"
echo "DEBUG: APPBASE_DB_PASSWORD=$APPBASE_DB_PASSWORD"
echo "DEBUG: APPBASE_CONFIG_DB_NAME=$APPBASE_CONFIG_DB_NAME"
echo "DEBUG: APPBASE_CONFIG_DB_HOST=$APPBASE_CONFIG_DB_HOST"
echo "DEBUG: APPBASE_CONFIG_DB_PORT=$APPBASE_CONFIG_DB_PORT"
echo "DEBUG: APPBASE_CONFIG_DB_USER=$APPBASE_CONFIG_DB_USER"
echo "DEBUG: APPBASE_CONFIG_DB_PASSWORD=$APPBASE_CONFIG_DB_PASSWORD"
echo "DEBUG: HASURA_DB_NAME=$HASURA_DB_NAME"
echo "DEBUG: METABASE_DB_NAME=$METABASE_DB_NAME"
echo "DEBUG: SUPERSET_DB_NAME=$SUPERSET_DB_NAME"

faros_db_name=${APPBASE_DB_NAME}
faros_db_host=${APPBASE_DB_HOST}
faros_db_port=${APPBASE_DB_PORT}
faros_db_user=${APPBASE_DB_USER}
faros_db_pass=${APPBASE_DB_PASSWORD}

cfg_db_host=${APPBASE_CONFIG_DB_HOST}
cfg_db_port=${APPBASE_CONFIG_DB_PORT}
cfg_db_user=${APPBASE_CONFIG_DB_USER}
cfg_db_pass=${APPBASE_CONFIG_DB_PASSWORD}

hasura_db_name=${HASURA_DB_NAME}
metabase_db_name=${METABASE_DB_NAME}
superset_db_name=${SUPERSET_DB_NAME}
run_db_migrations=${RUN_DB_MIGRATIONS}

# Function to get service state from service.yaml
get_service_state() {
    local service_path="$1"
    node parse-service-yaml.js "$service_path" 2>/dev/null || echo "false"
}

# aribyte_cash_db_name_sweden=${PYAIRBYTE_CACHE_SWEDEN_DB_NAME}
# aribyte_cash_db_host_sweden=${PYAIRBYTE_CACHE_SWEDEN_DB_HOST}
# aribyte_cash_db_port_sweden=${PYAIRBYTE_CACHE_SWEDEN_DB_PORT}
# aribyte_cash_db_user_sweden=${PYAIRBYTE_CACHE_SWEDEN_DB_USER}
# aribyte_cash_db_pass_sweden=${PYAIRBYTE_CACHE_SWEDEN_DB_PASSWORD}

create_database() {
  db_name=$1
  db_host=$2
  db_port=$3
  db_user=$4
  db_pass=$5
  db_url="postgres://$db_user:$db_pass@$db_host:$db_port/postgres"

  if [ "$( psql "$db_url" -tAc "SELECT 1 FROM pg_database WHERE datname='$db_name'" )" = '1' ]
  then
      echo "Database $db_name already exists"
  else
      echo "Creating database $db_name"
      PGPASSWORD=$db_pass createdb -h "$db_host" -U "$db_user" -p "$db_port" "$db_name"
  fi
}

create_database "$faros_db_name" "$faros_db_host" "$faros_db_port" "$faros_db_user" "$faros_db_pass"

# create_database "$aribyte_cash_db_name_sweden" "$aribyte_cash_db_host_sweden" "$aribyte_cash_db_port_sweden" "$aribyte_cash_db_user_sweden" "$aribyte_cash_db_pass_sweden"
# Check if run_db_migrations is true and only run below if it is
if [ "$run_db_migrations" = "true" ]
then
  echo "Applying Flyway migrations"
  flyway_args=(
    -locations="filesystem:/home/flyway/appbase/appbase-schemas"
    -url="jdbc:postgresql://$faros_db_host:$faros_db_port/$faros_db_name"
    -user="$faros_db_user"
    -password="$faros_db_pass"
  )
  set +e
  migrate_output=$(flyway "${flyway_args[@]}" migrate 2>&1)
  migrate_exit=$?
  set -e
  if [ "$migrate_exit" -ne 0 ]; then
    echo "$migrate_output"
    if echo "$migrate_output" | grep -q "checksum mismatch\|Validate failed"; then
      echo "Flyway validation failed (checksum mismatch). Running flyway repair to align schema history with current migrations, then retrying migrate."
      flyway "${flyway_args[@]}" repair
      flyway "${flyway_args[@]}" migrate
    else
      exit $migrate_exit
    fi
  fi
else
  echo "RUN_DB_MIGRATIONS is set to Skip Flyway migrations"
fi


# Verify service databases exist (created by platform initialization)
# IMPORTANT: Only verify databases for services enabled in service.yaml (SINGLE SOURCE OF TRUTH)
echo "Verifying service databases exist (based on service.yaml configuration)..."

# Define db_url for verification (using config database connection)
db_url="postgres://$cfg_db_user:$cfg_db_pass@$cfg_db_host:$cfg_db_port/postgres"

# Check Hasura database if Hasura is enabled in service.yaml (SINGLE SOURCE OF TRUTH)
if [ "$(get_service_state platform.hasura)" = "true" ]; then
    if [ "$( psql "$db_url" -tAc "SELECT 1 FROM pg_database WHERE datname='$hasura_db_name'" )" != '1' ]; then
        echo "ERROR: Hasura database '$hasura_db_name' does not exist"
        echo "       Hasura is enabled in service.yaml but database was not created by platform initialization"
        exit 1
    fi
    echo "Hasura database '$hasura_db_name' verified (enabled in service.yaml)"
else
    echo "Hasura service disabled in service.yaml, skipping database verification"
fi

# Check Metabase database if Metabase is enabled in service.yaml (SINGLE SOURCE OF TRUTH)
if [ "$(get_service_state platform.metabase)" = "true" ]; then
    if [ "$( psql "$db_url" -tAc "SELECT 1 FROM pg_database WHERE datname='$metabase_db_name'" )" != '1' ]; then
        echo "ERROR: Metabase database '$metabase_db_name' does not exist"
        echo "       Metabase is enabled in service.yaml but database was not created by platform initialization"
        exit 1
    fi
    echo "Metabase database '$metabase_db_name' verified (enabled in service.yaml)"
else
    echo "Metabase service disabled in service.yaml, skipping database verification"
fi

# Check Superset database if Superset is enabled in service.yaml (SINGLE SOURCE OF TRUTH)
if [ "$(get_service_state platform.superset)" = "true" ]; then
    if [ "$( psql "$db_url" -tAc "SELECT 1 FROM pg_database WHERE datname='$superset_db_name'" )" != '1' ]; then
        echo "ERROR: Superset database '$superset_db_name' does not exist"
        echo "       Superset is enabled in service.yaml but database was not created by platform initialization"
        exit 1
    fi
    echo "Superset database '$superset_db_name' verified (enabled in service.yaml)"
else
    echo "Superset service disabled in service.yaml, skipping database verification"
fi

echo "All required service databases verified successfully"
