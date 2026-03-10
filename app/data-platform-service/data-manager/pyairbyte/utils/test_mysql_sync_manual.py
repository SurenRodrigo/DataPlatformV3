#!/usr/bin/env python3
"""
Manual test script for MySQL to PostgreSQL sync utility.

This script tests the mysql_sync utility with real MySQL and PostgreSQL connections.

Usage:
    python test_mysql_sync_manual.py

Environment Variables (from .env):
    PYAIRBYTE_CACHE_DB_HOST=db
    PYAIRBYTE_CACHE_DB_PORT=5432
    PYAIRBYTE_CACHE_DB_USER=dataplatuser
    PYAIRBYTE_CACHE_DB_PASSWORD=dataplatpassword
    PYAIRBYTE_CACHE_DB_NAME=dataplatform
"""

import os
import sys
import psycopg2
import logging
from pathlib import Path
from typing import List

# Add the data-manager path to sys.path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyairbyte.utils.mysql_sync import sync_mysql_tables
from airbyte.caches import PostgresCache

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.StreamHandler(sys.stderr)
    ],
    force=True
)
logger = logging.getLogger(__name__)


def print_info(msg):
    print(f"INFO: {msg}", flush=True)
    logger.info(msg)


def print_success(msg):
    print(f"✓ {msg}", flush=True)
    logger.info(f"✓ {msg}")


def print_error(msg):
    print(f"✗ {msg}", flush=True)
    logger.error(f"✗ {msg}")


def print_step(msg):
    print(f"\n{'='*80}", flush=True)
    print(f"{msg}", flush=True)
    print(f"{'='*80}", flush=True)
    logger.info(msg)


def create_postgres_cache():
    cache = PostgresCache(
        host=os.getenv('PYAIRBYTE_CACHE_DB_HOST', 'db'),
        port=int(os.getenv('PYAIRBYTE_CACHE_DB_PORT', '5432')),
        database=os.getenv('PYAIRBYTE_CACHE_DB_NAME', 'dataplatform'),
        username=os.getenv('PYAIRBYTE_CACHE_DB_USER', 'dataplatuser'),
        password=os.getenv('PYAIRBYTE_CACHE_DB_PASSWORD', 'dataplatpassword'),
        schema_name='pyairbyte_cache',
        table_prefix='mysql_test_',
        cleanup=True
    )
    return cache


def cleanup_test_tables(table_names: List[str]) -> bool:
    try:
        print_info("Connecting to PostgreSQL to cleanup any prior test tables...")
        pg_conn = psycopg2.connect(
            host=os.getenv('PYAIRBYTE_CACHE_DB_HOST', 'db'),
            port=int(os.getenv('PYAIRBYTE_CACHE_DB_PORT', '5432')),
            database=os.getenv('PYAIRBYTE_CACHE_DB_NAME', 'dataplatform'),
            user=os.getenv('PYAIRBYTE_CACHE_DB_USER', 'dataplatuser'),
            password=os.getenv('PYAIRBYTE_CACHE_DB_PASSWORD', 'dataplatpassword')
        )

        cursor = pg_conn.cursor()

        print_info(f"Ensuring schema exists: pyairbyte_cache")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS pyairbyte_cache")

        for table_name in table_names:
            safe_table_name = f"mysql_test_{table_name}".replace('-', '_').replace(' ', '_').lower()
            print_info(f"Dropping table if exists: pyairbyte_cache.{safe_table_name}")
            cursor.execute(f'DROP TABLE IF EXISTS pyairbyte_cache."{safe_table_name}"')

        pg_conn.commit()
        cursor.close()
        pg_conn.close()
        print_success(f"Cleanup completed (dropped {len(table_names)} previous test table(s) if they existed)")
        return True

    except Exception as e:
        print_error(f"Cleanup failed: {e}")
        return False


def verify_postgres_data(table_name: str):
    try:
        pg_conn = psycopg2.connect(
            host=os.getenv('PYAIRBYTE_CACHE_DB_HOST', 'db'),
            port=int(os.getenv('PYAIRBYTE_CACHE_DB_PORT', '5432')),
            database=os.getenv('PYAIRBYTE_CACHE_DB_NAME', 'dataplatform'),
            user=os.getenv('PYAIRBYTE_CACHE_DB_USER', 'dataplatuser'),
            password=os.getenv('PYAIRBYTE_CACHE_DB_PASSWORD', 'dataplatpassword')
        )

        cursor = pg_conn.cursor()

        logger.info("- Verifying that the synced table exists and contains data...")
        safe_table_name = f"mysql_test_{table_name}".replace('-', '_').replace(' ', '_').lower()
        cursor.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'pyairbyte_cache' 
                AND LOWER(table_name) = %s
            );
        """, (safe_table_name,))

        table_exists = cursor.fetchone()[0]
        if not table_exists:
            print_error(f"Table {safe_table_name} does not exist in PostgreSQL!")
            cursor.close()
            pg_conn.close()
            return False

        print_success(f"Table {safe_table_name} exists in PostgreSQL")

        cursor.execute(f'SELECT COUNT(*) FROM pyairbyte_cache."{safe_table_name}"')
        row_count = cursor.fetchone()[0]
        print_success(f"Row count: {row_count}")

        cursor.close()
        pg_conn.close()
        return True
    except Exception as e:
        print_error(f"Error verifying PostgreSQL data: {e}")
        return False


def main():
    print_step("MySQL to PostgreSQL Sync Utility - Manual Test")

    connector_name = 'jobylonbi-connector'
    table_names = ['job']

    print_info("\nMySQL Configuration:")
    print_info(f"  Connector: {connector_name}")
    print_info(f"  Config Source: PYAIRBYTE_CONNECTOR_CONFIGS environment variable")
    print_info(f"\nTables to sync: {table_names}")

    print_info("\nPostgreSQL Cache Configuration:")
    print_info(f"  Host: {os.getenv('PYAIRBYTE_CACHE_DB_HOST', 'db')}")
    print_info(f"  Port: {os.getenv('PYAIRBYTE_CACHE_DB_PORT', '5432')}")
    print_info(f"  Database: {os.getenv('PYAIRBYTE_CACHE_DB_NAME', 'dataplatform')}")
    print_info(f"  Schema: pyairbyte_cache")
    print_info(f"  Table prefix: mysql_test_")

    try:
        print_step("Step 1: Pre-test cleanup")
        cleaned = cleanup_test_tables(table_names)
        if not cleaned:
            print_info("⚠ Pre-test cleanup reported an issue; proceeding with test anyway")

        print_step("Step 2: Creating PostgresCache instance...")
        cache = create_postgres_cache()
        print_success("PostgresCache created successfully")
    except Exception as e:
        print_error(f"Failed to create PostgresCache: {e}")
        sys.exit(1)

    try:
        print_step("Step 3: Starting MySQL sync...")
        print_info("This step will:")
        print_info("  1. Connect to MySQL database (SSL enabled)")
        print_info("  2. Extract schema from source table")
        print_info("  3. Create PostgreSQL table with mapped types")
        print_info("  4. Extract and transform data from MySQL")
        print_info("  5. Bulk insert data into PostgreSQL")
        print_info("  6. Validate row counts match")

        result = sync_mysql_tables(
            connector_name=connector_name,
            table_names=table_names,
            cache=cache,
            batch_size=10000,
            table_prefix='mysql_test_'
        )

        print_step("Step 4: Sync Results")
        print_info(f"  Status: {result.get('status')}")
        print_info(f"  Connector: {result.get('connector')}")
        print_info(f"  Cache Schema: {result.get('cache_schema')}")

        if result.get('status') == 'error':
            print_error(f"  Error: {result.get('error')}")
            sys.exit(1)

        if 'result' in result:
            res = result['result']
            print_info(f"  Total Tables: {res.get('total_tables', 0)}")
            print_info(f"  Successful Tables: {res.get('successful_tables', 0)}")
            print_info(f"  Failed Tables: {res.get('failed_tables', 0)}")
            print_info(f"  Total Records: {res.get('total_records', 0)}")

            print_info(f"\n  Table Details:")
            for table_name, table_info in res.get('tables', {}).items():
                print_info(f"    {table_name}:")
                print_info(f"      Rows Synced: {table_info.get('rows_synced', 0)}")
                print_info(f"      Schema Synced: {table_info.get('schema_synced', False)}")
                if table_info.get('errors'):
                    print_error(f"      Errors: {table_info.get('errors')}")

    except Exception as e:
        print_error(f"Sync failed with exception: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    print_step("Step 5: Verifying PostgreSQL data...")
    for table_name in table_names:
        if verify_postgres_data(table_name):
            print_success(f"Verification passed for table: {table_name}")
        else:
            print_error(f"Verification failed for table: {table_name}")
            sys.exit(1)

    print_step("✓ Test completed successfully!")


if __name__ == '__main__':
    main()


