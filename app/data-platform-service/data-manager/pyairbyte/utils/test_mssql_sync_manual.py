#!/usr/bin/env python3
"""
Manual test script for MSSQL to PostgreSQL sync utility.

This script tests the mssql_sync utility with real MSSQL and PostgreSQL connections.

Usage:
    python test_mssql_sync_manual.py

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

from pyairbyte.utils.mssql_sync import sync_mssql_tables
from airbyte.caches import PostgresCache

# Configure logging with both file handler and console handler for visibility
import sys
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

# Also use print for critical messages to ensure visibility
def print_info(msg):
    """Print info message to both logger and stdout for maximum visibility"""
    print(f"INFO: {msg}", flush=True)
    logger.info(msg)
    
def print_success(msg):
    """Print success message"""
    print(f"✓ {msg}", flush=True)
    logger.info(f"✓ {msg}")
    
def print_error(msg):
    """Print error message"""
    print(f"✗ {msg}", flush=True)
    logger.error(f"✗ {msg}")
    
def print_step(msg):
    """Print step message"""
    print(f"\n{'='*80}", flush=True)
    print(f"{msg}", flush=True)
    print(f"{'='*80}", flush=True)
    logger.info(msg)


def create_postgres_cache():
    """
    Create PostgresCache instance using environment variables from .env file.
    """
    cache = PostgresCache(
        host=os.getenv('PYAIRBYTE_CACHE_DB_HOST', 'db'),
        port=int(os.getenv('PYAIRBYTE_CACHE_DB_PORT', '5432')),
        database=os.getenv('PYAIRBYTE_CACHE_DB_NAME', 'dataplatform'),
        username=os.getenv('PYAIRBYTE_CACHE_DB_USER', 'dataplatuser'),
        password=os.getenv('PYAIRBYTE_CACHE_DB_PASSWORD', 'dataplatpassword'),
        schema_name='pyairbyte_cache',
        table_prefix='mssql_test_',
        cleanup=True
    )
    return cache


def cleanup_test_tables(table_names: List[str]) -> bool:
    """
    Drop test tables in PostgreSQL cache if they exist before running the sync.
    
    Args:
        table_names: List of table names to clean up
    """
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

        # Drop each table
        for table_name in table_names:
            safe_table_name = f"mssql_test_{table_name}".replace('-', '_').replace(' ', '_').lower()
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
    """
    Verify that data was written to PostgreSQL correctly.
    
    Args:
        table_name: Name of the table to verify
    """
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
        # Check if table exists (PostgreSQL converts unquoted identifiers to lowercase)
        safe_table_name = f"mssql_test_{table_name}".replace('-', '_').replace(' ', '_').lower()
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
        
        # Get row count (table name needs to be lowercase or quoted)
        cursor.execute(f'SELECT COUNT(*) FROM pyairbyte_cache."{safe_table_name}"')
        row_count = cursor.fetchone()[0]
        print_success(f"Row count: {row_count}")
        
        # Get column information
        cursor.execute(f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'pyairbyte_cache'
            AND LOWER(table_name) = %s
            ORDER BY ordinal_position
        """, (safe_table_name,))
        
        columns = cursor.fetchall()
        print_success(f"Table has {len(columns)} columns:")
        for col_name, col_type, is_nullable in columns[:10]:  # Show first 10 columns
            print_info(f"  - {col_name}: {col_type} ({'NULL' if is_nullable == 'YES' else 'NOT NULL'})")
        if len(columns) > 10:
            print_info(f"  ... and {len(columns) - 10} more columns")
        
        # Sample a few rows if data exists
        if row_count > 0:
            cursor.execute(f'SELECT * FROM pyairbyte_cache."{safe_table_name}" LIMIT 3')
            sample_rows = cursor.fetchall()
            print_success(f"Sample data (first {len(sample_rows)} rows):")
            for i, row in enumerate(sample_rows, 1):
                # Truncate long rows for display
                row_str = str(row)
                if len(row_str) > 200:
                    row_str = row_str[:200] + "..."
                print_info(f"  Row {i}: {row_str}")
        else:
            print_info(f"⚠ Table {safe_table_name} is empty!")
        
        cursor.close()
        pg_conn.close()
        
        return True
        
    except Exception as e:
        print_error(f"Error verifying PostgreSQL data: {e}")
        return False


def main():
    """Main test function."""
    print_step("MSSQL to PostgreSQL Sync Utility - Manual Test")
    
    # Connector name (must match entry in PYAIRBYTE_CONNECTOR_CONFIGS)
    connector_name = 'admmit-connector'
    
    # Tables to sync
    table_names = ['Vehicle', 'OrderItemVehicleType']
    
    print_info("\nMSSQL Configuration:")
    print_info(f"  Connector: {connector_name}")
    print_info(f"  Config Source: PYAIRBYTE_CONNECTOR_CONFIGS environment variable")
    print_info(f"\nTables to sync: {table_names}")
    
    # PostgreSQL Cache Configuration
    print_info("\nPostgreSQL Cache Configuration:")
    print_info(f"  Host: {os.getenv('PYAIRBYTE_CACHE_DB_HOST', 'db')}")
    print_info(f"  Port: {os.getenv('PYAIRBYTE_CACHE_DB_PORT', '5432')}")
    print_info(f"  Database: {os.getenv('PYAIRBYTE_CACHE_DB_NAME', 'dataplatform')}")
    print_info(f"  Schema: pyairbyte_cache")
    print_info(f"  Table prefix: mssql_test_")
    
    # Create PostgresCache
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
    
    # Test MSSQL sync
    try:
        print_step("Step 3: Starting MSSQL sync...")
        print_info("This step will:")
        print_info("  1. Connect to MSSQL database")
        print_info("  2. Extract schema from source table")
        print_info("  3. Create PostgreSQL table with mapped types")
        print_info("  4. Extract and transform data from MSSQL")
        print_info("  5. Bulk insert data into PostgreSQL")
        print_info("  6. Validate row counts match")
        
        # Use connector_name approach (consistent with production)
        # The connector 'admmit-connector' should be configured in PYAIRBYTE_CONNECTOR_CONFIGS
        connector_name = 'admmit-connector'
        
        result = sync_mssql_tables(
            connector_name=connector_name,
            table_names=table_names,
            cache=cache,
            batch_size=10000,
            table_prefix='mssql_test_'  # Explicitly pass table prefix parameter
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
    
    # Verify PostgreSQL data
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

