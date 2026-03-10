#!/usr/bin/env python3
"""
PyAirbyte Cache Database Initialization Script

This script initializes the PyAirbyte cache database and creates schemas
for all available connectors found in the external-connectors directory.
"""

import os
import sys
import time

# Add the pyairbyte utils to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '../pyairbyte/utils'))

from cache_db_manager import PyAirbyteCacheDBManager
from connector_loader import load_all_connectors


def wait_for_database(max_retries=30, retry_interval=2):
    """Wait for the database to be available."""
    print("Waiting for database to be available...")
    
    for attempt in range(max_retries):
        try:
            manager = PyAirbyteCacheDBManager()
            # Try to connect to the main database
            conn = manager.get_connection()
            conn.close()
            print("Database is available!")
            return True
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Database not ready (attempt {attempt + 1}/{max_retries}): {e}")
                time.sleep(retry_interval)
            else:
                print(f"Failed to connect to database after {max_retries} attempts")
                return False
    
    return False


def main():
    """Main initialization function."""
    print("Starting PyAirbyte cache database initialization...")
    
    # Wait for database to be available
    if not wait_for_database():
        print("ERROR: Database is not available. Exiting.")
        sys.exit(1)
    
    # Initialize cache database manager
    manager = PyAirbyteCacheDBManager()
    
    # Initialize the cache schema
    print("Initializing cache schema...")
    if not manager.initialize_cache_database():
        print("ERROR: Failed to initialize cache schema. Exiting.")
        sys.exit(1)
    
    # Load all connector configurations
    print("Loading connector configurations...")
    try:
        connectors = load_all_connectors()
        print(f"Found {len(connectors)} connector(s)")
    except Exception as e:
        print(f"WARNING: Failed to load connector configurations: {e}")
        connectors = []
    
    # Create schemas for each connector
    if connectors:
        print("Creating schemas for connectors...")
        for connector in connectors:
            connector_name = connector.get('name')
            if connector_name:
                if manager.create_connector_schema(connector_name):
                    print(f"✓ Created schema for connector: {connector_name}")
                else:
                    print(f"✗ Failed to create schema for connector: {connector_name}")
            else:
                print(f"WARNING: Connector missing 'name' field: {connector}")
    
    # List all created schemas
    schemas = manager.list_connector_schemas()
    if schemas:
        print(f"\nCreated schemas: {', '.join(schemas)}")
    else:
        print("\nNo connector schemas found.")
    
    print("PyAirbyte cache database initialization completed successfully!")


if __name__ == "__main__":
    main() 