import os
import tempfile
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import duckdb

class PyAirbyteDBBridge:
    """Bridge utility to copy data from DuckDB cache to PostgreSQL for DBT processing."""
    
    def __init__(self):
        self.pg_host = os.getenv('APPBASE_DB_HOST', 'db')
        self.pg_port = os.getenv('APPBASE_DB_PORT', '5432')
        self.pg_user = os.getenv('APPBASE_DB_USER', 'dataplatuser')
        self.pg_password = os.getenv('APPBASE_DB_PASSWORD', 'dataplatpassword')
        self.pg_database = os.getenv('APPBASE_DB_NAME', 'dataplatform')
        
    def get_pg_connection(self):
        """Get PostgreSQL connection."""
        return psycopg2.connect(
            host=self.pg_host,
            port=self.pg_port,
            user=self.pg_user,
            password=self.pg_password,
            database=self.pg_database
        )
    
    def copy_duckdb_to_postgres(self, duckdb_path: str, table_name: str, schema_name: str = 'pyairbyte_cache'):
        """
        Copy data from DuckDB cache file to PostgreSQL table.
        
        Args:
            duckdb_path: Path to DuckDB cache file
            table_name: Name of the table to copy
            schema_name: PostgreSQL schema name (default: pyairbyte_cache)
        """
        try:
            # Connect to DuckDB and read data
            duck_conn = duckdb.connect(duckdb_path)
            
            # Get table data
            query = f"SELECT * FROM {table_name}"
            df = duck_conn.execute(query).df()
            
            if df.empty:
                print(f"No data found in DuckDB table: {table_name}")
                return False
            
            # Connect to PostgreSQL
            pg_conn = self.get_pg_connection()
            cursor = pg_conn.cursor()
            
            # Create schema if it doesn't exist
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            
            # Check if table exists
            cursor.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = '{schema_name}' 
                    AND table_name = '{table_name}'
                );
            """)
            table_exists = cursor.fetchone()[0]
            
            if table_exists:
                # Table exists, truncate it instead of dropping to avoid dependency issues
                print(f"Table {schema_name}.{table_name} exists, truncating and reinserting data...")
                cursor.execute(f"TRUNCATE TABLE {schema_name}.{table_name}")
            else:
                # Table doesn't exist, create it
                print(f"Creating new table {schema_name}.{table_name}...")
                # Create table based on DataFrame structure
                columns = []
                for col, dtype in df.dtypes.items():
                    if 'int' in str(dtype):
                        pg_type = 'INTEGER'
                    elif 'float' in str(dtype):
                        pg_type = 'DOUBLE PRECISION'
                    elif 'bool' in str(dtype):
                        pg_type = 'BOOLEAN'
                    else:
                        pg_type = 'TEXT'
                    columns.append(f"{col} {pg_type}")
                
                create_table_sql = f"""
                    CREATE TABLE {schema_name}.{table_name} (
                        {', '.join(columns)}
                    )
                """
                cursor.execute(create_table_sql)
            
            # Insert data
            if not df.empty:
                # Convert DataFrame to list of tuples
                data = [tuple(row) for row in df.values]
                columns = list(df.columns)
                
                # Use execute_values for efficient bulk insert
                execute_values(
                    cursor,
                    f"INSERT INTO {schema_name}.{table_name} ({','.join(columns)}) VALUES %s",
                    data
                )
            
            pg_conn.commit()
            cursor.close()
            pg_conn.close()
            duck_conn.close()
            
            print(f"Successfully copied {len(df)} rows from DuckDB to PostgreSQL: {schema_name}.{table_name}")
            return True
            
        except Exception as e:
            print(f"Error copying data from DuckDB to PostgreSQL: {e}")
            return False
    
    def copy_faker_data(self, duckdb_path: str):
        """Copy faker users data from DuckDB to PostgreSQL."""
        return self.copy_duckdb_to_postgres(duckdb_path, 'users', 'pyairbyte_cache') 