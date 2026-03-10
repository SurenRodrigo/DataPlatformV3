import os
import pyodbc
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.extensions import register_adapter, AsIs
import logging
import json
import yaml
from typing import List, Optional, Dict, Any, Tuple
from airbyte.caches import PostgresCache
import time

# Register psycopg2 adapter for pandas NaT to handle NULL values properly
try:
    register_adapter(pd._libs.tslibs.nattype.NaTType, lambda x: AsIs('NULL'))
except (AttributeError, ImportError):
    # Fallback if NaTType is not accessible
    pass

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# MSSQL to PostgreSQL Type Mapping
MSSQL_TO_POSTGRES_TYPE_MAP = {
    # Numeric types
    'int': 'INTEGER',
    'bigint': 'BIGINT',
    'smallint': 'SMALLINT',
    'tinyint': 'SMALLINT',
    'bit': 'BOOLEAN',
    'float': 'DOUBLE PRECISION',
    'real': 'REAL',
    'numeric': 'NUMERIC',
    'decimal': 'DECIMAL',
    'money': 'NUMERIC(19,4)',
    'smallmoney': 'NUMERIC(10,4)',
    
    # String types
    'varchar': 'VARCHAR',
    'nvarchar': 'VARCHAR',
    'char': 'CHAR',
    'nchar': 'CHAR',
    'text': 'TEXT',
    'ntext': 'TEXT',
    
    # Date/Time types
    'datetime': 'TIMESTAMP',
    'datetime2': 'TIMESTAMP',
    'smalldatetime': 'TIMESTAMP',
    'date': 'DATE',
    'time': 'TIME',
    'datetimeoffset': 'TIMESTAMP WITH TIME ZONE',
    
    # Special types
    'uniqueidentifier': 'UUID',
    'binary': 'BYTEA',
    'varbinary': 'BYTEA',
    'image': 'BYTEA',
    'xml': 'TEXT',
}


def map_mssql_to_postgres_type(
    mssql_type: str,
    max_length: Optional[int] = None,
    precision: Optional[int] = None,
    scale: Optional[int] = None
) -> str:
    """
    Map MSSQL data type to PostgreSQL data type.
    
    Args:
        mssql_type: MSSQL data type (lowercase)
        max_length: Maximum length for string types
        precision: Precision for numeric types
        scale: Scale for numeric types
        
    Returns:
        PostgreSQL data type string
    """
    mssql_type_lower = mssql_type.lower()
    
    # Handle MAX length strings
    if mssql_type_lower in ('varchar', 'nvarchar') and (max_length is None or max_length == -1):
        return 'TEXT'
    
    # Handle VARCHAR/NVARCHAR with length
    if mssql_type_lower in ('varchar', 'nvarchar') and max_length and max_length > 0:
        return f'VARCHAR({max_length})'
    
    # Handle CHAR/NCHAR with length
    if mssql_type_lower in ('char', 'nchar') and max_length and max_length > 0:
        return f'CHAR({max_length})'
    
    # Handle NUMERIC/DECIMAL with precision and scale
    if mssql_type_lower in ('numeric', 'decimal') and precision is not None:
        if scale is not None:
            return f'NUMERIC({precision},{scale})'
        return f'NUMERIC({precision})'
    
    # Handle fixed type mappings
    base_type = MSSQL_TO_POSTGRES_TYPE_MAP.get(mssql_type_lower)
    if base_type:
        return base_type
    
    # Default fallback for unknown types
    logger.warning(f"Unknown MSSQL type '{mssql_type}', defaulting to TEXT")
    return 'TEXT'


def get_mssql_connection(
    server: str,
    database: str,
    username: str,
    password: str,
    max_retries: int = 3,
    retry_delay: int = 5
) -> pyodbc.Connection:
    """
    Create MSSQL connection with retry logic.
    
    Args:
        server: MSSQL server hostname or IP
        database: Database name
        username: Username
        password: Password
        max_retries: Maximum number of retry attempts
        retry_delay: Delay between retries in seconds
        
    Returns:
        pyodbc.Connection object
        
    Raises:
        Exception: If connection fails after all retries
    """
    # Try ODBC Driver 18 first, fallback to 17
    drivers_to_try = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server"
    ]
    
    last_error = None
    for driver in drivers_to_try:
        connection_string = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            f"TrustServerCertificate=yes;"
            f"Connection Timeout=30;"
        )
        
        for attempt in range(max_retries):
            try:
                conn = pyodbc.connect(connection_string, timeout=30)
                logger.info(f"Successfully connected to MSSQL server: {server}/{database} using {driver}")
                return conn
            except pyodbc.Error as e:
                last_error = e
                logger.warning(f"MSSQL connection attempt {attempt + 1}/{max_retries} with {driver} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
            except Exception as e:
                last_error = e
                logger.warning(f"Unexpected error with {driver}: {e}")
        
        # If this driver failed, try the next one
        logger.info(f"Trying next ODBC driver...")
    
    # All drivers failed
    logger.error(f"Failed to connect to MSSQL with any available driver after {max_retries} attempts per driver")
    raise Exception(f"MSSQL connection failed: {last_error}")


def extract_mssql_schema(
    conn: pyodbc.Connection,
    schema_name: str,
    table_name: str
) -> List[Dict[str, Any]]:
    """
    Extract table schema from MSSQL.
    
    Args:
        conn: MSSQL connection
        schema_name: Schema name
        table_name: Table name
        
    Returns:
        List of column metadata dictionaries
    """
    query = """
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            IS_NULLABLE,
            COLUMN_DEFAULT
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
    """
    
    cursor = conn.cursor()
    cursor.execute(query, schema_name, table_name)
    columns = []
    
    for row in cursor.fetchall():
        columns.append({
            'name': row.COLUMN_NAME,
            'mssql_type': row.DATA_TYPE,
            'max_length': row.CHARACTER_MAXIMUM_LENGTH,
            'precision': row.NUMERIC_PRECISION,
            'scale': row.NUMERIC_SCALE,
            'is_nullable': row.IS_NULLABLE == 'YES',
            'default': row.COLUMN_DEFAULT
        })
    
    cursor.close()
    return columns


def validate_tables_exist(
    conn: pyodbc.Connection,
    schema_name: str,
    table_names: List[str]
) -> Tuple[List[str], List[str]]:
    """
    Validate that all specified tables exist in MSSQL.
    
    Args:
        conn: MSSQL connection
        schema_name: Schema name
        table_names: List of table names to validate
        
    Returns:
        Tuple of (existing_tables, missing_tables)
    """
    query = """
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    """
    
    cursor = conn.cursor()
    existing = []
    missing = []
    
    for table_name in table_names:
        cursor.execute(query, schema_name, table_name)
        if cursor.fetchone():
            existing.append(table_name)
        else:
            missing.append(table_name)
    
    cursor.close()
    return existing, missing


def create_postgresql_table(
    pg_conn: psycopg2.extensions.connection,
    schema_name: str,
    table_name: str,
    columns: List[Dict[str, Any]],
    table_prefix: str = ''
) -> bool:
    """
    Create PostgreSQL table from MSSQL schema.
    
    Args:
        pg_conn: PostgreSQL connection
        schema_name: Schema name (e.g., 'pyairbyte_cache')
        table_name: Table name
        columns: List of column metadata dictionaries
        table_prefix: Optional prefix for table name
        
    Returns:
        True if successful, False otherwise
    """
    try:
        cursor = pg_conn.cursor()
        
        # Create schema if it doesn't exist
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        
        # Sanitize table name
        safe_table_name = f"{table_prefix}{table_name}".replace('-', '_').replace(' ', '_')
        
        # Drop table if exists (replace mode)
        cursor.execute(f"DROP TABLE IF EXISTS {schema_name}.{safe_table_name}")
        
        # Build column definitions
        column_defs = []
        for col in columns:
            col_name = col['name'].replace('-', '_').replace(' ', '_')
            pg_type = map_mssql_to_postgres_type(
                col['mssql_type'],
                col['max_length'],
                col['precision'],
                col['scale']
            )
            nullable = 'NULL' if col['is_nullable'] else 'NOT NULL'
            column_defs.append(f"{col_name} {pg_type} {nullable}")
        
        # Create table
        create_sql = f"""
            CREATE TABLE {schema_name}.{safe_table_name} (
                {', '.join(column_defs)}
            )
        """
        
        cursor.execute(create_sql)
        pg_conn.commit()
        cursor.close()
        
        logger.info(f"Created PostgreSQL table: {schema_name}.{safe_table_name}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to create PostgreSQL table {table_name}: {e}")
        pg_conn.rollback()
        return False


def extract_and_transform_data(
    mssql_conn: pyodbc.Connection,
    schema_name: str,
    table_name: str,
    columns: List[Dict[str, Any]],
    batch_size: int = 10000
) -> pd.DataFrame:
    """
    Extract data from MSSQL table and transform for PostgreSQL.
    
    Args:
        mssql_conn: MSSQL connection
        schema_name: Schema name
        table_name: Table name
        columns: Column metadata
        batch_size: Batch size for reading data
        
    Returns:
        DataFrame with transformed data
    """
    query = f'SELECT * FROM [{schema_name}].[{table_name}]'
    
    logger.info(f"Extracting data from MSSQL table: {schema_name}.{table_name}")
    
    # Read data in chunks
    chunks = []
    try:
        for chunk in pd.read_sql(query, mssql_conn, chunksize=batch_size):
            # Transform data types
            for col in columns:
                col_name = col['name']
                if col_name not in chunk.columns:
                    continue
                    
                mssql_type = col['mssql_type'].lower()
                
                # Handle UNIQUEIDENTIFIER -> UUID (convert to string first)
                if mssql_type == 'uniqueidentifier':
                    chunk[col_name] = chunk[col_name].astype(str)
                
                # Handle BIT -> BOOLEAN
                elif mssql_type == 'bit':
                    chunk[col_name] = chunk[col_name].astype(bool)
                
                # Handle DATE/TIME types: coerce invalid to NaT, then convert to python datetime or None
                elif mssql_type in ('datetime', 'datetime2', 'smalldatetime', 'date', 'datetimeoffset', 'time'):
                    try:
                        # Coerce to datetime where applicable; for 'time' keep as string then parse if needed
                        if mssql_type == 'time':
                            # Ensure strings and replace NaN/NaT with None
                            chunk[col_name] = chunk[col_name].astype(object).where(pd.notnull(chunk[col_name]), None)
                        else:
                            series_dt = pd.to_datetime(chunk[col_name], errors='coerce', utc=False)
                            # Convert to Python datetime (or None) to avoid 'NaT' literals reaching psycopg2
                            chunk[col_name] = series_dt.apply(lambda v: v.to_pydatetime() if pd.notnull(v) else None)
                    except Exception:
                        # Fallback: ensure None for null-like values
                        chunk[col_name] = chunk[col_name].where(pd.notnull(chunk[col_name]), None)
                
                # Handle NULL values for all other types
                if mssql_type not in ('datetime', 'datetime2', 'smalldatetime', 'date', 'datetimeoffset', 'time'):
                    chunk[col_name] = chunk[col_name].where(pd.notnull(chunk[col_name]), None)
            
            chunks.append(chunk)
            
    except Exception as e:
        logger.error(f"Error extracting data from {table_name}: {e}")
        raise
    
    if not chunks:
        logger.warning(f"No data found in table: {schema_name}.{table_name}")
        # Return empty DataFrame with correct columns
        return pd.DataFrame(columns=[col['name'] for col in columns])
    
    # Combine all chunks
    df = pd.concat(chunks, ignore_index=True)
    logger.info(f"Extracted {len(df)} rows from {table_name}")
    
    return df


def load_data_to_postgres(
    pg_conn: psycopg2.extensions.connection,
    schema_name: str,
    table_name: str,
    df: pd.DataFrame,
    table_prefix: str = ''
) -> int:
    """
    Load data into PostgreSQL table using bulk insert.
    
    Args:
        pg_conn: PostgreSQL connection
        schema_name: Schema name
        table_name: Table name
        df: DataFrame with data to insert
        table_prefix: Optional prefix for table name
        
    Returns:
        Number of rows inserted
    """
    if df.empty:
        logger.info(f"No data to insert for table {table_name}")
        return 0
    
    try:
        cursor = pg_conn.cursor()
        safe_table_name = f"{table_prefix}{table_name}".replace('-', '_').replace(' ', '_')
        
        # Prepare data for bulk insert
        # Convert DataFrame to list of tuples, replacing NaT/NaN with None
        # This must be done carefully to avoid NaT being converted to string "NaT"
        columns = list(df.columns)
        data_tuples = []
        
        for _, row in df.iterrows():
            # Convert each row to a tuple, handling NaT/NaN properly
            row_values = []
            for col in columns:
                val = row[col]
                # Check for pandas NaT/NaN and convert to None
                if pd.isna(val) or val is pd.NaT:
                    row_values.append(None)
                # Convert pandas datetime to Python datetime
                elif isinstance(val, pd.Timestamp):
                    row_values.append(val.to_pydatetime())
                # Convert numpy datetime64 to Python datetime
                elif hasattr(val, 'to_pydatetime'):
                    try:
                        row_values.append(val.to_pydatetime())
                    except (ValueError, AttributeError):
                        row_values.append(None)
                else:
                    row_values.append(val)
            data_tuples.append(tuple(row_values))
        
        # Use execute_values for efficient bulk insert
        execute_values(
            cursor,
            f"INSERT INTO {schema_name}.{safe_table_name} ({','.join(columns)}) VALUES %s",
            data_tuples,
            page_size=1000
        )
        
        pg_conn.commit()
        rows_inserted = len(data_tuples)
        cursor.close()
        
        logger.info(f"Inserted {rows_inserted} rows into {schema_name}.{safe_table_name}")
        return rows_inserted
        
    except Exception as e:
        logger.error(f"Error loading data into PostgreSQL table {table_name}: {e}")
        pg_conn.rollback()
        raise


def validate_row_counts(
    mssql_conn: pyodbc.Connection,
    pg_conn: psycopg2.extensions.connection,
    schema_name: str,
    table_name: str,
    pg_schema_name: str,
    table_prefix: str = ''
) -> Tuple[int, int]:
    """
    Validate row counts between MSSQL and PostgreSQL.
    
    Args:
        mssql_conn: MSSQL connection
        pg_conn: PostgreSQL connection
        schema_name: MSSQL schema name
        table_name: Table name
        pg_schema_name: PostgreSQL schema name
        table_prefix: Optional prefix for table name
        
    Returns:
        Tuple of (mssql_count, postgres_count)
    """
    try:
        # Get MSSQL count
        mssql_cursor = mssql_conn.cursor()
        mssql_cursor.execute(f'SELECT COUNT(*) FROM [{schema_name}].[{table_name}]')
        mssql_count = mssql_cursor.fetchone()[0]
        mssql_cursor.close()
        
        # Get PostgreSQL count
        safe_table_name = f"{table_prefix}{table_name}".replace('-', '_').replace(' ', '_')
        pg_cursor = pg_conn.cursor()
        pg_cursor.execute(f'SELECT COUNT(*) FROM {pg_schema_name}.{safe_table_name}')
        pg_count = pg_cursor.fetchone()[0]
        pg_cursor.close()
        
        return mssql_count, pg_count
        
    except Exception as e:
        logger.error(f"Error validating row counts for {table_name}: {e}")
        return -1, -1


def get_mssql_config_from_connector(connector_name: str) -> Dict[str, str]:
    """
    Extract MSSQL configuration from PYAIRBYTE_CONNECTOR_CONFIGS environment variable.
    
    Args:
        connector_name: Name of the connector to get configuration for
        
    Returns:
        Dictionary with MSSQL configuration keys: server, database, username, password, schema
        
    Raises:
        ValueError: If connector configuration is not found or invalid
    """
    # Load connector-specific overrides from JSON env var (same pattern as pyairbyte_sync.py)
    env_json = os.getenv('PYAIRBYTE_CONNECTOR_CONFIGS')
    if not env_json:
        raise ValueError(f"PYAIRBYTE_CONNECTOR_CONFIGS environment variable not set")
    
    # Normalize value: trim and strip surrounding quotes if present
    normalized = env_json.strip()
    if (normalized.startswith("'") and normalized.endswith("'")) or (normalized.startswith('"') and normalized.endswith('"')):
        normalized = normalized[1:-1]
    
    # Try JSON first, then YAML as a fallback
    parsed_configs = None
    try:
        parsed_configs = json.loads(normalized)
    except Exception:
        try:
            parsed_configs = yaml.safe_load(normalized)
        except Exception as e_yaml:
            logger.error(f"Failed to parse PYAIRBYTE_CONNECTOR_CONFIGS: {e_yaml}")
            raise ValueError(f"Failed to parse PYAIRBYTE_CONNECTOR_CONFIGS: {e_yaml}")
    
    if not isinstance(parsed_configs, dict):
        raise ValueError("PYAIRBYTE_CONNECTOR_CONFIGS must be a JSON object")
    
    connector_config = parsed_configs.get(connector_name)
    if not isinstance(connector_config, dict):
        raise ValueError(f"Connector '{connector_name}' not found in PYAIRBYTE_CONNECTOR_CONFIGS")
    
    # Extract MSSQL configuration
    required_keys = ['server', 'database', 'username', 'password']
    missing_keys = [key for key in required_keys if key not in connector_config]
    if missing_keys:
        raise ValueError(f"Missing required MSSQL config keys for connector '{connector_name}': {missing_keys}")
    
    mssql_config = {
        'server': connector_config['server'],
        'database': connector_config['database'],
        'username': connector_config['username'],
        'password': connector_config['password'],
        'schema': connector_config.get('schema', 'dbo')
    }
    
    logger.info(f"Loaded MSSQL configuration for connector '{connector_name}' from PYAIRBYTE_CONNECTOR_CONFIGS")
    return mssql_config


def sync_mssql_tables(
    connector_name: str,
    table_names: List[str],
    cache: Optional[PostgresCache] = None,
    batch_size: int = 10000,
    table_prefix: str = 'mssql_'
) -> Dict[str, Any]:
    """
    Synchronize tables from MSSQL to PostgreSQL cache.
    
    Args:
        connector_name: Name of the connector (used to extract config from PYAIRBYTE_CONNECTOR_CONFIGS)
        table_names: List of table names to sync
        cache: Optional PostgresCache instance (creates new one if None)
        batch_size: Number of records per batch (default: 10000)
        table_prefix: Optional prefix for table names (default: 'mssql_')
        
    Returns:
        Dictionary with sync status and metadata (matching pyairbyte_sync.py format)
    """
    mssql_conn = None
    pg_conn = None
    
    try:
        # Get MSSQL configuration from PYAIRBYTE_CONNECTOR_CONFIGS
        mssql_config = get_mssql_config_from_connector(connector_name)
        
        server = mssql_config['server']
        database = mssql_config['database']
        username = mssql_config['username']
        password = mssql_config['password']
        schema_name = mssql_config.get('schema', 'dbo')
        
        logger.info(f"Starting MSSQL sync for connector '{connector_name}' with {len(table_names)} tables from {server}/{database}")
        
        # Connect to MSSQL
        mssql_conn = get_mssql_connection(server, database, username, password)
        
        # Validate tables exist
        existing_tables, missing_tables = validate_tables_exist(mssql_conn, schema_name, table_names)
        if missing_tables:
            error_msg = f"Tables not found in MSSQL: {missing_tables}"
            logger.error(error_msg)
            return {
                'status': 'error',
                'connector': connector_name,
                'cache_type': 'PostgresCache',
                'cache_schema': 'pyairbyte_cache',
                'error': error_msg
            }
        
        # Create or use PostgresCache
        if cache is None:
            try:
                cache = PostgresCache(
                    host=os.getenv('PYAIRBYTE_CACHE_DB_HOST', 'db'),
                    port=int(os.getenv('PYAIRBYTE_CACHE_DB_PORT', '5432')),
                    database=os.getenv('PYAIRBYTE_CACHE_DB_NAME', 'dataplatform'),
                    username=os.getenv('PYAIRBYTE_CACHE_DB_USER', 'dataplatuser'),
                    password=os.getenv('PYAIRBYTE_CACHE_DB_PASSWORD', 'dataplatpassword'),
                    schema_name='pyairbyte_cache',
                    table_prefix=table_prefix,
                    cleanup=True
                )
                logger.info(f"Created PostgresCache for MSSQL sync with table prefix: '{table_prefix}'")
            except Exception as e:
                logger.error(f"Failed to create PostgresCache: {e}")
                return {
                    'status': 'error',
                    'connector': connector_name,
                    'cache_type': 'PostgresCache',
                    'cache_schema': 'pyairbyte_cache',
                    'error': f"Failed to create PostgreSQL cache: {e}"
                }
        else:
            # If cache is provided, use its table_prefix if available, otherwise use parameter
            provided_prefix = cache.table_prefix if hasattr(cache, 'table_prefix') else None
            if provided_prefix:
                table_prefix = provided_prefix
                logger.info(f"Using table prefix from provided PostgresCache: '{table_prefix}'")
            else:
                logger.info(f"Using provided PostgresCache with parameter table prefix: '{table_prefix}'")
        
        # Get PostgreSQL connection details and create connection
        # PostgresCache doesn't expose get_connection(), so we create our own
        pg_schema_name = cache.schema_name if hasattr(cache, 'schema_name') else 'pyairbyte_cache'
        pg_conn = psycopg2.connect(
            host=os.getenv('PYAIRBYTE_CACHE_DB_HOST', 'db'),
            port=int(os.getenv('PYAIRBYTE_CACHE_DB_PORT', '5432')),
            database=os.getenv('PYAIRBYTE_CACHE_DB_NAME', 'dataplatform'),
            user=os.getenv('PYAIRBYTE_CACHE_DB_USER', 'dataplatuser'),
            password=os.getenv('PYAIRBYTE_CACHE_DB_PASSWORD', 'dataplatpassword')
        )
        
        # Initialize result dictionary
        result_tables = {}
        total_records = 0
        successful_tables = 0
        failed_tables = 0
        
        # Sync each table
        for table_name in existing_tables:
            table_result = {
                'rows_synced': 0,
                'schema_synced': False,
                'errors': []
            }
            
            try:
                logger.info(f"Syncing table: {schema_name}.{table_name}")
                
                # Extract schema
                columns = extract_mssql_schema(mssql_conn, schema_name, table_name)
                if not columns:
                    raise ValueError(f"No columns found for table {table_name}")
                
                # Create PostgreSQL table
                if not create_postgresql_table(pg_conn, pg_schema_name, table_name, columns, table_prefix):
                    raise Exception("Failed to create PostgreSQL table")
                
                table_result['schema_synced'] = True
                
                # Extract and transform data
                df = extract_and_transform_data(mssql_conn, schema_name, table_name, columns, batch_size)
                
                # Load data to PostgreSQL
                rows_inserted = load_data_to_postgres(pg_conn, pg_schema_name, table_name, df, table_prefix)
                table_result['rows_synced'] = rows_inserted
                total_records += rows_inserted
                
                # Validate row counts
                mssql_count, pg_count = validate_row_counts(
                    mssql_conn, pg_conn, schema_name, table_name, pg_schema_name, table_prefix
                )
                
                if mssql_count != pg_count:
                    warning_msg = f"Row count mismatch for {table_name}: MSSQL={mssql_count}, PostgreSQL={pg_count}"
                    logger.warning(warning_msg)
                    table_result['errors'].append(warning_msg)
                else:
                    logger.info(f"Row count validated for {table_name}: {mssql_count} rows")
                
                successful_tables += 1
                
            except Exception as e:
                error_msg = f"Error syncing table {table_name}: {str(e)}"
                logger.error(error_msg)
                table_result['errors'].append(error_msg)
                failed_tables += 1
            
            result_tables[table_name] = table_result
        
        # Close connections
        if mssql_conn:
            mssql_conn.close()
        if pg_conn:
            pg_conn.close()
        
        # Return result
        return {
            'status': 'success' if failed_tables == 0 else 'partial_success',
            'connector': connector_name,
            'cache_type': 'PostgresCache',
            'cache_schema': pg_schema_name,
            'result': {
                'tables': result_tables,
                'total_records': total_records,
                'total_tables': len(table_names),
                'successful_tables': successful_tables,
                'failed_tables': failed_tables
            }
        }
        
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        if mssql_conn:
            mssql_conn.close()
        if pg_conn:
            pg_conn.close()
        return {
            'status': 'error',
            'connector': connector_name,
            'cache_type': 'PostgresCache',
            'cache_schema': 'pyairbyte_cache',
            'error': str(e)
        }
    except Exception as e:
        logger.error(f"Unexpected error during MSSQL sync: {e}")
        if mssql_conn:
            mssql_conn.close()
        if pg_conn:
            pg_conn.close()
        return {
            'status': 'error',
            'connector': connector_name,
            'cache_type': 'PostgresCache',
            'cache_schema': 'pyairbyte_cache',
            'error': f"Unexpected error: {e}"
        }

