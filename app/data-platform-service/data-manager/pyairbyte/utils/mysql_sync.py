import os
import logging
import json
import yaml
from typing import List, Optional, Dict, Any, Tuple

import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from airbyte.caches import PostgresCache


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# MySQL to PostgreSQL Type Mapping
MYSQL_TO_POSTGRES_TYPE_MAP = {
    # Numeric types
    'int': 'INTEGER',
    'integer': 'INTEGER',
    'bigint': 'BIGINT',
    'smallint': 'SMALLINT',
    'tinyint': 'SMALLINT',  # Note: tinyint(1) often used as boolean
    'bit': 'BOOLEAN',
    'float': 'DOUBLE PRECISION',
    'double': 'DOUBLE PRECISION',
    'double precision': 'DOUBLE PRECISION',
    'real': 'REAL',
    'decimal': 'DECIMAL',
    'numeric': 'NUMERIC',

    # String types
    'varchar': 'VARCHAR',
    'char': 'CHAR',
    'text': 'TEXT',
    'tinytext': 'TEXT',
    'mediumtext': 'TEXT',
    'longtext': 'TEXT',

    # Binary types
    'binary': 'BYTEA',
    'varbinary': 'BYTEA',
    'tinyblob': 'BYTEA',
    'blob': 'BYTEA',
    'mediumblob': 'BYTEA',
    'longblob': 'BYTEA',

    # Date/Time types
    'datetime': 'TIMESTAMP',
    'timestamp': 'TIMESTAMP',
    'date': 'DATE',
    'time': 'TIME',
    'year': 'INTEGER',
}


def map_mysql_to_postgres_type(
    mysql_type: str,
    max_length: Optional[int] = None,
    precision: Optional[int] = None,
    scale: Optional[int] = None
) -> str:
    mysql_type_lower = mysql_type.lower()

    # Handle VARCHAR with length
    if mysql_type_lower in ('varchar',) and max_length and max_length > 0:
        return f'VARCHAR({max_length})'

    # Handle CHAR with length
    if mysql_type_lower in ('char',) and max_length and max_length > 0:
        return f'CHAR({max_length})'

    # Handle DECIMAL/NUMERIC with precision and scale
    if mysql_type_lower in ('decimal', 'numeric') and precision is not None:
        if scale is not None:
            return f'DECIMAL({precision},{scale})'
        return f'DECIMAL({precision})'

    # Fixed mappings
    base_type = MYSQL_TO_POSTGRES_TYPE_MAP.get(mysql_type_lower)
    if base_type:
        return base_type

    logger.warning(f"Unknown MySQL type '{mysql_type}', defaulting to TEXT")
    return 'TEXT'


def get_mysql_config_from_connector(connector_name: str) -> Dict[str, str]:
    """
    Extract MySQL configuration from PYAIRBYTE_CONNECTOR_CONFIGS environment variable.
    Expected keys: host/server, database, username/user, password, (optional) schema
    """
    env_json = os.getenv('PYAIRBYTE_CONNECTOR_CONFIGS')
    if not env_json:
        raise ValueError('PYAIRBYTE_CONNECTOR_CONFIGS environment variable not set')

    normalized = env_json.strip()
    if (normalized.startswith("'") and normalized.endswith("'")) or (normalized.startswith('"') and normalized.endswith('"')):
        normalized = normalized[1:-1]

    try:
        parsed_configs = json.loads(normalized)
    except Exception:
        try:
            parsed_configs = yaml.safe_load(normalized)
        except Exception as e_yaml:
            logger.error(f"Failed to parse PYAIRBYTE_CONNECTOR_CONFIGS: {e_yaml}")
            raise ValueError(f"Failed to parse PYAIRBYTE_CONNECTOR_CONFIGS: {e_yaml}")

    if not isinstance(parsed_configs, dict):
        raise ValueError('PYAIRBYTE_CONNECTOR_CONFIGS must be a JSON object')

    connector_config = parsed_configs.get(connector_name)
    if not isinstance(connector_config, dict):
        raise ValueError(f"Connector '{connector_name}' not found in PYAIRBYTE_CONNECTOR_CONFIGS")

    # Accept either host or server; user or username
    host = connector_config.get('host') or connector_config.get('server')
    database = connector_config.get('database')
    username = connector_config.get('username') or connector_config.get('user')
    password = connector_config.get('password')
    schema = connector_config.get('schema')  # optional; in MySQL this is typically the database

    # If database is not provided but schema is, use schema as database (MySQL "database" == schema)
    if (not database) and schema:
        database = schema

    missing = []
    if not host: missing.append('host/server')
    # Require at least one of database or schema
    if not database and not schema: missing.append('database/schema')
    if not username: missing.append('username/user')
    if not password: missing.append('password')
    if missing:
        raise ValueError(f"Missing required MySQL config keys for connector '{connector_name}': {missing}")

    return {
        'host': host,
        'database': database,
        'username': username,
        'password': password,
        'schema': schema or database,
        'port': str(connector_config.get('port', '3306')),
        'ssl': connector_config.get('ssl', {})
    }


def get_mysql_engine(host: str, port: str, database: str, username: str, password: str, ssl: Optional[Dict[str, Any]] = None) -> Engine:
    """
    Create a SQLAlchemy engine for MySQL using mysql-connector-python driver.
    """
    url = f"mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}"

    # Enforce SSL; allow optional strict verification via connector config
    connect_args: Dict[str, Any] = { 'ssl_disabled': False }
    if isinstance(ssl, dict):
        # Map commonly used SSL fields
        if 'ssl_ca' in ssl and ssl['ssl_ca']:
            connect_args['ssl_ca'] = ssl['ssl_ca']
        if 'ssl_cert' in ssl and ssl['ssl_cert']:
            connect_args['ssl_cert'] = ssl['ssl_cert']
        if 'ssl_key' in ssl and ssl['ssl_key']:
            connect_args['ssl_key'] = ssl['ssl_key']
        if 'ssl_verify_cert' in ssl:
            connect_args['ssl_verify_cert'] = bool(ssl['ssl_verify_cert'])
        if 'ssl_verify_identity' in ssl:
            connect_args['ssl_verify_identity'] = bool(ssl['ssl_verify_identity'])
        if 'tls_versions' in ssl and ssl['tls_versions']:
            connect_args['tls_versions'] = ssl['tls_versions']

    engine = create_engine(url, pool_pre_ping=True, connect_args=connect_args)
    logger.info(f"Created MySQL engine to {host}/{database}")
    return engine


def extract_mysql_schema(engine: Engine, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
    query = text(
        """
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            IS_NULLABLE,
            COLUMN_DEFAULT
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = :schema_name AND TABLE_NAME = :table_name
        ORDER BY ORDINAL_POSITION
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, { 'schema_name': schema_name, 'table_name': table_name }).mappings().all()
    columns = []
    for row in rows:
        columns.append({
            'name': row['COLUMN_NAME'],
            'mysql_type': row['DATA_TYPE'],
            'max_length': row['CHARACTER_MAXIMUM_LENGTH'],
            'precision': row['NUMERIC_PRECISION'],
            'scale': row['NUMERIC_SCALE'],
            'is_nullable': (row['IS_NULLABLE'] == 'YES'),
            'default': row['COLUMN_DEFAULT']
        })
    return columns


def validate_tables_exist(engine: Engine, schema_name: str, table_names: List[str]) -> Tuple[List[str], List[str]]:
    query = text(
        """
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = :schema_name AND TABLE_NAME = :table_name
        """
    )
    existing, missing = [], []
    with engine.connect() as conn:
        for table_name in table_names:
            res = conn.execute(query, { 'schema_name': schema_name, 'table_name': table_name }).first()
            if res:
                existing.append(table_name)
            else:
                missing.append(table_name)
    return existing, missing


def create_postgresql_table(
    pg_conn: psycopg2.extensions.connection,
    schema_name: str,
    table_name: str,
    columns: List[Dict[str, Any]],
    table_prefix: str = ''
) -> bool:
    try:
        cursor = pg_conn.cursor()
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

        safe_table_name = f"{table_prefix}{table_name}".replace('-', '_').replace(' ', '_')

        cursor.execute(f"DROP TABLE IF EXISTS {schema_name}.{safe_table_name}")

        column_defs = []
        for col in columns:
            col_name = col['name'].replace('-', '_').replace(' ', '_')
            pg_type = map_mysql_to_postgres_type(
                col['mysql_type'],
                col['max_length'],
                col['precision'],
                col['scale']
            )
            nullable = 'NULL' if col['is_nullable'] else 'NOT NULL'
            column_defs.append(f"{col_name} {pg_type} {nullable}")

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
    engine: Engine,
    schema_name: str,
    table_name: str,
    columns: List[Dict[str, Any]],
    batch_size: int = 10000
) -> pd.DataFrame:
    query = f"SELECT * FROM `{schema_name}`.`{table_name}`"
    logger.info(f"Extracting data from MySQL table: {schema_name}.{table_name}")
    chunks: List[pd.DataFrame] = []
    try:
        with engine.connect() as conn:
            for chunk in pd.read_sql(text(query), conn, chunksize=batch_size):
                for col in columns:
                    col_name = col['name']
                    if col_name not in chunk.columns:
                        continue
                    mysql_type = col['mysql_type'].lower()

                    # BIT -> BOOLEAN
                    if mysql_type == 'bit':
                        # MySQL returns bytes for BIT; convert to bool
                        chunk[col_name] = chunk[col_name].apply(lambda v: bool(int.from_bytes(v, 'little')) if isinstance(v, (bytes, bytearray)) else bool(v) if pd.notnull(v) else None)

                    # DATETIME/TIMESTAMP/DATE/TIME handling
                    elif mysql_type in ('datetime', 'timestamp', 'date', 'time'):
                        try:
                            if mysql_type == 'time':
                                chunk[col_name] = chunk[col_name].astype(object).where(pd.notnull(chunk[col_name]), None)
                            else:
                                series_dt = pd.to_datetime(chunk[col_name], errors='coerce', utc=False)
                                chunk[col_name] = series_dt.apply(lambda v: v.to_pydatetime() if pd.notnull(v) else None)
                        except Exception:
                            chunk[col_name] = chunk[col_name].where(pd.notnull(chunk[col_name]), None)

                    # NULLs for others
                    if mysql_type not in ('datetime', 'timestamp', 'date', 'time'):
                        chunk[col_name] = chunk[col_name].where(pd.notnull(chunk[col_name]), None)

                chunks.append(chunk)
    except Exception as e:
        logger.error(f"Error extracting data from {table_name}: {e}")
        raise

    if not chunks:
        logger.warning(f"No data found in table: {schema_name}.{table_name}")
        return pd.DataFrame(columns=[c['name'] for c in columns])

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
    if df.empty:
        logger.info(f"No data to insert for table {table_name}")
        return 0
    try:
        cursor = pg_conn.cursor()
        safe_table_name = f"{table_prefix}{table_name}".replace('-', '_').replace(' ', '_')
        columns = list(df.columns)
        data_tuples = []
        for _, row in df.iterrows():
            row_values = []
            for col in columns:
                val = row[col]
                if pd.isna(val) or val is pd.NaT:
                    row_values.append(None)
                elif isinstance(val, pd.Timestamp):
                    row_values.append(val.to_pydatetime())
                elif hasattr(val, 'to_pydatetime'):
                    try:
                        row_values.append(val.to_pydatetime())
                    except (ValueError, AttributeError):
                        row_values.append(None)
                else:
                    row_values.append(val)
            data_tuples.append(tuple(row_values))

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
    engine: Engine,
    pg_conn: psycopg2.extensions.connection,
    schema_name: str,
    table_name: str,
    pg_schema_name: str,
    table_prefix: str = ''
) -> Tuple[int, int]:
    try:
        with engine.connect() as conn:
            mssql_count = conn.execute(text(f"SELECT COUNT(*) AS c FROM `{schema_name}`.`{table_name}`")).scalar()  # reuse variable name for parity
        safe_table_name = f"{table_prefix}{table_name}".replace('-', '_').replace(' ', '_')
        pg_cursor = pg_conn.cursor()
        pg_cursor.execute(f'SELECT COUNT(*) FROM {pg_schema_name}.{safe_table_name}')
        pg_count = pg_cursor.fetchone()[0]
        pg_cursor.close()
        return int(mssql_count), int(pg_count)
    except Exception as e:
        logger.error(f"Error validating row counts for {table_name}: {e}")
        return -1, -1


def sync_mysql_tables(
    connector_name: str,
    table_names: List[str],
    cache: Optional[PostgresCache] = None,
    batch_size: int = 10000,
    table_prefix: str = 'mysql_'
) -> Dict[str, Any]:
    engine: Optional[Engine] = None
    pg_conn: Optional[psycopg2.extensions.connection] = None
    try:
        mysql_config = get_mysql_config_from_connector(connector_name)
        host = mysql_config['host']
        database = mysql_config['database']
        username = mysql_config['username']
        password = mysql_config['password']
        schema_name = mysql_config['schema']
        port = mysql_config['port']
        ssl = mysql_config.get('ssl', {})

        logger.info(f"Starting MySQL sync for connector '{connector_name}' with {len(table_names)} tables from {host}/{database}")

        engine = get_mysql_engine(host, port, database, username, password, ssl)

        existing_tables, missing_tables = validate_tables_exist(engine, schema_name, table_names)
        if missing_tables:
            error_msg = f"Tables not found in MySQL: {missing_tables}"
            logger.error(error_msg)
            return {
                'status': 'error',
                'connector': connector_name,
                'cache_type': 'PostgresCache',
                'cache_schema': 'pyairbyte_cache',
                'error': error_msg
            }

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
                logger.info(f"Created PostgresCache for MySQL sync with table prefix: '{table_prefix}'")
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
            provided_prefix = cache.table_prefix if hasattr(cache, 'table_prefix') else None
            if provided_prefix:
                table_prefix = provided_prefix
                logger.info(f"Using table prefix from provided PostgresCache: '{table_prefix}'")
            else:
                logger.info(f"Using provided PostgresCache with parameter table prefix: '{table_prefix}'")

        pg_schema_name = cache.schema_name if hasattr(cache, 'schema_name') else 'pyairbyte_cache'
        pg_conn = psycopg2.connect(
            host=os.getenv('PYAIRBYTE_CACHE_DB_HOST', 'db'),
            port=int(os.getenv('PYAIRBYTE_CACHE_DB_PORT', '5432')),
            database=os.getenv('PYAIRBYTE_CACHE_DB_NAME', 'dataplatform'),
            user=os.getenv('PYAIRBYTE_CACHE_DB_USER', 'dataplatuser'),
            password=os.getenv('PYAIRBYTE_CACHE_DB_PASSWORD', 'dataplatpassword')
        )

        result_tables: Dict[str, Any] = {}
        total_records = 0
        successful_tables = 0
        failed_tables = 0

        for table_name in existing_tables:
            table_result = { 'rows_synced': 0, 'schema_synced': False, 'errors': [] }
            try:
                logger.info(f"Syncing table: {schema_name}.{table_name}")
                columns = extract_mysql_schema(engine, schema_name, table_name)
                if not columns:
                    raise ValueError(f"No columns found for table {table_name}")

                if not create_postgresql_table(pg_conn, pg_schema_name, table_name, columns, table_prefix):
                    raise Exception('Failed to create PostgreSQL table')
                table_result['schema_synced'] = True

                df = extract_and_transform_data(engine, schema_name, table_name, columns, batch_size)
                rows_inserted = load_data_to_postgres(pg_conn, pg_schema_name, table_name, df, table_prefix)
                table_result['rows_synced'] = rows_inserted
                total_records += rows_inserted

                src_count, dst_count = validate_row_counts(engine, pg_conn, schema_name, table_name, pg_schema_name, table_prefix)
                if src_count != dst_count:
                    warning_msg = f"Row count mismatch for {table_name}: MySQL={src_count}, PostgreSQL={dst_count}"
                    logger.warning(warning_msg)
                    table_result['errors'].append(warning_msg)
                else:
                    logger.info(f"Row count validated for {table_name}: {src_count} rows")

                successful_tables += 1
            except Exception as e:
                error_msg = f"Error syncing table {table_name}: {str(e)}"
                logger.error(error_msg)
                table_result['errors'].append(error_msg)
                failed_tables += 1
            result_tables[table_name] = table_result

        if pg_conn:
            pg_conn.close()

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
        logger.error(f"Unexpected error during MySQL sync: {e}")
        if pg_conn:
            pg_conn.close()
        return {
            'status': 'error',
            'connector': connector_name,
            'cache_type': 'PostgresCache',
            'cache_schema': 'pyairbyte_cache',
            'error': f"Unexpected error: {e}"
        }


