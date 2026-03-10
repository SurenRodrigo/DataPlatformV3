import os
import psycopg2
import logging
from typing import List, Optional, Dict, Any
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2 import OperationalError, ProgrammingError, IntegrityError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PyAirbyteCacheDBManager:
    """Manages PyAirbyte cache database schemas and metadata for PostgreSQL cache."""
    
    def __init__(self, cache_config: Optional[Dict[str, Any]] = None):
        """
        Initialize the cache database manager.
        
        Args:
            cache_config: Optional cache configuration dictionary. If not provided,
                         uses environment variables for default configuration.
        """
        if cache_config:
            self.host = cache_config.get('host', os.getenv('PYAIRBYTE_CACHE_DB_HOST', 'db'))
            self.port = cache_config.get('port', int(os.getenv('PYAIRBYTE_CACHE_DB_PORT', '5432')))
            self.user = cache_config.get('username', os.getenv('PYAIRBYTE_CACHE_DB_USER', 'dataplatuser'))
            self.password = cache_config.get('password', os.getenv('PYAIRBYTE_CACHE_DB_PASSWORD', 'dataplatpassword'))
            self.db_name = cache_config.get('database', os.getenv('PYAIRBYTE_CACHE_DB_NAME', 'dataplatform'))
            self.schema_name = cache_config.get('schema_name', 'pyairbyte_cache')
        else:
            # Use environment variables for default configuration
            self.host = os.getenv('PYAIRBYTE_CACHE_DB_HOST', 'db')
            self.port = os.getenv('PYAIRBYTE_CACHE_DB_PORT', '5432')
            self.user = os.getenv('PYAIRBYTE_CACHE_DB_USER', 'dataplatuser')
            self.password = os.getenv('PYAIRBYTE_CACHE_DB_PASSWORD', 'dataplatpassword')
            self.db_name = os.getenv('PYAIRBYTE_CACHE_DB_NAME', 'dataplatform')
            self.schema_name = 'pyairbyte_cache'
        
        self.max_retries = int(os.getenv('PYAIRBYTE_CACHE_MAX_RETRIES', '3'))
        self.retry_delay = int(os.getenv('PYAIRBYTE_CACHE_RETRY_DELAY', '5'))
        
        logger.info(f"Initialized PyAirbyteCacheDBManager with schema: {self.schema_name}")
    
    @classmethod
    def from_cache_name(cls, cache_name: str) -> 'PyAirbyteCacheDBManager':
        """
        Create a PyAirbyteCacheDBManager instance from a cache name.
        
        Args:
            cache_name: Name of the cache configuration from common_cache.CACHE_CONFIGS
            
        Returns:
            PyAirbyteCacheDBManager: Configured instance
            
        Raises:
            ValueError: If cache_name is not found in CACHE_CONFIGS
        """
        try:
            from .common_cache import CACHE_CONFIGS
            if cache_name not in CACHE_CONFIGS:
                available_caches = list(CACHE_CONFIGS.keys())
                raise ValueError(f"Cache '{cache_name}' not found. Available caches: {available_caches}")
            
            cache_config = CACHE_CONFIGS[cache_name]
            return cls(cache_config)
        except ImportError:
            raise ImportError("common_cache module not available. Please provide cache_config directly.")
    
    def get_connection(self, database: Optional[str] = None) -> psycopg2.extensions.connection:
        """Get a database connection with retry logic."""
        db_to_connect = database or self.db_name
        
        for attempt in range(self.max_retries):
            try:
                conn = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=db_to_connect
                )
                logger.info(f"Successfully connected to database: {db_to_connect}")
                return conn
            except OperationalError as e:
                logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
                if attempt < self.max_retries - 1:
                    import time
                    time.sleep(self.retry_delay)
                else:
                    logger.error(f"Failed to connect to database after {self.max_retries} attempts")
                    raise
            except Exception as e:
                logger.error(f"Unexpected error connecting to database: {e}")
                raise
    
    def create_cache_database(self) -> bool:
        """Create the PyAirbyte cache schema if it doesn't exist."""
        try:
            # Connect to the main database and create schema
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Create schema if it doesn't exist
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema_name}")
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Created PyAirbyte cache schema: {self.schema_name}")
            return True
            
        except OperationalError as e:
            logger.error(f"Database operation failed: {e}")
            return False
        except Exception as e:
            logger.error(f"Error creating cache schema: {e}")
            return False
    
    def create_cache_schema(self) -> bool:
        """Create the pyairbyte_cache schema if it doesn't exist."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Create schema if it doesn't exist
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema_name}")
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Created cache schema: {self.schema_name}")
            return True
            
        except OperationalError as e:
            logger.error(f"Schema creation failed: {e}")
            return False
        except Exception as e:
            logger.error(f"Error creating cache schema: {e}")
            return False
    
    def create_connector_schema(self, connector_name: str) -> bool:
        """Create a dedicated schema for a specific connector."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Create schema if it doesn't exist - sanitize name for SQL
            schema_name = f"airbyte_{connector_name.replace('-', '_')}"
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            
            # Create sync metadata table for this connector
            self._create_sync_metadata_table(cursor, schema_name)
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Created schema for connector '{connector_name}': {schema_name}")
            return True
            
        except OperationalError as e:
            logger.error(f"Schema creation failed for connector '{connector_name}': {e}")
            return False
        except Exception as e:
            logger.error(f"Error creating schema for connector '{connector_name}': {e}")
            return False
    
    def _create_sync_metadata_table(self, cursor, schema_name: str):
        """Create a sync metadata table within the given schema."""
        try:
            table_name = f"{schema_name}.sync_metadata"
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    connector_name VARCHAR(255) NOT NULL,
                    sync_started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    sync_completed_at TIMESTAMP,
                    sync_status VARCHAR(50) DEFAULT 'running',
                    records_synced INTEGER DEFAULT 0,
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        except ProgrammingError as e:
            logger.error(f"Error creating sync metadata table: {e}")
            raise
    
    def list_connector_schemas(self) -> List[str]:
        """List all connector schemas in the cache database."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name LIKE 'airbyte_%'
                ORDER BY schema_name
            """)
            
            schemas = [row[0] for row in cursor.fetchall()]
            
            cursor.close()
            conn.close()
            
            return schemas
            
        except OperationalError as e:
            logger.error(f"Database operation failed while listing schemas: {e}")
            return []
        except Exception as e:
            logger.error(f"Error listing connector schemas: {e}")
            return []
    
    def get_schema_for_connector(self, connector_name: str) -> str:
        """Get the schema name for a specific connector."""
        return f"airbyte_{connector_name.replace('-', '_')}"
    
    def list_cache_tables(self) -> List[str]:
        """List all tables in the pyairbyte_cache schema."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(f"""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = '{self.schema_name}'
                ORDER BY table_name
            """)
            
            tables = [row[0] for row in cursor.fetchall()]
            
            cursor.close()
            conn.close()
            
            return tables
            
        except OperationalError as e:
            logger.error(f"Database operation failed while listing tables: {e}")
            return []
        except Exception as e:
            logger.error(f"Error listing cache tables: {e}")
            return []
    
    def get_cache_table_info(self, table_name: str) -> dict:
        """Get information about a specific table in the cache schema."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Get table structure
            cursor.execute(f"""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_schema = '{self.schema_name}' 
                AND table_name = '{table_name}'
                ORDER BY ordinal_position
            """)
            
            columns = []
            for row in cursor.fetchall():
                columns.append({
                    'name': row[0],
                    'type': row[1],
                    'nullable': row[2] == 'YES'
                })
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {self.schema_name}.{table_name}")
            row_count = cursor.fetchone()[0]
            
            cursor.close()
            conn.close()
            
            return {
                'table_name': table_name,
                'schema_name': self.schema_name,
                'columns': columns,
                'row_count': row_count
            }
            
        except OperationalError as e:
            logger.error(f"Database operation failed while getting table info: {e}")
            return {}
        except Exception as e:
            logger.error(f"Error getting table info for {table_name}: {e}")
            return {}
    
    def truncate_cache_table(self, table_name: str) -> bool:
        """Truncate a table in the cache schema."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(f"TRUNCATE TABLE {self.schema_name}.{table_name}")
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Truncated table: {self.schema_name}.{table_name}")
            return True
            
        except OperationalError as e:
            logger.error(f"Database operation failed while truncating table: {e}")
            return False
        except Exception as e:
            logger.error(f"Error truncating table {table_name}: {e}")
            return False
    
    def drop_cache_table(self, table_name: str) -> bool:
        """Drop a table from the cache schema."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(f"DROP TABLE IF EXISTS {self.schema_name}.{table_name}")
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Dropped table: {self.schema_name}.{table_name}")
            return True
            
        except OperationalError as e:
            logger.error(f"Database operation failed while dropping table: {e}")
            return False
        except Exception as e:
            logger.error(f"Error dropping table {table_name}: {e}")
            return False
    
    def initialize_cache_database(self) -> bool:
        """Initialize the cache schema and create central metadata table."""
        try:
            # Create the cache schema (not a separate database)
            if not self.create_cache_schema():
                return False
            
            # Connect to the main database
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Create central sync metadata table
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema_name}.sync_metadata (
                    id SERIAL PRIMARY KEY,
                    connector_name VARCHAR(255) NOT NULL,
                    schema_name VARCHAR(255) NOT NULL,
                    sync_started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    sync_completed_at TIMESTAMP,
                    sync_status VARCHAR(50) DEFAULT 'running',
                    records_synced INTEGER DEFAULT 0,
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info("Initialized PyAirbyte cache schema with metadata table")
            return True
            
        except OperationalError as e:
            logger.error(f"Database operation failed during initialization: {e}")
            return False
        except Exception as e:
            logger.error(f"Error initializing cache schema: {e}")
            return False 