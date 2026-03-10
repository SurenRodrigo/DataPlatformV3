import os
import logging
from typing import Optional
from sqlalchemy import create_engine, text
import pandas as pd
from airbyte.caches import PostgresCache

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SqlWriter:
    """Utility to write pandas DataFrames to SQL tables using PostgresCache configuration."""
    
    def __init__(self, connector_name: str, cache: Optional[PostgresCache] = None):
        """
        Initialize SqlWriter with connector name and optional cache.
        
        Args:
            connector_name: Name of the connector (used for table prefix if cache not provided)
            cache: Optional PostgresCache instance. If not provided, creates one using default env vars.
        """
        # Create cache if not provided
        if cache is None:
            try:
                table_prefix = f"{connector_name.replace('-', '_')}_"
                
                cache = PostgresCache(
                    host=os.getenv('PYAIRBYTE_CACHE_DB_HOST', 'db'),
                    port=int(os.getenv('PYAIRBYTE_CACHE_DB_PORT', '5432')),
                    database=os.getenv('PYAIRBYTE_CACHE_DB_NAME', 'dataplatform'),
                    username=os.getenv('PYAIRBYTE_CACHE_DB_USER', 'dataplatuser'),
                    password=os.getenv('PYAIRBYTE_CACHE_DB_PASSWORD', 'dataplatpassword'),
                    schema_name='pyairbyte_cache',  # Use pyairbyte_cache schema
                    table_prefix=table_prefix,  # Dynamic prefix based on connector
                    cleanup=True
                )
                logger.info(f"Created PostgresCache for {connector_name} in pyairbyte_cache schema with prefix '{table_prefix}'")
            except Exception as e:
                logger.error(f"Failed to create PostgresCache for {connector_name}: {e}")
                raise ValueError(f"Failed to create PostgreSQL cache: {e}")
        else:
            logger.info(f"Using provided cache object for {connector_name}")
        
        self.cache = cache
        self.connector_name = connector_name
        
        # Extract schema name from cache
        self.schema_name = cache.schema_name if hasattr(cache, 'schema_name') else 'pyairbyte_cache'
        
        # Extract connection details for SQLAlchemy engine
        # Try to get from cache attributes first, fall back to environment variables
        host = getattr(cache, 'host', None) or os.getenv('PYAIRBYTE_CACHE_DB_HOST', 'db')
        port = getattr(cache, 'port', None) or int(os.getenv('PYAIRBYTE_CACHE_DB_PORT', '5432'))
        database = getattr(cache, 'database', None) or os.getenv('PYAIRBYTE_CACHE_DB_NAME', 'dataplatform')
        username = getattr(cache, 'username', None) or os.getenv('PYAIRBYTE_CACHE_DB_USER', 'dataplatuser')
        password = getattr(cache, 'password', None) or os.getenv('PYAIRBYTE_CACHE_DB_PASSWORD', 'dataplatpassword')
        
        # Convert port to string for SQLAlchemy connection string
        port = str(port)
        
        # Create SQLAlchemy engine
        self.engine = create_engine(
            f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}'
        )
        
        logger.info(f"SqlWriter initialized for connector '{connector_name}' with schema '{self.schema_name}'")
        
    def _validate_schema_exists(self):
        """
        Validate that the schema exists in the database.
        Raises ValueError if schema does not exist.
        """
        try:
            with self.engine.connect() as conn:
                # Check if schema exists
                query = text("""
                    SELECT schema_name 
                    FROM information_schema.schemata 
                    WHERE schema_name = :schema_name
                """)
                result = conn.execute(query, {"schema_name": self.schema_name})
                schema_exists = result.fetchone() is not None
                
                if not schema_exists:
                    raise ValueError(
                        f"Schema '{self.schema_name}' does not exist. "
                        f"Please ensure the cache is properly configured and the schema exists. "
                        f"SqlWriter does not create schemas automatically."
                    )
                
                logger.info(f"Schema '{self.schema_name}' validated successfully")
        except Exception as e:
            logger.error(f"Error validating schema '{self.schema_name}': {e}")
            raise
        
    def write_df_to_table(self, df: pd.DataFrame, table_name: str, if_exists: str = 'append'):
        """
        Write a pandas DataFrame to a PostgreSQL table.
        
        Args:
            df: DataFrame to write
            table_name: Name of the table
            if_exists: What to do if table exists ('append', 'replace', 'fail')
        
        Raises:
            ValueError: If schema does not exist or cache is not properly configured
        """
        # Validate schema exists (raises error if not)
        self._validate_schema_exists()
        
        # Write DataFrame to table
        df.to_sql(
            table_name, 
            self.engine,
            schema=self.schema_name,
            if_exists=if_exists,
            index=False
        )
        
        logger.info(f"Successfully wrote {len(df)} rows to table '{self.schema_name}.{table_name}'")

