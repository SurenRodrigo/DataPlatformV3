import os
import logging
from airbyte.caches import PostgresCache

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cache configurations object array
CACHE_CONFIGS = {
    'default': {
        'host': os.getenv('PYAIRBYTE_CACHE_DB_HOST', 'db'),
        'port': int(os.getenv('PYAIRBYTE_CACHE_DB_PORT', '5432')),
        'database': os.getenv('PYAIRBYTE_CACHE_DB_NAME', 'dataplatform'),
        'username': os.getenv('PYAIRBYTE_CACHE_DB_USER', 'dataplatuser'),
        'password': os.getenv('PYAIRBYTE_CACHE_DB_PASSWORD', 'dataplatpassword'),
        'schema_name': 'pyairbyte_cache',
        'table_prefix': 'default_',
        'cleanup': True
    }
    # Add more cache configurations here as needed
    # 'nrc': { ... },
    # 'cleanup': { ... },
    # 'custom': { ... }
}

def get_cache(cache_name: str, connector_name: str = None) -> PostgresCache:
    """
    Get a PostgresCache instance by cache name.
    
    Args:
        cache_name: Name of the cache configuration to use
        connector_name: Optional connector name to generate table prefix
        
    Returns:
        PostgresCache: Configured cache instance
        
    Raises:
        ValueError: If cache_name is not found in CACHE_CONFIGS
        Exception: If cache creation fails
    """
    if cache_name not in CACHE_CONFIGS:
        available_caches = list(CACHE_CONFIGS.keys())
        raise ValueError(f"Cache '{cache_name}' not found. Available caches: {available_caches}")
    
    cache_config = CACHE_CONFIGS[cache_name].copy()
    
    # If connector_name is provided, use it to generate table prefix
    if connector_name:
        table_prefix = f"{connector_name.replace('-', '_')}_"
        cache_config['table_prefix'] = table_prefix
        logger.info(f"Using connector-specific table prefix: '{table_prefix}'")
    
    try:
        cache = PostgresCache(**cache_config)
        logger.info(f"Created PostgresCache '{cache_name}' with prefix '{cache_config.get('table_prefix', '')}'")
        return cache
    except Exception as e:
        logger.error(f"Failed to create PostgresCache '{cache_name}': {e}")
        raise

# Export the main function
__all__ = ['get_cache', 'CACHE_CONFIGS']
