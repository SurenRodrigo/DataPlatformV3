import os
import tempfile
import yaml
import airbyte as ab
import logging
import json
from pathlib import Path
from typing import List, Optional
from .connector_loader import get_connector_by_name
from airbyte.caches import PostgresCache

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def sync_connector(connector_name: str, streams_to_sync: Optional[List[str]] = None, cache: Optional[PostgresCache] = None) -> dict:
    """
    Loads the connector config, instantiates the PyAirbyte source, and syncs data to the PostgreSQL cache.
    Returns a dict with sync status and metadata.
    If streams_to_sync is provided and non-empty, only those streams will be selected for sync.
    If cache is provided, uses the provided cache object; otherwise creates a new PostgresCache.
    """
    try:
        config = get_connector_by_name(connector_name)
        if not config:
            raise ValueError(f"Connector '{connector_name}' not found.")

        logger.info(f"Starting sync for connector: {connector_name}")

        # Handle different connector types
        if config.get('type') == 'DeclarativeSource':
            # For declarative sources, load the YAML content and pass it as a dictionary
            connector_file_path = Path('/app/data-manager/external-connectors') / f'{connector_name}.yaml'
            
            try:
                # Load the YAML content as a dictionary
                with open(connector_file_path, 'r') as f:
                    source_manifest_dict = yaml.safe_load(f)
                
                logger.info(f"Loaded declarative source manifest for {connector_name}")
                
                # Create configuration based on the connection specification and environment variables
                minimal_config = {}

                # Load connector-specific overrides from JSON env var
                env_json = os.getenv('PYAIRBYTE_CONNECTOR_CONFIGS')
                if env_json:
                    # Normalize value: trim and strip surrounding quotes if present
                    normalized = env_json.strip()
                    if (normalized.startswith("'") and normalized.endswith("'")) or (normalized.startswith('"') and normalized.endswith('"')):
                        normalized = normalized[1:-1]
                    # Try JSON first, then YAML as a fallback (users sometimes provide YAML-like maps)
                    parsed_configs = None
                    try:
                        parsed_configs = json.loads(normalized)
                    except Exception:
                        try:
                            parsed_configs = yaml.safe_load(normalized)
                        except Exception as e_yaml:
                            logger.error(f"Failed to parse PYAIRBYTE_CONNECTOR_CONFIGS: {e_yaml}")
                    if isinstance(parsed_configs, dict):
                        connector_overrides = parsed_configs.get(connector_name)
                        if isinstance(connector_overrides, dict):
                            minimal_config.update(connector_overrides)
                            logger.info(f"Loaded dynamic config overrides for '{connector_name}' from PYAIRBYTE_CONNECTOR_CONFIGS")
                    elif parsed_configs is not None:
                        logger.warning("PYAIRBYTE_CONNECTOR_CONFIGS parsed but is not a mapping; ignoring.")


                # Avoid logging secrets; only log config keys
                logger.info(f"Creating source with config keys: {list(minimal_config.keys())}")
                source = ab.get_source(
                    config['name'],
                    source_manifest=source_manifest_dict,  # Pass the YAML content as a dictionary
                    config=minimal_config,  # Provide configuration including API key
                    install_if_missing=True
                )
                logger.info(f"Source created successfully for {connector_name}")
            except FileNotFoundError:
                logger.error(f"Connector file not found: {connector_file_path}")
                return {
                    'status': 'error',
                    'connector': connector_name,
                    'cache_type': 'PostgresCache',
                    'cache_schema': 'pyairbyte_cache',
                    'error': f"Connector file not found: {connector_file_path}"
                }
            except yaml.YAMLError as e:
                logger.error(f"Invalid YAML in connector file: {e}")
                return {
                    'status': 'error',
                    'connector': connector_name,
                    'cache_type': 'PostgresCache',
                    'cache_schema': 'pyairbyte_cache',
                    'error': f"Invalid YAML in connector file: {e}"
                }
        else:
            # For standard connectors, use the config field
            try:
                source = ab.get_source(
                    config['name'],
                    config=config['config'],
                    install_if_missing=True
                )
                logger.info(f"Loaded standard connector: {config['name']}")
            except Exception as e:
                logger.error(f"Failed to load standard connector {config['name']}: {e}")
                return {
                    'status': 'error',
                    'connector': connector_name,
                    'cache_type': 'PostgresCache',
                    'cache_schema': 'pyairbyte_cache',
                    'error': f"Failed to load connector: {e}"
                }
        
        # Validate source configuration
        try:
            source.check()
            logger.info(f"Source validation passed for {connector_name}")
        except Exception as e:
            logger.error(f"Source validation failed for {connector_name}: {e}")
            return {
                'status': 'error',
                'connector': connector_name,
                'cache_type': 'PostgresCache',
                'cache_schema': 'pyairbyte_cache',
                'error': f"Source validation failed: {e}"
            }
        
        # Select streams
        try:
            if streams_to_sync:
                if not isinstance(streams_to_sync, (list, tuple)):
                    raise ValueError("streams_to_sync must be a list of stream names")
                source.select_streams(list(streams_to_sync))
                logger.info(f"Selected specific streams for {connector_name}: {streams_to_sync}")
            else:
                source.select_all_streams()
                logger.info(f"Selected all streams for {connector_name}")
        except Exception as e:
            logger.error(f"Failed to select streams for {connector_name}: {e}")
            return {
                'status': 'error',
                'connector': connector_name,
                'cache_type': 'PostgresCache',
                'cache_schema': 'pyairbyte_cache',
                'error': f"Failed to select streams: {e}"
            }

        # Use PostgresCache for direct PostgreSQL storage
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
                return {
                    'status': 'error',
                    'connector': connector_name,
                    'cache_type': 'PostgresCache',
                    'cache_schema': 'pyairbyte_cache',
                    'error': f"Failed to create PostgreSQL cache: {e}"
                }
        else:
            logger.info(f"Using provided cache object for {connector_name}")

        # Run sync
        try:
            logger.info(f"Starting data sync for {connector_name}")
            result = source.read(cache=cache)
            logger.info(f"Sync completed successfully for {connector_name}")
            
            return {
                'status': 'success',
                'connector': connector_name,
                'cache_type': 'PostgresCache',
                'cache_schema': 'pyairbyte_cache',
                'result': {
                    'streams': {k: len(list(v)) for k, v in result.streams.items()},
                    'processed_records': result.processed_records
                }
            }
        except Exception as e:
            logger.error(f"Sync failed for {connector_name}: {e}")
            return {
                'status': 'error',
                'connector': connector_name,
                'cache_type': 'PostgresCache',
                'cache_schema': 'pyairbyte_cache',
                'error': str(e)
            }
            
    except ValueError as e:
        logger.error(f"Configuration error for {connector_name}: {e}")
        return {
            'status': 'error',
            'connector': connector_name,
            'cache_type': 'PostgresCache',
            'cache_schema': 'pyairbyte_cache',
            'error': str(e)
        }
    except Exception as e:
        logger.error(f"Unexpected error during sync for {connector_name}: {e}")
        return {
            'status': 'error',
            'connector': connector_name,
            'cache_type': 'PostgresCache',
            'cache_schema': 'pyairbyte_cache',
            'error': f"Unexpected error: {e}"
        } 