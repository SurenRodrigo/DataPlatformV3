import os
import yaml
from typing import List, Dict

EXTERNAL_CONNECTORS_DIR = os.path.join(os.path.dirname(__file__), '../../external-connectors')


def list_connector_files() -> List[str]:
    """List all YAML connector files in the external-connectors directory."""
    files = []
    for fname in os.listdir(EXTERNAL_CONNECTORS_DIR):
        if fname.endswith('.yaml') or fname.endswith('.yml'):
            files.append(os.path.join(EXTERNAL_CONNECTORS_DIR, fname))
    return files


def load_connector_config(file_path: str) -> Dict:
    """Load a single connector YAML file as a Python dict."""
    with open(file_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # For declarative sources, add a name field based on the filename
    if config.get('type') == 'DeclarativeSource':
        # Extract name from filename (e.g., 'sample-connector.yaml' -> 'sample-connector')
        filename = os.path.basename(file_path)
        connector_name = filename.replace('.yaml', '').replace('.yml', '')
        config['name'] = connector_name
    
    return config


def load_all_connectors() -> List[Dict]:
    """Load all connector configs from the external-connectors directory."""
    configs = []
    for file_path in list_connector_files():
        configs.append(load_connector_config(file_path))
    return configs


def get_connector_by_name(name: str) -> Dict:
    """Get a connector config by its 'name' field."""
    for config in load_all_connectors():
        if config.get('name') == name:
            return config
    raise ValueError(f"Connector with name '{name}' not found.") 