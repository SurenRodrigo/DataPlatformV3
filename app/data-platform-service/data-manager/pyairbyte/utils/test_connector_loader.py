import unittest
from unittest.mock import Mock, patch, MagicMock
import os
import sys
sys.path.append('/Users/surenr/dev/99x-data-platform/version2/dataplatform-99x/app/data-manager')
from pyairbyte.utils.connector_loader import (
    list_connector_files, 
    load_connector_config, 
    load_all_connectors, 
    get_connector_by_name
)


class TestConnectorLoader(unittest.TestCase):
    """Test cases for connector_loader module."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Mock the external connectors directory
        self.external_connectors_dir = '/app/data-manager/external-connectors'
    
    @patch('pyairbyte.utils.connector_loader.os.listdir')
    def test_list_connector_files(self, mock_listdir):
        """Test listing connector files."""
        mock_listdir.return_value = [
            'example-faker.yaml',
            'sample-connector.yaml',
            'ignore.txt',
            'another.yml'
        ]
        
        files = list_connector_files()
        
        # Should only include YAML files with relative paths
        expected_files = [
            '/Users/surenr/dev/99x-data-platform/version2/dataplatform-99x/app/data-manager/pyairbyte/utils/../../external-connectors/example-faker.yaml',
            '/Users/surenr/dev/99x-data-platform/version2/dataplatform-99x/app/data-manager/pyairbyte/utils/../../external-connectors/sample-connector.yaml',
            '/Users/surenr/dev/99x-data-platform/version2/dataplatform-99x/app/data-manager/pyairbyte/utils/../../external-connectors/another.yml'
        ]
        self.assertEqual(files, expected_files)
    
    @patch('builtins.open', create=True)
    @patch('pyairbyte.utils.connector_loader.yaml.safe_load')
    def test_load_connector_config_standard(self, mock_yaml_load, mock_open):
        """Test loading standard connector config."""
        mock_yaml_load.return_value = {
            'version': 1,
            'name': 'source-faker',
            'connector_type': 'source',
            'image': 'airbyte/source-faker:latest',
            'config': {'count': 1000, 'seed': 123}
        }
        
        config = load_connector_config('/app/data-manager/external-connectors/example-faker.yaml')
        
        self.assertEqual(config['name'], 'source-faker')
        self.assertEqual(config['config']['count'], 1000)
        self.assertNotIn('type', config)  # Standard connector doesn't have type field
    
    @patch('builtins.open', create=True)
    @patch('pyairbyte.utils.connector_loader.yaml.safe_load')
    def test_load_connector_config_declarative(self, mock_yaml_load, mock_open):
        """Test loading declarative connector config."""
        mock_yaml_load.return_value = {
            'version': '6.60.0',
            'type': 'DeclarativeSource',
            'streams': []
        }
        
        config = load_connector_config('/app/data-manager/external-connectors/sample-connector.yaml')
        
        self.assertEqual(config['name'], 'sample-connector')  # Name extracted from filename
        self.assertEqual(config['type'], 'DeclarativeSource')
        self.assertEqual(config['version'], '6.60.0')
    
    @patch('pyairbyte.utils.connector_loader.list_connector_files')
    @patch('pyairbyte.utils.connector_loader.load_connector_config')
    def test_load_all_connectors(self, mock_load_config, mock_list_files):
        """Test loading all connector configs."""
        mock_list_files.return_value = [
            '/app/data-manager/external-connectors/example-faker.yaml',
            '/app/data-manager/external-connectors/sample-connector.yaml'
        ]
        
        mock_load_config.side_effect = [
            {'name': 'source-faker', 'config': {'count': 1000}},
            {'name': 'sample-connector', 'type': 'DeclarativeSource'}
        ]
        
        configs = load_all_connectors()
        
        self.assertEqual(len(configs), 2)
        self.assertEqual(configs[0]['name'], 'source-faker')
        self.assertEqual(configs[1]['name'], 'sample-connector')
    
    @patch('pyairbyte.utils.connector_loader.load_all_connectors')
    def test_get_connector_by_name_found(self, mock_load_all):
        """Test getting connector by name when found."""
        mock_load_all.return_value = [
            {'name': 'source-faker', 'config': {'count': 1000}},
            {'name': 'sample-connector', 'type': 'DeclarativeSource'}
        ]
        
        config = get_connector_by_name('source-faker')
        
        self.assertEqual(config['name'], 'source-faker')
        self.assertEqual(config['config']['count'], 1000)
    
    @patch('pyairbyte.utils.connector_loader.load_all_connectors')
    def test_get_connector_by_name_not_found(self, mock_load_all):
        """Test getting connector by name when not found."""
        mock_load_all.return_value = [
            {'name': 'source-faker', 'config': {'count': 1000}},
            {'name': 'sample-connector', 'type': 'DeclarativeSource'}
        ]
        
        with self.assertRaises(ValueError) as context:
            get_connector_by_name('non-existent-connector')
        
        self.assertIn("Connector with name 'non-existent-connector' not found", str(context.exception))
    
    @patch('pyairbyte.utils.connector_loader.load_all_connectors')
    def test_get_connector_by_name_declarative(self, mock_load_all):
        """Test getting declarative connector by name."""
        mock_load_all.return_value = [
            {'name': 'source-faker', 'config': {'count': 1000}},
            {'name': 'sample-connector', 'type': 'DeclarativeSource'}
        ]
        
        config = get_connector_by_name('sample-connector')
        
        self.assertEqual(config['name'], 'sample-connector')
        self.assertEqual(config['type'], 'DeclarativeSource')


if __name__ == '__main__':
    unittest.main() 