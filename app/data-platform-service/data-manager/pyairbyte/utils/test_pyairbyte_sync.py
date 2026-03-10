import unittest
from unittest.mock import Mock, patch, MagicMock
import os
import sys
sys.path.append('/Users/surenr/dev/99x-data-platform/version2/dataplatform-99x/app/data-manager')
from pyairbyte.utils.pyairbyte_sync import sync_connector


class TestPyAirbyteSync(unittest.TestCase):
    """Test cases for pyairbyte_sync module."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Mock environment variables
        self.env_patcher = patch.dict(os.environ, {
            'PYAIRBYTE_CACHE_DB_HOST': 'test_host',
            'PYAIRBYTE_CACHE_DB_PORT': '5432',
            'PYAIRBYTE_CACHE_DB_USER': 'test_user',
            'PYAIRBYTE_CACHE_DB_PASSWORD': 'test_password',
            'PYAIRBYTE_CACHE_DB_NAME': 'test_cache_db'
        })
        self.env_patcher.start()
    
    def tearDown(self):
        """Clean up after tests."""
        self.env_patcher.stop()
    
    @patch('pyairbyte.utils.pyairbyte_sync.get_connector_by_name')
    @patch('pyairbyte.utils.pyairbyte_sync.ab.get_source')
    @patch('pyairbyte.utils.pyairbyte_sync.PostgresCache')
    def test_sync_connector_standard_source_success(self, mock_postgres_cache, mock_get_source, mock_get_connector):
        """Test successful sync with standard connector (source-faker)."""
        # Mock connector config
        mock_get_connector.return_value = {
            'name': 'source-faker',
            'config': {'count': 100, 'seed': 123}
        }
        
        # Mock source
        mock_source = Mock()
        mock_source.check.return_value = None
        mock_source.select_all_streams.return_value = None
        mock_get_source.return_value = mock_source
        
        # Mock cache
        mock_cache = Mock()
        mock_postgres_cache.return_value = mock_cache
        
        # Mock sync result
        mock_result = Mock()
        mock_result.streams = {'users': [{'id': 1, 'name': 'test'}]}
        mock_result.processed_records = 1
        mock_source.read.return_value = mock_result
        
        # Test sync
        result = sync_connector('source-faker')
        
        # Verify PostgresCache was created with correct parameters
        mock_postgres_cache.assert_called_once_with(
            host='test_host',
            port=5432,
            database='test_cache_db',
            username='test_user',
            password='test_password',
            schema_name='pyairbyte_cache',
            table_prefix='',
            cleanup=True
        )
        
        # Verify result
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['connector'], 'source-faker')
        self.assertEqual(result['cache_type'], 'PostgresCache')
        self.assertEqual(result['cache_schema'], 'pyairbyte_cache')
        self.assertEqual(result['result']['processed_records'], 1)
    
    @patch('pyairbyte.utils.pyairbyte_sync.get_connector_by_name')
    @patch('pyairbyte.utils.pyairbyte_sync.ab.get_source')
    @patch('pyairbyte.utils.pyairbyte_sync.PostgresCache')
    @patch('builtins.open', create=True)
    @patch('pyairbyte.utils.pyairbyte_sync.yaml.safe_load')
    def test_sync_connector_declarative_source_success(self, mock_yaml_load, mock_open, mock_postgres_cache, mock_get_source, mock_get_connector):
        """Test successful sync with declarative connector."""
        # Mock connector config
        mock_get_connector.return_value = {
            'name': 'sample-connector',
            'type': 'DeclarativeSource'
        }
        
        # Mock YAML content
        mock_yaml_load.return_value = {
            'version': '6.60.0',
            'type': 'DeclarativeSource',
            'streams': []
        }
        
        # Mock source
        mock_source = Mock()
        mock_source.check.return_value = None
        mock_source.select_all_streams.return_value = None
        mock_get_source.return_value = mock_source
        
        # Mock cache
        mock_cache = Mock()
        mock_postgres_cache.return_value = mock_cache
        
        # Mock sync result
        mock_result = Mock()
        mock_result.streams = {'products': [{'id': 1, 'title': 'test'}]}
        mock_result.processed_records = 1
        mock_source.read.return_value = mock_result
        
        # Test sync
        result = sync_connector('sample-connector')
        
        # Verify PostgresCache was created
        mock_postgres_cache.assert_called_once()
        
        # Verify result
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['connector'], 'sample-connector')
        self.assertEqual(result['cache_type'], 'PostgresCache')
        self.assertEqual(result['cache_schema'], 'pyairbyte_cache')
    
    @patch('pyairbyte.utils.pyairbyte_sync.get_connector_by_name')
    def test_sync_connector_not_found(self, mock_get_connector):
        """Test sync with non-existent connector."""
        mock_get_connector.return_value = None
        
        result = sync_connector('non-existent-connector')
        
        self.assertEqual(result['status'], 'error')
        self.assertIn('Connector', result['error'])
    
    @patch('pyairbyte.utils.pyairbyte_sync.get_connector_by_name')
    @patch('pyairbyte.utils.pyairbyte_sync.ab.get_source')
    @patch('pyairbyte.utils.pyairbyte_sync.PostgresCache')
    def test_sync_connector_error(self, mock_postgres_cache, mock_get_source, mock_get_connector):
        """Test sync with error during execution."""
        # Mock connector config
        mock_get_connector.return_value = {
            'name': 'source-faker',
            'config': {'count': 100, 'seed': 123}
        }
        
        # Mock source
        mock_source = Mock()
        mock_source.check.return_value = None
        mock_source.select_all_streams.return_value = None
        mock_get_source.return_value = mock_source
        
        # Mock cache
        mock_cache = Mock()
        mock_postgres_cache.return_value = mock_cache
        
        # Mock error during sync
        mock_source.read.side_effect = Exception("Sync failed")
        
        # Test sync
        result = sync_connector('source-faker')
        
        # Verify error result
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['connector'], 'source-faker')
        self.assertEqual(result['cache_type'], 'PostgresCache')
        self.assertEqual(result['cache_schema'], 'pyairbyte_cache')
        self.assertIn('Sync failed', result['error'])


if __name__ == '__main__':
    unittest.main() 