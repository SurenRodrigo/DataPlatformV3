import unittest
from unittest.mock import Mock, patch, MagicMock
import os
import sys
import psycopg2
sys.path.append('/Users/surenr/dev/99x-data-platform/version2/dataplatform-99x/app/data-manager')
from pyairbyte.utils.cache_db_manager import PyAirbyteCacheDBManager
from pyairbyte.utils.pyairbyte_sync import sync_connector


class TestErrorHandling(unittest.TestCase):
    """Test cases for error handling and recovery mechanisms."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Mock environment variables
        self.env_patcher = patch.dict(os.environ, {
            'PYAIRBYTE_CACHE_DB_HOST': 'test_host',
            'PYAIRBYTE_CACHE_DB_PORT': '5432',
            'PYAIRBYTE_CACHE_DB_USER': 'test_user',
            'PYAIRBYTE_CACHE_DB_PASSWORD': 'test_password',
            'PYAIRBYTE_CACHE_DB_NAME': 'test_cache_db',
            'PYAIRBYTE_CACHE_MAX_RETRIES': '3',
            'PYAIRBYTE_CACHE_RETRY_DELAY': '1'
        })
        self.env_patcher.start()
        
        self.cache_manager = PyAirbyteCacheDBManager()
    
    def tearDown(self):
        """Clean up after tests."""
        self.env_patcher.stop()
    
    @patch('psycopg2.connect')
    def test_connection_retry_success(self, mock_connect):
        """Test connection retry logic with eventual success."""
        # First two attempts fail, third succeeds
        mock_connect.side_effect = [
            psycopg2.OperationalError("Connection failed"),
            psycopg2.OperationalError("Connection failed"),
            Mock()  # Successful connection
        ]
        
        conn = self.cache_manager.get_connection()
        
        self.assertEqual(mock_connect.call_count, 3)
        self.assertIsNotNone(conn)
    
    @patch('psycopg2.connect')
    def test_connection_retry_failure(self, mock_connect):
        """Test connection retry logic with eventual failure."""
        # All attempts fail
        mock_connect.side_effect = psycopg2.OperationalError("Connection failed")
        
        with self.assertRaises(psycopg2.OperationalError):
            self.cache_manager.get_connection()
        
        self.assertEqual(mock_connect.call_count, 3)
    
    @patch('psycopg2.connect')
    def test_operational_error_handling(self, mock_connect):
        """Test handling of OperationalError in database operations."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = psycopg2.OperationalError("Database operation failed")
        mock_connect.return_value = mock_conn
        
        result = self.cache_manager.create_cache_schema()
        
        self.assertFalse(result)
    
    @patch('psycopg2.connect')
    def test_programming_error_handling(self, mock_connect):
        """Test handling of ProgrammingError in table creation."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = psycopg2.ProgrammingError("Invalid SQL")
        mock_connect.return_value = mock_conn
        
        with self.assertRaises(psycopg2.ProgrammingError):
            self.cache_manager._create_sync_metadata_table(mock_cursor, 'test_schema')
    
    @patch('pyairbyte.utils.pyairbyte_sync.get_connector_by_name')
    def test_connector_not_found_error(self, mock_get_connector):
        """Test handling of connector not found error."""
        mock_get_connector.return_value = None
        
        result = sync_connector('non-existent-connector')
        
        self.assertEqual(result['status'], 'error')
        self.assertIn('Connector', result['error'])
    
    @patch('pyairbyte.utils.pyairbyte_sync.get_connector_by_name')
    @patch('pyairbyte.utils.pyairbyte_sync.ab.get_source')
    def test_source_validation_error(self, mock_get_source, mock_get_connector):
        """Test handling of source validation error."""
        mock_get_connector.return_value = {
            'name': 'source-faker',
            'config': {'count': 100, 'seed': 123}
        }
        
        mock_source = Mock()
        mock_source.check.side_effect = Exception("Validation failed")
        mock_get_source.return_value = mock_source
        
        result = sync_connector('source-faker')
        
        self.assertEqual(result['status'], 'error')
        self.assertIn('Source validation failed', result['error'])
    
    @patch('pyairbyte.utils.pyairbyte_sync.get_connector_by_name')
    @patch('pyairbyte.utils.pyairbyte_sync.ab.get_source')
    def test_stream_selection_error(self, mock_get_source, mock_get_connector):
        """Test handling of stream selection error."""
        mock_get_connector.return_value = {
            'name': 'source-faker',
            'config': {'count': 100, 'seed': 123}
        }
        
        mock_source = Mock()
        mock_source.check.return_value = None
        mock_source.select_all_streams.side_effect = Exception("Stream selection failed")
        mock_get_source.return_value = mock_source
        
        result = sync_connector('source-faker')
        
        self.assertEqual(result['status'], 'error')
        self.assertIn('Failed to select streams', result['error'])
    
    @patch('pyairbyte.utils.pyairbyte_sync.get_connector_by_name')
    @patch('pyairbyte.utils.pyairbyte_sync.ab.get_source')
    @patch('pyairbyte.utils.pyairbyte_sync.PostgresCache')
    def test_cache_creation_error(self, mock_postgres_cache, mock_get_source, mock_get_connector):
        """Test handling of cache creation error."""
        mock_get_connector.return_value = {
            'name': 'source-faker',
            'config': {'count': 100, 'seed': 123}
        }
        
        # Mock successful source creation
        mock_source = Mock()
        mock_source.check.return_value = None
        mock_source.select_all_streams.return_value = None
        mock_get_source.return_value = mock_source
        
        # Mock cache creation failure
        mock_postgres_cache.side_effect = Exception("Cache creation failed")
        
        result = sync_connector('source-faker')
        
        self.assertEqual(result['status'], 'error')
        self.assertIn('Failed to create PostgreSQL cache', result['error'])
    
    @patch('pyairbyte.utils.pyairbyte_sync.get_connector_by_name')
    @patch('pyairbyte.utils.pyairbyte_sync.ab.get_source')
    @patch('pyairbyte.utils.pyairbyte_sync.PostgresCache')
    def test_sync_execution_error(self, mock_postgres_cache, mock_get_source, mock_get_connector):
        """Test handling of sync execution error."""
        mock_get_connector.return_value = {
            'name': 'source-faker',
            'config': {'count': 100, 'seed': 123}
        }
        
        mock_source = Mock()
        mock_source.check.return_value = None
        mock_source.select_all_streams.return_value = None
        mock_source.read.side_effect = Exception("Sync execution failed")
        mock_get_source.return_value = mock_source
        
        mock_cache = Mock()
        mock_postgres_cache.return_value = mock_cache
        
        result = sync_connector('source-faker')
        
        self.assertEqual(result['status'], 'error')
        self.assertIn('Sync execution failed', result['error'])
    
    @patch('pyairbyte.utils.pyairbyte_sync.get_connector_by_name')
    @patch('builtins.open', create=True)
    def test_file_not_found_error(self, mock_open, mock_get_connector):
        """Test handling of file not found error for declarative sources."""
        mock_get_connector.return_value = {
            'name': 'sample-connector',
            'type': 'DeclarativeSource'
        }
        
        mock_open.side_effect = FileNotFoundError("File not found")
        
        result = sync_connector('sample-connector')
        
        self.assertEqual(result['status'], 'error')
        self.assertIn('Connector file not found', result['error'])
    
    @patch('pyairbyte.utils.pyairbyte_sync.get_connector_by_name')
    @patch('builtins.open', create=True)
    @patch('pyairbyte.utils.pyairbyte_sync.yaml.safe_load')
    def test_yaml_error_handling(self, mock_yaml_load, mock_open, mock_get_connector):
        """Test handling of YAML parsing errors."""
        mock_get_connector.return_value = {
            'name': 'sample-connector',
            'type': 'DeclarativeSource'
        }
        
        mock_yaml_load.side_effect = Exception("Invalid YAML")
        
        result = sync_connector('sample-connector')
        
        self.assertEqual(result['status'], 'error')
        self.assertIn('Invalid YAML', result['error'])


if __name__ == '__main__':
    unittest.main() 