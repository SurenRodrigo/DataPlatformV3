import unittest
from unittest.mock import Mock, patch, MagicMock
import os
import psycopg2
import sys
sys.path.append('/Users/surenr/dev/99x-data-platform/version2/dataplatform-99x/app/data-manager')
from pyairbyte.utils.cache_db_manager import PyAirbyteCacheDBManager


class TestPyAirbyteCacheDBManager(unittest.TestCase):
    """Test cases for PyAirbyteCacheDBManager."""
    
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
        
        self.cache_manager = PyAirbyteCacheDBManager()
    
    def tearDown(self):
        """Clean up after tests."""
        self.env_patcher.stop()
    
    @patch('psycopg2.connect')
    def test_get_connection(self, mock_connect):
        """Test database connection creation."""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        
        conn = self.cache_manager.get_connection()
        
        mock_connect.assert_called_once_with(
            host='test_host',
            port='5432',
            user='test_user',
            password='test_password',
            database='test_cache_db'
        )
        self.assertEqual(conn, mock_conn)
    
    @patch('psycopg2.connect')
    def test_create_cache_database_success(self, mock_connect):
        """Test successful cache database creation."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = None  # Database doesn't exist
        mock_connect.return_value = mock_conn
        
        result = self.cache_manager.create_cache_database()
        
        self.assertTrue(result)
        mock_cursor.execute.assert_any_call("SELECT 1 FROM pg_database WHERE datname = %s", ('test_cache_db',))
        mock_cursor.execute.assert_any_call("CREATE DATABASE test_cache_db")
    
    @patch('psycopg2.connect')
    def test_create_cache_database_exists(self, mock_connect):
        """Test cache database creation when database already exists."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = ('test_cache_db',)  # Database exists
        mock_connect.return_value = mock_conn
        
        result = self.cache_manager.create_cache_database()
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once_with("SELECT 1 FROM pg_database WHERE datname = %s", ('test_cache_db',))
        # Should not call CREATE DATABASE
    
    @patch('psycopg2.connect')
    def test_create_cache_schema(self, mock_connect):
        """Test cache schema creation."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        result = self.cache_manager.create_cache_schema()
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once_with("CREATE SCHEMA IF NOT EXISTS pyairbyte_cache")
        mock_conn.commit.assert_called_once()
    
    @patch('psycopg2.connect')
    def test_create_connector_schema(self, mock_connect):
        """Test connector schema creation."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        result = self.cache_manager.create_connector_schema('test_connector')
        
        self.assertTrue(result)
        mock_cursor.execute.assert_any_call("CREATE SCHEMA IF NOT EXISTS airbyte_test_connector")
        mock_conn.commit.assert_called_once()
    
    @patch('psycopg2.connect')
    def test_list_cache_tables(self, mock_connect):
        """Test listing cache tables."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [('table1',), ('table2',)]
        mock_connect.return_value = mock_conn
        
        tables = self.cache_manager.list_cache_tables()
        
        self.assertEqual(tables, ['table1', 'table2'])
        mock_cursor.execute.assert_called_once()
    
    @patch('psycopg2.connect')
    def test_get_cache_table_info(self, mock_connect):
        """Test getting table information."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.side_effect = [
            [('id', 'integer', 'NO'), ('name', 'text', 'YES')],  # Columns
            [(100,)]  # Row count
        ]
        mock_cursor.fetchone.return_value = (100,)  # Row count for COUNT query
        mock_connect.return_value = mock_conn
        
        table_info = self.cache_manager.get_cache_table_info('test_table')
        
        expected_info = {
            'table_name': 'test_table',
            'schema_name': 'pyairbyte_cache',
            'columns': [
                {'name': 'id', 'type': 'integer', 'nullable': False},
                {'name': 'name', 'type': 'text', 'nullable': True}
            ],
            'row_count': 100
        }
        self.assertEqual(table_info, expected_info)
    
    @patch('psycopg2.connect')
    def test_truncate_cache_table(self, mock_connect):
        """Test truncating cache table."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        result = self.cache_manager.truncate_cache_table('test_table')
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once_with("TRUNCATE TABLE pyairbyte_cache.test_table")
        mock_conn.commit.assert_called_once()
    
    @patch('psycopg2.connect')
    def test_drop_cache_table(self, mock_connect):
        """Test dropping cache table."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        result = self.cache_manager.drop_cache_table('test_table')
        
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once_with("DROP TABLE IF EXISTS pyairbyte_cache.test_table")
        mock_conn.commit.assert_called_once()
    
    @patch('psycopg2.connect')
    def test_initialize_cache_database(self, mock_connect):
        """Test cache database initialization."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = None  # Database doesn't exist
        mock_connect.return_value = mock_conn
        
        result = self.cache_manager.initialize_cache_database()
        
        self.assertTrue(result)
        # Should create database, schema, and metadata table
        self.assertGreaterEqual(mock_cursor.execute.call_count, 3)
    
    def test_get_schema_for_connector(self):
        """Test getting schema name for connector."""
        schema_name = self.cache_manager.get_schema_for_connector('test_connector')
        self.assertEqual(schema_name, 'airbyte_test_connector')


if __name__ == '__main__':
    unittest.main() 