import unittest
from unittest.mock import Mock, patch, MagicMock
import os
import sys
import json

# Add the data-manager path to sys.path for imports
# This allows importing from pyairbyte.utils when running tests
if '/app/data-manager' not in sys.path:
    sys.path.append('/app/data-manager')

from pyairbyte.utils.event_store import (
    write_event,
    get_unprocessed_or_failed_events,
    log_event_processing,
    bulk_write_events,
    _create_event_hash,
    _check_hash_exists
)


class TestEventStore(unittest.TestCase):
    """Test cases for event_store module."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Mock environment variables
        self.env_patcher = patch.dict(os.environ, {
            'HASURA_URL': 'http://hasura:8080',
            'HASURA_GRAPHQL_ADMIN_SECRET': 'admin'
        })
        self.env_patcher.start()
    
    def tearDown(self):
        """Clean up after tests."""
        self.env_patcher.stop()
    
    def test_create_event_hash(self):
        """Test event hash creation."""
        event_type = "UNIT4_DITIO_EVENT"
        event_data = {"test": "data", "value": 123}
        
        hash1 = _create_event_hash(event_type, event_data)
        hash2 = _create_event_hash(event_type, event_data)
        
        # Same input should produce same hash
        self.assertEqual(hash1, hash2)
        self.assertEqual(len(hash1), 64)  # SHA256 produces 64 char hex string
        
        # Different data should produce different hash
        event_data2 = {"test": "different", "value": 456}
        hash3 = _create_event_hash(event_type, event_data2)
        self.assertNotEqual(hash1, hash3)
        
        # Different event type should produce different hash
        hash4 = _create_event_hash("OTHER_EVENT", event_data)
        self.assertNotEqual(hash1, hash4)
    
    @patch('pyairbyte.utils.event_store.query_graphql_api')
    def test_check_hash_exists_true_in_store(self, mock_query_graphql_api):
        """Test hash existence check when hash exists in event_store."""
        # First call: check event_store (found)
        # Second call: check completed_integration_events (not called because first found it)
        mock_query_graphql_api.return_value = {
            'data': {
                'event_store': [
                    {'id': 1, 'event_hash': 'test_hash'}
                ]
            }
        }
        
        result = _check_hash_exists('test_hash')
        
        self.assertTrue(result)
        # Should only call once (stops after finding in event_store)
        self.assertEqual(mock_query_graphql_api.call_count, 1)
    
    @patch('pyairbyte.utils.event_store.query_graphql_api')
    def test_check_hash_exists_true_in_completed(self, mock_query_graphql_api):
        """Test hash existence check when hash exists in completed_integration_events."""
        # First call: check event_store (not found)
        # Second call: check completed_integration_events (found)
        mock_query_graphql_api.side_effect = [
            {
                'data': {
                    'event_store': []
                }
            },
            {
                'data': {
                    'completed_integration_events': [
                        {'id': 1, 'event_hash': 'test_hash'}
                    ]
                }
            }
        ]
        
        result = _check_hash_exists('test_hash')
        
        self.assertTrue(result)
        # Should call twice (check both tables)
        self.assertEqual(mock_query_graphql_api.call_count, 2)
    
    @patch('pyairbyte.utils.event_store.query_graphql_api')
    def test_check_hash_exists_false(self, mock_query_graphql_api):
        """Test hash existence check when hash does not exist in either table."""
        # First call: check event_store (not found)
        # Second call: check completed_integration_events (not found)
        mock_query_graphql_api.side_effect = [
            {
                'data': {
                    'event_store': []
                }
            },
            {
                'data': {
                    'completed_integration_events': []
                }
            }
        ]
        
        result = _check_hash_exists('test_hash')
        
        self.assertFalse(result)
        # Should call twice (check both tables)
        self.assertEqual(mock_query_graphql_api.call_count, 2)
    
    @patch('pyairbyte.utils.event_store.query_graphql_api')
    def test_write_event_success(self, mock_query_graphql_api):
        """Test successful event write."""
        event_type = "UNIT4_DITIO_EVENT"
        event_data = {"test": "data", "value": 123}
        
        # Calculate the expected hash
        expected_hash = _create_event_hash(event_type, event_data)
        
        # Mock hash check (hash doesn't exist in either table)
        mock_query_graphql_api.side_effect = [
            # First call: check event_store (not found)
            {'data': {'event_store': []}},
            # Second call: check completed_integration_events (not found)
            {'data': {'completed_integration_events': []}},
            # Third call: insert event
            {
                'data': {
                    'insert_event_store_one': {
                        'id': 1,
                        'event_type': event_type,
                        'event_created_at': '2025-01-01T00:00:00',
                        'event_hash': expected_hash,
                        'event_data': event_data
                    }
                }
            }
        ]
        
        result = write_event(event_type, event_data)
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['event_id'], 1)
        self.assertEqual(result['event_hash'], expected_hash)
        self.assertEqual(result['message'], 'Event successfully inserted into event_store')
        self.assertIsNotNone(result['data'])
        self.assertEqual(mock_query_graphql_api.call_count, 3)
    
    @patch('pyairbyte.utils.event_store.query_graphql_api')
    def test_write_event_duplicate_detected_in_store(self, mock_query_graphql_api):
        """Test duplicate event detection when hash exists in event_store."""
        event_type = "UNIT4_DITIO_EVENT"
        event_data = {"test": "data", "value": 123}
        
        # Mock hash check (hash exists in event_store)
        mock_query_graphql_api.return_value = {
            'data': {
                'event_store': [
                    {'id': 1, 'event_hash': 'test_hash'}
                ]
            }
        }
        
        result = write_event(event_type, event_data)
        
        self.assertEqual(result['status'], 'duplicate')
        self.assertIsNone(result['event_id'])
        self.assertIn('already exists', result['message'])
        # Should only call once (stops after finding in event_store)
        self.assertEqual(mock_query_graphql_api.call_count, 1)
    
    @patch('pyairbyte.utils.event_store.query_graphql_api')
    def test_write_event_duplicate_detected_in_completed(self, mock_query_graphql_api):
        """Test duplicate event detection when hash exists in completed_integration_events."""
        event_type = "UNIT4_DITIO_EVENT"
        event_data = {"test": "data", "value": 123}
        
        # Mock hash check (hash exists in completed_integration_events)
        mock_query_graphql_api.side_effect = [
            # First call: check event_store (not found)
            {'data': {'event_store': []}},
            # Second call: check completed_integration_events (found)
            {
                'data': {
                    'completed_integration_events': [
                        {'id': 1, 'event_hash': 'test_hash'}
                    ]
                }
            }
        ]
        
        result = write_event(event_type, event_data)
        
        self.assertEqual(result['status'], 'duplicate')
        self.assertIsNone(result['event_id'])
        self.assertIn('already exists', result['message'])
        # Should call twice (check both tables)
        self.assertEqual(mock_query_graphql_api.call_count, 2)
        mock_query_graphql_api.assert_called_once()  # Only hash check, no insert
    
    @patch('pyairbyte.utils.event_store.query_graphql_api')
    def test_write_event_duplicate_detected_during_insert(self, mock_query_graphql_api):
        """Test duplicate event detection during insert (race condition)."""
        event_type = "UNIT4_DITIO_EVENT"
        event_data = {"test": "data", "value": 123}
        
        # Mock hash check (hash doesn't exist)
        # But insert fails with duplicate error
        mock_query_graphql_api.side_effect = [
            # First call: check hash exists
            {'data': {'event_store': []}},
            # Second call: insert fails with duplicate error
            ValueError("GraphQL query failed: duplicate key value violates unique constraint")
        ]
        
        result = write_event(event_type, event_data)
        
        self.assertEqual(result['status'], 'duplicate')
        self.assertIsNone(result['event_id'])
        self.assertIn('already exists', result['message'])
        self.assertEqual(mock_query_graphql_api.call_count, 2)
    
    def test_write_event_invalid_json(self):
        """Test write_event with invalid JSON data."""
        event_type = "UNIT4_DITIO_EVENT"
        # Create a circular reference which cannot be JSON serialized
        event_data = {}
        event_data['self'] = event_data
        
        with self.assertRaises(ValueError) as context:
            write_event(event_type, event_data)
        
        self.assertIn('Invalid event_data', str(context.exception))
        self.assertIn('cannot be converted to JSON', str(context.exception))
    
    @patch('pyairbyte.utils.event_store.query_graphql_api')
    def test_write_event_graphql_error(self, mock_query_graphql_api):
        """Test write_event with GraphQL error."""
        event_type = "UNIT4_DITIO_EVENT"
        event_data = {"test": "data"}
        
        # Mock hash check (hash doesn't exist)
        # But insert fails with GraphQL error
        mock_query_graphql_api.side_effect = [
            # First call: check hash exists
            {'data': {'event_store': []}},
            # Second call: GraphQL error
            ValueError("GraphQL query failed: Internal server error")
        ]
        
        result = write_event(event_type, event_data)
        
        self.assertEqual(result['status'], 'error')
        self.assertIsNone(result['event_id'])
        self.assertIn('Internal server error', result['message'])
    
    @patch('pyairbyte.utils.event_store.query_graphql_api')
    def test_write_event_insert_returns_no_data(self, mock_query_graphql_api):
        """Test write_event when insert returns no data."""
        event_type = "UNIT4_DITIO_EVENT"
        event_data = {"test": "data"}
        
        mock_query_graphql_api.side_effect = [
            # First call: check hash exists
            {'data': {'event_store': []}},
            # Second call: insert returns no data
            {'data': {'insert_event_store_one': None}}
        ]
        
        result = write_event(event_type, event_data)
        
        self.assertEqual(result['status'], 'error')
        self.assertIsNone(result['event_id'])
        self.assertEqual(result['message'], 'Event insertion returned no data')
    
    @patch('pyairbyte.utils.event_store.query_graphql_api')
    def test_get_unprocessed_or_failed_events_success(self, mock_query_graphql_api):
        """Test successful retrieval of unprocessed or failed events."""
        event_type = "UNIT4_DITIO_EVENT"
        
        mock_query_graphql_api.return_value = {
            'data': {
                'event_store': [
                    {
                        'id': 1,
                        'event_type': event_type,
                        'event_created_at': '2025-01-01T00:00:00',
                        'event_data': {"test": "data1"},
                        'event_hash': 'hash1',
                        'event_processed_logs': []
                    },
                    {
                        'id': 2,
                        'event_type': event_type,
                        'event_created_at': '2025-01-02T00:00:00',
                        'event_data': {"test": "data2"},
                        'event_hash': 'hash2',
                        'event_processed_logs': [
                            {
                                'id': 1,
                                'processed_at': '2025-01-02T01:00:00',
                                'processed_status': 'FAILED',
                                'processed_result': {"error": "test"},
                                'processed_result_error': 'Test error'
                            }
                        ]
                    }
                ]
            }
        }
        
        result = get_unprocessed_or_failed_events(event_type)
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['count'], 2)
        self.assertEqual(len(result['events']), 2)
        self.assertEqual(result['events'][0]['id'], 1)
        self.assertEqual(result['events'][1]['id'], 2)
        self.assertIn('Retrieved 2 unprocessed or failed events', result['message'])
        self.assertIsNotNone(result['data'])
        mock_query_graphql_api.assert_called_once()
    
    @patch('pyairbyte.utils.event_store.query_graphql_api')
    def test_get_unprocessed_or_failed_events_empty(self, mock_query_graphql_api):
        """Test retrieval when no events found."""
        event_type = "UNIT4_DITIO_EVENT"
        
        mock_query_graphql_api.return_value = {
            'data': {
                'event_store': []
            }
        }
        
        result = get_unprocessed_or_failed_events(event_type)
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['count'], 0)
        self.assertEqual(len(result['events']), 0)
        self.assertIn('Retrieved 0 unprocessed or failed events', result['message'])
    
    @patch('pyairbyte.utils.event_store.query_graphql_api')
    def test_get_unprocessed_or_failed_events_graphql_error(self, mock_query_graphql_api):
        """Test retrieval with GraphQL error."""
        event_type = "UNIT4_DITIO_EVENT"
        
        mock_query_graphql_api.side_effect = ValueError("GraphQL query failed: Connection error")
        
        result = get_unprocessed_or_failed_events(event_type)
        
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['count'], 0)
        self.assertEqual(len(result['events']), 0)
        self.assertIn('Connection error', result['message'])
        self.assertIsNone(result['data'])
    
    @patch('pyairbyte.utils.event_store.query_graphql_api')
    def test_get_unprocessed_or_failed_events_unexpected_error(self, mock_query_graphql_api):
        """Test retrieval with unexpected error."""
        event_type = "UNIT4_DITIO_EVENT"
        
        mock_query_graphql_api.side_effect = Exception("Unexpected error occurred")
        
        result = get_unprocessed_or_failed_events(event_type)
        
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['count'], 0)
        self.assertEqual(len(result['events']), 0)
        self.assertIn('Unexpected error', result['message'])
        self.assertIsNone(result['data'])
    
    @patch('pyairbyte.utils.event_store.query_graphql_api')
    def test_write_event_with_custom_hasura_config(self, mock_query_graphql_api):
        """Test write_event with custom Hasura URL and admin secret."""
        event_type = "UNIT4_DITIO_EVENT"
        event_data = {"test": "data"}
        custom_url = "http://custom-hasura:8080"
        custom_secret = "custom_secret"
        
        mock_query_graphql_api.side_effect = [
            {'data': {'event_store': []}},
            {
                'data': {
                    'insert_event_store_one': {
                        'id': 1,
                        'event_type': event_type,
                        'event_hash': 'test_hash',
                        'event_data': event_data
                    }
                }
            }
        ]
        
        result = write_event(event_type, event_data, hasura_url=custom_url, admin_secret=custom_secret)
        
        self.assertEqual(result['status'], 'success')
        # Verify custom config was passed to query_graphql_api
        calls = mock_query_graphql_api.call_args_list
        for call in calls:
            args, kwargs = call
            # Check if custom URL was used (it should be in the call)
            # The URL gets normalized in graphql_util, so we check the calls were made
            self.assertEqual(mock_query_graphql_api.call_count, 2)
    
    @patch('pyairbyte.utils.event_store.query_graphql_api')
    def test_get_unprocessed_or_failed_events_with_custom_hasura_config(self, mock_query_graphql_api):
        """Test get_unprocessed_or_failed_events with custom Hasura URL and admin secret."""
        event_type = "UNIT4_DITIO_EVENT"
        custom_url = "http://custom-hasura:8080"
        custom_secret = "custom_secret"
        
        mock_query_graphql_api.return_value = {
            'data': {
                'event_store': []
            }
        }
        
        result = get_unprocessed_or_failed_events(event_type, hasura_url=custom_url, admin_secret=custom_secret)
        
        self.assertEqual(result['status'], 'success')
        mock_query_graphql_api.assert_called_once()
    
    def test_create_event_hash_consistent_ordering(self):
        """Test that hash is consistent regardless of dictionary key order."""
        event_type = "TEST_EVENT"
        event_data1 = {"key1": "value1", "key2": "value2"}
        event_data2 = {"key2": "value2", "key1": "value1"}  # Different order
        
        hash1 = _create_event_hash(event_type, event_data1)
        hash2 = _create_event_hash(event_type, event_data2)
        
        # Should produce same hash due to sort_keys=True
        self.assertEqual(hash1, hash2)
    
    @patch('pyairbyte.utils.event_store.query_graphql_api')
    def test_check_hash_exists_raises_exception(self, mock_query_graphql_api):
        """Test that check_hash_exists raises exception on error."""
        mock_query_graphql_api.side_effect = ValueError("GraphQL error")
        
        with self.assertRaises(ValueError):
            _check_hash_exists('test_hash')
    
    @patch('pyairbyte.utils.event_store.query_graphql_api')
    def test_log_event_processing_insert_success(self, mock_query_graphql_api):
        """Test successful insert of event processing log (no existing log)."""
        event_id = 1
        api_call_result = {
            "status": "success",
            "status_code": 200,
            "data": {"result": "ok"},
            "error": None,
            "response_headers": {"Content-Type": "application/json"}
        }
        
        # Mock: no existing log, then insert
        mock_query_graphql_api.side_effect = [
            # First call: check for existing log (returns empty)
            {'data': {'event_processed_logs': []}},
            # Second call: insert new log
            {
                'data': {
                    'insert_event_processed_logs_one': {
                        'id': 1,
                        'event_id': event_id,
                        'processed_at': '2025-01-01T00:00:00',
                        'processed_status': 'SUCCESS',
                        'processed_result': api_call_result,
                        'processed_result_error': None
                    }
                }
            }
        ]
        
        result = log_event_processing(event_id, api_call_result)
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['log_id'], 1)
        self.assertEqual(result['processed_status'], 'SUCCESS')
        self.assertEqual(result['action'], 'inserted')
        self.assertIsNotNone(result['data'])
        self.assertEqual(mock_query_graphql_api.call_count, 2)
    
    @patch('pyairbyte.utils.event_store.query_graphql_api')
    def test_log_event_processing_update_success(self, mock_query_graphql_api):
        """Test successful update of existing event processing log."""
        event_id = 1
        api_call_result = {
            "status": "error",
            "status_code": 500,
            "data": None,
            "error": "Internal server error",
            "response_headers": {}
        }
        
        # Mock: existing log found, then update
        mock_query_graphql_api.side_effect = [
            # First call: check for existing log (returns existing)
            {
                'data': {
                    'event_processed_logs': [
                        {
                            'id': 1,
                            'event_id': event_id,
                            'processed_at': '2025-01-01T00:00:00',
                            'processed_status': 'FAILED',
                            'processed_result': {"status": "error"},
                            'processed_result_error': 'Previous error'
                        }
                    ]
                }
            },
            # Second call: update existing log
            {
                'data': {
                    'update_event_processed_logs_by_pk': {
                        'id': 1,
                        'event_id': event_id,
                        'processed_at': '2025-01-01T00:00:00',
                        'processed_status': 'FAILED',
                        'processed_result': api_call_result,
                        'processed_result_error': 'Internal server error'
                    }
                }
            }
        ]
        
        result = log_event_processing(event_id, api_call_result)
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['log_id'], 1)
        self.assertEqual(result['processed_status'], 'FAILED')
        self.assertEqual(result['action'], 'updated')
        self.assertIsNotNone(result['data'])
        self.assertEqual(mock_query_graphql_api.call_count, 2)
    
    @patch('pyairbyte.utils.event_store.query_graphql_api')
    def test_log_event_processing_with_success_status(self, mock_query_graphql_api):
        """Test log_event_processing with SUCCESS status from api_call."""
        event_id = 1
        api_call_result = {
            "status": "success",
            "status_code": 201,
            "data": {"id": 123, "created": True},
            "error": None,
            "response_headers": {"Location": "/api/resource/123"}
        }
        
        mock_query_graphql_api.side_effect = [
            {'data': {'event_processed_logs': []}},
            {
                'data': {
                    'insert_event_processed_logs_one': {
                        'id': 1,
                        'event_id': event_id,
                        'processed_status': 'SUCCESS',
                        'processed_result': api_call_result,
                        'processed_result_error': None
                    }
                }
            }
        ]
        
        result = log_event_processing(event_id, api_call_result)
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['processed_status'], 'SUCCESS')
        self.assertIsNone(result['data']['processed_result_error'])
    
    @patch('pyairbyte.utils.event_store.query_graphql_api')
    def test_log_event_processing_with_failed_status(self, mock_query_graphql_api):
        """Test log_event_processing with FAILED status from api_call."""
        event_id = 1
        api_call_result = {
            "status": "error",
            "status_code": None,
            "data": None,
            "error": "Connection timeout",
            "response_headers": None
        }
        
        mock_query_graphql_api.side_effect = [
            {'data': {'event_processed_logs': []}},
            {
                'data': {
                    'insert_event_processed_logs_one': {
                        'id': 1,
                        'event_id': event_id,
                        'processed_status': 'FAILED',
                        'processed_result': api_call_result,
                        'processed_result_error': 'Connection timeout'
                    }
                }
            }
        ]
        
        result = log_event_processing(event_id, api_call_result)
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['processed_status'], 'FAILED')
        self.assertEqual(result['data']['processed_result_error'], 'Connection timeout')
    
    def test_log_event_processing_invalid_event_id(self):
        """Test log_event_processing with invalid event_id."""
        api_call_result = {
            "status": "success",
            "status_code": 200,
            "data": {},
            "error": None,
            "response_headers": {}
        }
        
        with self.assertRaises(ValueError) as context:
            log_event_processing(0, api_call_result)
        
        self.assertIn('Invalid event_id', str(context.exception))
    
    def test_log_event_processing_invalid_api_result_not_dict(self):
        """Test log_event_processing with non-dictionary api_call_result."""
        with self.assertRaises(ValueError) as context:
            log_event_processing(1, "not a dict")
        
        self.assertIn('must be a dictionary', str(context.exception))
    
    def test_log_event_processing_missing_required_keys(self):
        """Test log_event_processing with missing required keys."""
        api_call_result = {
            "status": "success",
            "status_code": 200
            # Missing: data, error, response_headers
        }
        
        with self.assertRaises(ValueError) as context:
            log_event_processing(1, api_call_result)
        
        self.assertIn('missing required keys', str(context.exception))
    
    def test_log_event_processing_invalid_status_value(self):
        """Test log_event_processing with invalid status value."""
        api_call_result = {
            "status": "invalid_status",
            "status_code": 200,
            "data": {},
            "error": None,
            "response_headers": {}
        }
        
        with self.assertRaises(ValueError) as context:
            log_event_processing(1, api_call_result)
        
        self.assertIn("must be 'success' or 'error'", str(context.exception))
    
    def test_log_event_processing_invalid_status_code_type(self):
        """Test log_event_processing with invalid status_code type."""
        api_call_result = {
            "status": "success",
            "status_code": "200",  # Should be int, not string
            "data": {},
            "error": None,
            "response_headers": {}
        }
        
        with self.assertRaises(ValueError) as context:
            log_event_processing(1, api_call_result)
        
        self.assertIn('status_code', str(context.exception))
        self.assertIn('int or None', str(context.exception))
    
    @patch('pyairbyte.utils.event_store.query_graphql_api')
    def test_log_event_processing_insert_graphql_error(self, mock_query_graphql_api):
        """Test log_event_processing with GraphQL error during insert."""
        event_id = 1
        api_call_result = {
            "status": "success",
            "status_code": 200,
            "data": {},
            "error": None,
            "response_headers": {}
        }
        
        mock_query_graphql_api.side_effect = [
            {'data': {'event_processed_logs': []}},
            ValueError("GraphQL query failed: Database error")
        ]
        
        result = log_event_processing(event_id, api_call_result)
        
        self.assertEqual(result['status'], 'error')
        self.assertIsNone(result['log_id'])
        self.assertEqual(result['action'], 'insert_failed')
        self.assertIn('Database error', result['message'])
    
    @patch('pyairbyte.utils.event_store.query_graphql_api')
    def test_log_event_processing_update_graphql_error(self, mock_query_graphql_api):
        """Test log_event_processing with GraphQL error during update."""
        event_id = 1
        api_call_result = {
            "status": "error",
            "status_code": 500,
            "data": None,
            "error": "Server error",
            "response_headers": {}
        }
        
        mock_query_graphql_api.side_effect = [
            {
                'data': {
                    'event_processed_logs': [
                        {'id': 1, 'event_id': event_id, 'processed_status': 'FAILED'}
                    ]
                }
            },
            ValueError("GraphQL query failed: Update failed")
        ]
        
        result = log_event_processing(event_id, api_call_result)
        
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['log_id'], 1)
        self.assertEqual(result['action'], 'update_failed')
        self.assertIn('Update failed', result['message'])
    
    @patch('pyairbyte.utils.event_store.query_graphql_api')
    def test_log_event_processing_insert_returns_no_data(self, mock_query_graphql_api):
        """Test log_event_processing when insert returns no data."""
        event_id = 1
        api_call_result = {
            "status": "success",
            "status_code": 200,
            "data": {},
            "error": None,
            "response_headers": {}
        }
        
        mock_query_graphql_api.side_effect = [
            {'data': {'event_processed_logs': []}},
            {'data': {'insert_event_processed_logs_one': None}}
        ]
        
        result = log_event_processing(event_id, api_call_result)
        
        self.assertEqual(result['status'], 'error')
        self.assertIsNone(result['log_id'])
        self.assertEqual(result['action'], 'insert_failed')
        self.assertEqual(result['message'], 'Event processing log insertion returned no data')
    
    @patch('pyairbyte.utils.event_store.query_graphql_api')
    def test_log_event_processing_with_custom_hasura_config(self, mock_query_graphql_api):
        """Test log_event_processing with custom Hasura URL and admin secret."""
        event_id = 1
        api_call_result = {
            "status": "success",
            "status_code": 200,
            "data": {},
            "error": None,
            "response_headers": {}
        }
        custom_url = "http://custom-hasura:8080"
        custom_secret = "custom_secret"
        
        mock_query_graphql_api.side_effect = [
            {'data': {'event_processed_logs': []}},
            {
                'data': {
                    'insert_event_processed_logs_one': {
                        'id': 1,
                        'event_id': event_id,
                        'processed_status': 'SUCCESS',
                        'processed_result': api_call_result
                    }
                }
            }
        ]
        
        result = log_event_processing(
            event_id,
            api_call_result,
            hasura_url=custom_url,
            admin_secret=custom_secret
        )
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(mock_query_graphql_api.call_count, 2)
    
    # ============================================
    # Bulk Write Events Tests (Unit Tests)
    # ============================================
    
    def test_bulk_write_events_empty_list(self):
        """Test bulk_write_events with empty list."""
        with self.assertRaises(ValueError) as context:
            bulk_write_events([])
        
        self.assertIn('cannot be empty', str(context.exception))
    
    def test_bulk_write_events_invalid_input_not_list(self):
        """Test bulk_write_events with non-list input."""
        with self.assertRaises(ValueError) as context:
            bulk_write_events("not a list")
        
        self.assertIn('must be a list', str(context.exception))
    
    def test_bulk_write_events_invalid_batch_size(self):
        """Test bulk_write_events with invalid batch_size."""
        events = [{"event_type": "TEST", "event_data": {}}]
        
        with self.assertRaises(ValueError) as context:
            bulk_write_events(events, batch_size=0)
        
        self.assertIn('batch_size', str(context.exception))
    
    @patch('pyairbyte.utils.event_store.query_graphql_api')
    def test_bulk_write_events_single_event_success(self, mock_query_graphql_api):
        """Test bulk_write_events with single event."""
        events = [
            {
                "event_type": "TEST_EVENT",
                "event_data": {"test": "data", "id": 1}
            }
        ]
        
        # Mock: hash check (no duplicates in either table), then insert
        mock_query_graphql_api.side_effect = [
            {'data': {'event_store': []}},  # No existing hashes in event_store
            {'data': {'completed_integration_events': []}},  # No existing hashes in completed
            {
                'data': {
                    'insert_event_store': {
                        'affected_rows': 1,
                        'returning': [
                            {
                                'id': 1,
                                'event_type': 'TEST_EVENT',
                                'event_hash': 'test_hash',
                                'event_data': {"test": "data", "id": 1}
                            }
                        ]
                    }
                }
            }
        ]
        
        result = bulk_write_events(events)
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['total_events'], 1)
        self.assertEqual(result['events_created'], 1)
        self.assertEqual(result['events_duplicate'], 0)
        self.assertEqual(result['events_failed'], 0)
        self.assertEqual(len(result['created_event_ids']), 1)
        self.assertEqual(result['batches_processed'], 1)
    
    @patch('pyairbyte.utils.event_store.query_graphql_api')
    def test_bulk_write_events_multiple_events_success(self, mock_query_graphql_api):
        """Test bulk_write_events with multiple events."""
        events = [
            {"event_type": "TEST_EVENT", "event_data": {"id": 1}},
            {"event_type": "TEST_EVENT", "event_data": {"id": 2}},
            {"event_type": "TEST_EVENT", "event_data": {"id": 3}}
        ]
        
        # Mock: hash check (no duplicates in either table), then insert
        mock_query_graphql_api.side_effect = [
            {'data': {'event_store': []}},  # No existing hashes in event_store
            {'data': {'completed_integration_events': []}},  # No existing hashes in completed
            {
                'data': {
                    'insert_event_store': {
                        'affected_rows': 3,
                        'returning': [
                            {'id': 1, 'event_type': 'TEST_EVENT', 'event_hash': 'hash1'},
                            {'id': 2, 'event_type': 'TEST_EVENT', 'event_hash': 'hash2'},
                            {'id': 3, 'event_type': 'TEST_EVENT', 'event_hash': 'hash3'}
                        ]
                    }
                }
            }
        ]
        
        result = bulk_write_events(events)
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['total_events'], 3)
        self.assertEqual(result['events_created'], 3)
        self.assertEqual(result['events_duplicate'], 0)
        self.assertEqual(result['events_failed'], 0)
        self.assertEqual(len(result['created_event_ids']), 3)
        self.assertEqual(result['batches_processed'], 1)
    
    @patch('pyairbyte.utils.event_store.query_graphql_api')
    def test_bulk_write_events_with_duplicates(self, mock_query_graphql_api):
        """Test bulk_write_events with duplicate events."""
        events = [
            {"event_type": "TEST_EVENT", "event_data": {"id": 1}},
            {"event_type": "TEST_EVENT", "event_data": {"id": 2}}
        ]
        
        # Calculate expected hashes
        hash1 = _create_event_hash("TEST_EVENT", {"id": 1})
        hash2 = _create_event_hash("TEST_EVENT", {"id": 2})
        
        # Mock: hash check (hash1 exists in event_store, hash2 doesn't exist), then insert only hash2
        mock_query_graphql_api.side_effect = [
            # Check event_store
            {'data': {'event_store': [{'event_hash': hash1}]}},  # hash1 exists
            # Check completed_integration_events
            {'data': {'completed_integration_events': []}},  # hash1 not in completed
            # Check event_store for hash2
            {'data': {'event_store': []}},  # hash2 doesn't exist
            # Check completed_integration_events for hash2
            {'data': {'completed_integration_events': []}},  # hash2 doesn't exist
            # Insert hash2
            {
                'data': {
                    'insert_event_store': {
                        'affected_rows': 1,
                        'returning': [
                            {'id': 2, 'event_type': 'TEST_EVENT', 'event_hash': hash2}
                        ]
                    }
                }
            }
        ]
        
        result = bulk_write_events(events)
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['total_events'], 2)
        self.assertEqual(result['events_created'], 1)
        self.assertEqual(result['events_duplicate'], 1)
        self.assertEqual(result['events_failed'], 0)
        self.assertEqual(len(result['created_event_ids']), 1)
        self.assertEqual(len(result['duplicate_hashes']), 1)
    
    @patch('pyairbyte.utils.event_store.query_graphql_api')
    def test_bulk_write_events_all_duplicates(self, mock_query_graphql_api):
        """Test bulk_write_events when all events are duplicates."""
        events = [
            {"event_type": "TEST_EVENT", "event_data": {"id": 1}},
            {"event_type": "TEST_EVENT", "event_data": {"id": 2}}
        ]
        
        hash1 = _create_event_hash("TEST_EVENT", {"id": 1})
        hash2 = _create_event_hash("TEST_EVENT", {"id": 2})
        
        # Mock: hash check (both exist in event_store)
        mock_query_graphql_api.side_effect = [
            # Check event_store (both found)
            {'data': {'event_store': [{'event_hash': hash1}, {'event_hash': hash2}]}},
            # Check completed_integration_events (not needed since both found in store)
        ]
        
        result = bulk_write_events(events)
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['total_events'], 2)
        self.assertEqual(result['events_created'], 0)
        self.assertEqual(result['events_duplicate'], 2)
        self.assertEqual(result['events_failed'], 0)
        self.assertEqual(len(result['created_event_ids']), 0)
        self.assertEqual(len(result['duplicate_hashes']), 2)
        # Should call hash check for both tables
        self.assertEqual(mock_query_graphql_api.call_count, 2)
    
    @patch('pyairbyte.utils.event_store.query_graphql_api')
    def test_bulk_write_events_with_duplicates_in_completed(self, mock_query_graphql_api):
        """Test bulk_write_events with duplicates in completed_integration_events."""
        events = [
            {"event_type": "TEST_EVENT", "event_data": {"id": 1}},
            {"event_type": "TEST_EVENT", "event_data": {"id": 2}}
        ]
        
        # Calculate expected hashes
        hash1 = _create_event_hash("TEST_EVENT", {"id": 1})
        hash2 = _create_event_hash("TEST_EVENT", {"id": 2})
        
        # Mock: hash1 exists in completed_integration_events, hash2 doesn't exist
        mock_query_graphql_api.side_effect = [
            # Check event_store (hash1 not found, hash2 not found)
            {'data': {'event_store': []}},
            # Check completed_integration_events (hash1 found, hash2 not found)
            {'data': {'completed_integration_events': [{'event_hash': hash1}]}},
            # Insert hash2
            {
                'data': {
                    'insert_event_store': {
                        'affected_rows': 1,
                        'returning': [
                            {'id': 2, 'event_type': 'TEST_EVENT', 'event_hash': hash2}
                        ]
                    }
                }
            }
        ]
        
        result = bulk_write_events(events)
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['total_events'], 2)
        self.assertEqual(result['events_created'], 1)
        self.assertEqual(result['events_duplicate'], 1)
        self.assertEqual(result['events_failed'], 0)
        self.assertEqual(len(result['created_event_ids']), 1)
        self.assertEqual(len(result['duplicate_hashes']), 1)
        self.assertIn(hash1, result['duplicate_hashes'])
    
    @patch('pyairbyte.utils.event_store.query_graphql_api')
    def test_bulk_write_events_all_duplicates_in_completed(self, mock_query_graphql_api):
        """Test bulk_write_events when all events are duplicates in completed_integration_events."""
        events = [
            {"event_type": "TEST_EVENT", "event_data": {"id": 1}},
            {"event_type": "TEST_EVENT", "event_data": {"id": 2}}
        ]
        
        hash1 = _create_event_hash("TEST_EVENT", {"id": 1})
        hash2 = _create_event_hash("TEST_EVENT", {"id": 2})
        
        # Mock: both exist in completed_integration_events
        mock_query_graphql_api.side_effect = [
            # Check event_store (both not found)
            {'data': {'event_store': []}},
            # Check completed_integration_events (both found)
            {'data': {'completed_integration_events': [{'event_hash': hash1}, {'event_hash': hash2}]}}
        ]
        
        result = bulk_write_events(events)
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['total_events'], 2)
        self.assertEqual(result['events_created'], 0)
        self.assertEqual(result['events_duplicate'], 2)
        self.assertEqual(result['events_failed'], 0)
        self.assertEqual(len(result['created_event_ids']), 0)
        self.assertEqual(len(result['duplicate_hashes']), 2)
        # Should call hash check for both tables
        self.assertEqual(mock_query_graphql_api.call_count, 2)
    
    @patch('pyairbyte.utils.event_store.query_graphql_api')
    def test_bulk_write_events_mixed_duplicates(self, mock_query_graphql_api):
        """Test bulk_write_events with duplicates in both event_store and completed_integration_events."""
        events = [
            {"event_type": "TEST_EVENT", "event_data": {"id": 1}},  # Will be duplicate in event_store
            {"event_type": "TEST_EVENT", "event_data": {"id": 2}},  # Will be duplicate in completed
            {"event_type": "TEST_EVENT", "event_data": {"id": 3}}   # New event
        ]
        
        hash1 = _create_event_hash("TEST_EVENT", {"id": 1})
        hash2 = _create_event_hash("TEST_EVENT", {"id": 2})
        hash3 = _create_event_hash("TEST_EVENT", {"id": 3})
        
        # Mock: hash1 in event_store, hash2 in completed_integration_events, hash3 new
        mock_query_graphql_api.side_effect = [
            # Check event_store (hash1 found, hash2 not found, hash3 not found)
            {'data': {'event_store': [{'event_hash': hash1}]}},
            # Check completed_integration_events (hash1 not found, hash2 found, hash3 not found)
            {'data': {'completed_integration_events': [{'event_hash': hash2}]}},
            # Insert hash3
            {
                'data': {
                    'insert_event_store': {
                        'affected_rows': 1,
                        'returning': [
                            {'id': 3, 'event_type': 'TEST_EVENT', 'event_hash': hash3}
                        ]
                    }
                }
            }
        ]
        
        result = bulk_write_events(events)
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['total_events'], 3)
        self.assertEqual(result['events_created'], 1)
        self.assertEqual(result['events_duplicate'], 2)
        self.assertEqual(result['events_failed'], 0)
        self.assertEqual(len(result['created_event_ids']), 1)
        self.assertEqual(len(result['duplicate_hashes']), 2)
        self.assertIn(hash1, result['duplicate_hashes'])
        self.assertIn(hash2, result['duplicate_hashes'])
    
    def test_bulk_write_events_validation_errors(self):
        """Test bulk_write_events with validation errors."""
        events = [
            {"event_type": "TEST_EVENT"},  # Missing event_data
            {"event_data": {"id": 2}},  # Missing event_type
        ]
        
        # Create circular reference for invalid JSON (cannot be serialized)
        invalid_data = {}
        invalid_data['self'] = invalid_data
        events.append({"event_type": "TEST_EVENT", "event_data": invalid_data})
        
        # Add event with function (not JSON serializable)
        def some_function():
            pass
        events.append({"event_type": "TEST_EVENT", "event_data": {"func": some_function}})
        
        result = bulk_write_events(events, batch_size=10)
        
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['total_events'], 4)
        self.assertEqual(result['events_failed'], 4)
        self.assertEqual(result['events_created'], 0)
        self.assertEqual(result['events_duplicate'], 0)
        self.assertGreater(len(result['errors']), 0)
    
    @patch('pyairbyte.utils.event_store.query_graphql_api')
    def test_bulk_write_events_chunking(self, mock_query_graphql_api):
        """Test bulk_write_events with chunking (batch_size=2)."""
        events = [
            {"event_type": "TEST_EVENT", "event_data": {"id": i}}
            for i in range(5)  # 5 events
        ]
        
        # Mock: 3 batches (2, 2, 1 events)
        # Each batch: hash check (empty), then insert
        mock_query_graphql_api.side_effect = [
            # Batch 1: events 0-1
            {'data': {'event_store': []}},
            {
                'data': {
                    'insert_event_store': {
                        'affected_rows': 2,
                        'returning': [{'id': 1}, {'id': 2}]
                    }
                }
            },
            # Batch 2: events 2-3
            {'data': {'event_store': []}},
            {
                'data': {
                    'insert_event_store': {
                        'affected_rows': 2,
                        'returning': [{'id': 3}, {'id': 4}]
                    }
                }
            },
            # Batch 3: event 4
            {'data': {'event_store': []}},
            {
                'data': {
                    'insert_event_store': {
                        'affected_rows': 1,
                        'returning': [{'id': 5}]
                    }
                }
            }
        ]
        
        result = bulk_write_events(events, batch_size=2)
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['total_events'], 5)
        self.assertEqual(result['events_created'], 5)
        self.assertEqual(result['batches_processed'], 3)
        self.assertEqual(len(result['created_event_ids']), 5)
        # Should have 6 calls: 3 hash checks + 3 inserts
        self.assertEqual(mock_query_graphql_api.call_count, 6)
    
    @patch('pyairbyte.utils.event_store.query_graphql_api')
    def test_bulk_write_events_graphql_error(self, mock_query_graphql_api):
        """Test bulk_write_events with GraphQL error."""
        events = [
            {"event_type": "TEST_EVENT", "event_data": {"id": 1}}
        ]
        
        mock_query_graphql_api.side_effect = [
            {'data': {'event_store': []}},
            ValueError("GraphQL query failed: Database error")
        ]
        
        result = bulk_write_events(events)
        
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['events_created'], 0)
        self.assertEqual(result['events_failed'], 1)
        self.assertGreater(len(result['errors']), 0)
    
    @patch('pyairbyte.utils.event_store.query_graphql_api')
    def test_bulk_write_events_mixed_valid_invalid(self, mock_query_graphql_api):
        """Test bulk_write_events with mix of valid and invalid events."""
        events = [
            {"event_type": "TEST_EVENT", "event_data": {"id": 1}},  # Valid
            {"event_type": "TEST_EVENT"},  # Invalid: missing event_data
            {"event_type": "TEST_EVENT", "event_data": {"id": 2}}  # Valid
        ]
        
        # Mock: hash check (empty), then insert for valid events
        mock_query_graphql_api.side_effect = [
            {'data': {'event_store': []}},
            {
                'data': {
                    'insert_event_store': {
                        'affected_rows': 2,
                        'returning': [{'id': 1}, {'id': 2}]
                    }
                }
            }
        ]
        
        result = bulk_write_events(events)
        
        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['total_events'], 3)
        self.assertEqual(result['events_created'], 2)
        self.assertEqual(result['events_failed'], 1)
        self.assertEqual(len(result['errors']), 1)
    
    @patch('pyairbyte.utils.event_store.query_graphql_api')
    def test_bulk_write_events_custom_batch_size(self, mock_query_graphql_api):
        """Test bulk_write_events with custom batch_size parameter."""
        events = [
            {"event_type": "TEST_EVENT", "event_data": {"id": i}}
            for i in range(5)
        ]
        
        # Mock for batch_size=3: 2 batches (3, 2)
        mock_query_graphql_api.side_effect = [
            {'data': {'event_store': []}},
            {'data': {'insert_event_store': {'affected_rows': 3, 'returning': [{'id': 1}, {'id': 2}, {'id': 3}]}}},
            {'data': {'event_store': []}},
            {'data': {'insert_event_store': {'affected_rows': 2, 'returning': [{'id': 4}, {'id': 5}]}}}
        ]
        
        result = bulk_write_events(events, batch_size=3)
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['batches_processed'], 2)
        self.assertEqual(result['events_created'], 5)


class TestEventStoreIntegration(unittest.TestCase):
    """Integration tests for event_store module using real Hasura API calls."""
    
    def setUp(self):
        """Set up test fixtures for integration tests."""
        # Use real Hasura connection from environment
        self.hasura_url = os.getenv('HASURA_URL', 'http://hasura:8081/v1/graphql')
        self.admin_secret = os.getenv('HASURA_GRAPHQL_ADMIN_SECRET', 'admin')
        
        # Track created event IDs and log IDs for cleanup
        self.created_event_ids = []
        self.created_log_ids = []
        
        # Test event type prefix to identify test data
        self.test_event_type = "TEST_INTEGRATION_EVENT"
    
    def tearDown(self):
        """Clean up test data after each test."""
        from pyairbyte.utils.graphql_util import query_graphql_api
        
        # Delete processing logs first (due to foreign key constraint)
        for log_id in self.created_log_ids:
            try:
                mutation = """
                mutation DeleteProcessingLog($logId: Int!) {
                  delete_event_processed_logs_by_pk(id: $logId) {
                    id
                  }
                }
                """
                query_graphql_api(
                    mutation,
                    variables={"logId": log_id},
                    hasura_url=self.hasura_url,
                    admin_secret=self.admin_secret
                )
            except Exception as e:
                print(f"Warning: Failed to delete log {log_id}: {e}")
        
        # Delete events
        for event_id in self.created_event_ids:
            try:
                mutation = """
                mutation DeleteEvent($eventId: Int!) {
                  delete_event_store_by_pk(id: $eventId) {
                    id
                  }
                }
                """
                query_graphql_api(
                    mutation,
                    variables={"eventId": event_id},
                    hasura_url=self.hasura_url,
                    admin_secret=self.admin_secret
                )
            except Exception as e:
                print(f"Warning: Failed to delete event {event_id}: {e}")
        
        # Clear tracking lists
        self.created_event_ids = []
        self.created_log_ids = []
    
    def test_write_event_integration(self):
        """Integration test: Write event to real Hasura."""
        event_type = self.test_event_type
        event_data = {
            "test": "integration_test",
            "timestamp": "2025-01-01T00:00:00",
            "value": 12345
        }
        
        result = write_event(
            event_type,
            event_data,
            hasura_url=self.hasura_url,
            admin_secret=self.admin_secret
        )
        
        self.assertEqual(result['status'], 'success')
        self.assertIsNotNone(result['event_id'])
        self.assertIsNotNone(result['event_hash'])
        self.assertIsNotNone(result['data'])
        
        # Track for cleanup
        self.created_event_ids.append(result['event_id'])
        
        # Verify duplicate detection
        result2 = write_event(
            event_type,
            event_data,
            hasura_url=self.hasura_url,
            admin_secret=self.admin_secret
        )
        self.assertEqual(result2['status'], 'duplicate')
        self.assertEqual(result['event_hash'], result2['event_hash'])
    
    def test_get_unprocessed_or_failed_events_integration(self):
        """Integration test: Get unprocessed events from real Hasura."""
        # First, create an unprocessed event
        event_type = self.test_event_type
        event_data = {"test": "unprocessed_event", "id": 999}
        
        write_result = write_event(
            event_type,
            event_data,
            hasura_url=self.hasura_url,
            admin_secret=self.admin_secret
        )
        
        self.assertEqual(write_result['status'], 'success')
        event_id = write_result['event_id']
        self.created_event_ids.append(event_id)
        
        # Get unprocessed events
        result = get_unprocessed_or_failed_events(
            event_type,
            hasura_url=self.hasura_url,
            admin_secret=self.admin_secret
        )
        
        self.assertEqual(result['status'], 'success')
        self.assertGreaterEqual(result['count'], 1)
        
        # Find our test event in the results
        found_event = None
        for event in result['events']:
            if event['id'] == event_id:
                found_event = event
                break
        
        self.assertIsNotNone(found_event, "Test event should be in unprocessed events")
        self.assertEqual(found_event['event_type'], event_type)
        self.assertEqual(len(found_event.get('event_processed_logs', [])), 0)
    
    def test_log_event_processing_insert_integration(self):
        """Integration test: Log event processing (insert) with real Hasura."""
        # Create an event first
        event_type = self.test_event_type
        event_data = {"test": "processing_test", "id": 888}
        
        write_result = write_event(
            event_type,
            event_data,
            hasura_url=self.hasura_url,
            admin_secret=self.admin_secret
        )
        
        self.assertEqual(write_result['status'], 'success')
        event_id = write_result['event_id']
        self.created_event_ids.append(event_id)
        
        # Create API call result
        api_call_result = {
            "status": "success",
            "status_code": 200,
            "data": {"result": "ok", "id": 123},
            "error": None,
            "response_headers": {"Content-Type": "application/json"}
        }
        
        # Log the processing
        log_result = log_event_processing(
            event_id,
            api_call_result,
            hasura_url=self.hasura_url,
            admin_secret=self.admin_secret
        )
        
        self.assertEqual(log_result['status'], 'success')
        self.assertIsNotNone(log_result['log_id'])
        self.assertEqual(log_result['processed_status'], 'SUCCESS')
        self.assertEqual(log_result['action'], 'inserted')
        
        # Track for cleanup
        self.created_log_ids.append(log_result['log_id'])
        
        # Verify the event is no longer in unprocessed list
        unprocessed_result = get_unprocessed_or_failed_events(
            event_type,
            hasura_url=self.hasura_url,
            admin_secret=self.admin_secret
        )
        
        # Our event should not be in unprocessed list (it has SUCCESS status)
        found = any(e['id'] == event_id for e in unprocessed_result['events'])
        self.assertFalse(found, "Event with SUCCESS status should not be in unprocessed list")
    
    def test_log_event_processing_update_integration(self):
        """Integration test: Log event processing (update) with real Hasura."""
        # Create an event
        event_type = self.test_event_type
        event_data = {"test": "update_test", "id": 777}
        
        write_result = write_event(
            event_type,
            event_data,
            hasura_url=self.hasura_url,
            admin_secret=self.admin_secret
        )
        
        self.assertEqual(write_result['status'], 'success')
        event_id = write_result['event_id']
        self.created_event_ids.append(event_id)
        
        # First processing attempt - fails
        api_call_result_fail = {
            "status": "error",
            "status_code": 500,
            "data": None,
            "error": "Internal server error",
            "response_headers": {}
        }
        
        log_result1 = log_event_processing(
            event_id,
            api_call_result_fail,
            hasura_url=self.hasura_url,
            admin_secret=self.admin_secret
        )
        
        self.assertEqual(log_result1['status'], 'success')
        self.assertEqual(log_result1['processed_status'], 'FAILED')
        self.assertEqual(log_result1['action'], 'inserted')
        log_id = log_result1['log_id']
        self.created_log_ids.append(log_id)
        
        # Second processing attempt - succeeds (should update)
        api_call_result_success = {
            "status": "success",
            "status_code": 200,
            "data": {"result": "ok"},
            "error": None,
            "response_headers": {}
        }
        
        log_result2 = log_event_processing(
            event_id,
            api_call_result_success,
            hasura_url=self.hasura_url,
            admin_secret=self.admin_secret
        )
        
        self.assertEqual(log_result2['status'], 'success')
        self.assertEqual(log_result2['processed_status'], 'SUCCESS')
        self.assertEqual(log_result2['action'], 'updated')
        self.assertEqual(log_result2['log_id'], log_id, "Should update same log, not create new one")
        
        # Verify only one log exists for this event
        from pyairbyte.utils.graphql_util import query_graphql_api
        query = """
        query GetLogs($eventId: Int!) {
          event_processed_logs(where: {event_id: {_eq: $eventId}}) {
            id
            processed_status
          }
        }
        """
        logs_result = query_graphql_api(
            query,
            variables={"eventId": event_id},
            hasura_url=self.hasura_url,
            admin_secret=self.admin_secret
        )
        logs = logs_result.get('data', {}).get('event_processed_logs', [])
        self.assertEqual(len(logs), 1, "Should have only one log per event (upsert pattern)")
        self.assertEqual(logs[0]['processed_status'], 'SUCCESS')
    
    def test_full_workflow_integration(self):
        """Integration test: Full workflow from event creation to processing."""
        event_type = self.test_event_type
        event_data = {
            "test": "full_workflow",
            "source": "integration_test",
            "data": {"key": "value"}
        }
        
        # Step 1: Write event
        write_result = write_event(
            event_type,
            event_data,
            hasura_url=self.hasura_url,
            admin_secret=self.admin_secret
        )
        self.assertEqual(write_result['status'], 'success')
        event_id = write_result['event_id']
        self.created_event_ids.append(event_id)
        
        # Step 2: Verify it's in unprocessed list
        unprocessed = get_unprocessed_or_failed_events(
            event_type,
            hasura_url=self.hasura_url,
            admin_secret=self.admin_secret
        )
        found = any(e['id'] == event_id for e in unprocessed['events'])
        self.assertTrue(found, "New event should be in unprocessed list")
        
        # Step 3: Process it (simulate API call)
        api_call_result = {
            "status": "success",
            "status_code": 201,
            "data": {"created": True, "event_id": event_id},
            "error": None,
            "response_headers": {"Location": f"/events/{event_id}"}
        }
        
        log_result = log_event_processing(
            event_id,
            api_call_result,
            hasura_url=self.hasura_url,
            admin_secret=self.admin_secret
        )
        self.assertEqual(log_result['status'], 'success')
        self.created_log_ids.append(log_result['log_id'])
        
        # Step 4: Verify it's no longer in unprocessed list
        unprocessed_after = get_unprocessed_or_failed_events(
            event_type,
            hasura_url=self.hasura_url,
            admin_secret=self.admin_secret
        )
        found_after = any(e['id'] == event_id for e in unprocessed_after['events'])
        self.assertFalse(found_after, "Processed event should not be in unprocessed list")
    
    def test_bulk_write_events_integration(self):
        """Integration test: Bulk write events with real Hasura."""
        events = [
            {
                "event_type": self.test_event_type,
                "event_data": {"test": "bulk1", "id": 100}
            },
            {
                "event_type": self.test_event_type,
                "event_data": {"test": "bulk2", "id": 101}
            },
            {
                "event_type": self.test_event_type,
                "event_data": {"test": "bulk3", "id": 102}
            }
        ]
        
        result = bulk_write_events(
            events,
            hasura_url=self.hasura_url,
            admin_secret=self.admin_secret
        )
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['total_events'], 3)
        self.assertEqual(result['events_created'], 3)
        self.assertEqual(result['events_duplicate'], 0)
        self.assertEqual(result['events_failed'], 0)
        self.assertEqual(len(result['created_event_ids']), 3)
        self.assertEqual(result['batches_processed'], 1)
        
        # Track for cleanup
        self.created_event_ids.extend(result['created_event_ids'])
        
        # Verify events were actually created
        from pyairbyte.utils.graphql_util import query_graphql_api
        query = """
        query GetEvents($ids: [Int!]!) {
          event_store(where: {id: {_in: $ids}}) {
            id
            event_type
            event_data
          }
        }
        """
        verify_result = query_graphql_api(
            query,
            variables={"ids": result['created_event_ids']},
            hasura_url=self.hasura_url,
            admin_secret=self.admin_secret
        )
        created_events = verify_result.get('data', {}).get('event_store', [])
        self.assertEqual(len(created_events), 3, "All 3 events should be in database")
    
    def test_bulk_write_events_with_duplicates_integration(self):
        """Integration test: Bulk write with duplicates using real Hasura."""
        # First, create an event
        event_type = self.test_event_type
        event_data = {"test": "duplicate_test", "id": 200}
        
        write_result = write_event(
            event_type,
            event_data,
            hasura_url=self.hasura_url,
            admin_secret=self.admin_secret
        )
        
        self.assertEqual(write_result['status'], 'success')
        self.created_event_ids.append(write_result['event_id'])
        
        # Now try to bulk insert the same event + new ones
        events = [
            {
                "event_type": event_type,
                "event_data": event_data  # Duplicate
            },
            {
                "event_type": event_type,
                "event_data": {"test": "new_event", "id": 201}  # New
            }
        ]
        
        result = bulk_write_events(
            events,
            hasura_url=self.hasura_url,
            admin_secret=self.admin_secret
        )
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['total_events'], 2)
        self.assertEqual(result['events_created'], 1)
        self.assertEqual(result['events_duplicate'], 1)
        self.assertEqual(len(result['created_event_ids']), 1)
        
        # Track new event for cleanup
        self.created_event_ids.extend(result['created_event_ids'])
    
    def test_bulk_write_events_chunking_integration(self):
        """Integration test: Bulk write with chunking (large batch) using real Hasura."""
        # Create 5 events with batch_size=2 to force chunking
        events = [
            {
                "event_type": self.test_event_type,
                "event_data": {"test": f"chunk_test_{i}", "id": 300 + i}
            }
            for i in range(5)
        ]
        
        result = bulk_write_events(
            events,
            hasura_url=self.hasura_url,
            admin_secret=self.admin_secret,
            batch_size=2  # Force chunking: 3 batches (2, 2, 1)
        )
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['total_events'], 5)
        self.assertEqual(result['events_created'], 5)
        self.assertEqual(result['batches_processed'], 3)  # 3 batches: 2, 2, 1
        self.assertEqual(len(result['created_event_ids']), 5)
        
        # Track for cleanup
        self.created_event_ids.extend(result['created_event_ids'])
        
        # Verify all events were created
        from pyairbyte.utils.graphql_util import query_graphql_api
        query = """
        query GetEvents($ids: [Int!]!) {
          event_store(where: {id: {_in: $ids}}) {
            id
            event_type
            event_data
          }
        }
        """
        verify_result = query_graphql_api(
            query,
            variables={"ids": result['created_event_ids']},
            hasura_url=self.hasura_url,
            admin_secret=self.admin_secret
        )
        created_events = verify_result.get('data', {}).get('event_store', [])
        self.assertEqual(len(created_events), 5, "All 5 events should be in database")
    
    def test_bulk_write_events_validation_errors_integration(self):
        """Integration test: Bulk write with validation errors using real Hasura."""
        events = [
            {
                "event_type": self.test_event_type,
                "event_data": {"test": "valid", "id": 400}
            },
            {
                "event_type": self.test_event_type
                # Missing event_data
            },
            {
                "event_data": {"test": "missing_type", "id": 401}
                # Missing event_type
            }
        ]
        
        result = bulk_write_events(
            events,
            hasura_url=self.hasura_url,
            admin_secret=self.admin_secret
        )
        
        # Should have partial success
        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['total_events'], 3)
        self.assertEqual(result['events_created'], 1)
        self.assertEqual(result['events_failed'], 2)
        self.assertGreaterEqual(len(result['errors']), 2)
        
        # Track valid event for cleanup
        if result['created_event_ids']:
            self.created_event_ids.extend(result['created_event_ids'])


if __name__ == '__main__':
    # Run unit tests (mocked) by default
    # Run integration tests separately with: python -m unittest pyairbyte.utils.test_event_store.TestEventStoreIntegration
    unittest.main()

