"""
Unit and integration tests for cleanup_events asset functionality.

This module tests the GraphQL queries and mutations used by the cleanup_events asset
to move successfully processed events from event_store and event_processed_logs
to completed_integration_events table.
"""

import unittest
import os
import json
from unittest.mock import patch, MagicMock
from typing import Dict, Any

# Import the utilities used by cleanup_events
from pyairbyte.utils.graphql_util import query_graphql_api
from pyairbyte.utils.event_store import write_event, log_event_processing
from pyairbyte.utils.api_call import call_api_for_event_processing

# Skip integration tests if migration V7 hasn't been applied
# (integration fields don't exist in event_processed_logs table)
SKIP_INTEGRATION_TESTS = os.getenv('SKIP_CLEANUP_INTEGRATION_TESTS', 'false').lower() == 'true'


class TestCleanupEventsQueries(unittest.TestCase):
    """Unit tests for cleanup_events GraphQL queries and mutations."""
    
    @patch('pyairbyte.utils.graphql_util.requests.post')
    def test_query_successfully_processed_events(self, mock_post):
        """Test the GraphQL query for getting successfully processed events."""
        # Mock response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'data': {
                'event_processed_logs': [
                    {
                        'id': 1,
                        'event_id': 100,
                        'processed_at': '2025-01-01T00:00:00',
                        'processed_status': 'SUCCESS',
                        'processed_result': {'status': 'success', 'status_code': 200},
                        'processed_result_error': None,
                        'integration_url': 'https://api.example.com/endpoint',
                        'integration_request_method': 'POST',
                        'integration_payload': {'key': 'value'},
                        'event_store': {
                            'id': 100,
                            'event_type': 'USER_SYNC_EVENT',
                            'event_created_at': '2025-01-01T00:00:00',
                            'event_data': {'user_id': 123},
                            'event_hash': 'abc123'
                        }
                    }
                ]
            }
        }
        mock_response.status_code = 200
        mock_post.return_value = mock_response
        
        query = """
        query GetSuccessfullyProcessedEvents {
          event_processed_logs(
            where: {
              processed_status: {
                _eq: "SUCCESS"
              }
            }
            order_by: {
              processed_at: asc
            }
          ) {
            id
            event_id
            processed_at
            processed_status
            processed_result
            processed_result_error
            integration_url
            integration_request_method
            integration_payload
            event_store {
              id
              event_type
              event_created_at
              event_data
              event_hash
            }
          }
        }
        """
        
        result = query_graphql_api(query)
        logs = result.get('data', {}).get('event_processed_logs', [])
        
        self.assertEqual(len(logs), 1)
        self.assertEqual(logs[0]['id'], 1)
        self.assertEqual(logs[0]['event_id'], 100)
        self.assertEqual(logs[0]['processed_status'], 'SUCCESS')
        self.assertIsNotNone(logs[0]['event_store'])
        self.assertEqual(logs[0]['event_store']['id'], 100)
        mock_post.assert_called_once()
    
    @patch('pyairbyte.utils.graphql_util.requests.post')
    def test_bulk_insert_completed_events(self, mock_post):
        """Test the bulk insert mutation for completed_integration_events."""
        # Mock response - need to properly configure the mock
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'data': {
                'insert_completed_integration_events': {
                    'affected_rows': 2,
                    'returning': [
                        {'id': 1, 'event_id': 100, 'event_type': 'USER_SYNC_EVENT'},
                        {'id': 2, 'event_id': 101, 'event_type': 'USER_SYNC_EVENT'}
                    ]
                }
            },
            'errors': None
        }
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()  # Don't raise on status check
        mock_post.return_value = mock_response
        
        objects = [
            {
                'event_id': 100,
                'event_type': 'USER_SYNC_EVENT',
                'event_created_at': '2025-01-01T00:00:00',
                'event_data': {'user_id': 123},
                'event_hash': 'abc123',
                'integration_completed_at': '2025-01-01T00:00:00',
                'integration_completed_status': 'SUCCESS',
                'integration_url': 'https://api.example.com/endpoint',
                'integration_request_method': 'POST',
                'integration_payload': {'key': 'value'},
                'integration_response': {'status': 'success'}
            },
            {
                'event_id': 101,
                'event_type': 'USER_SYNC_EVENT',
                'event_created_at': '2025-01-01T00:00:00',
                'event_data': {'user_id': 124},
                'event_hash': 'def456',
                'integration_completed_at': '2025-01-01T00:00:00',
                'integration_completed_status': 'SUCCESS',
                'integration_url': 'https://api.example.com/endpoint',
                'integration_request_method': 'POST',
                'integration_payload': {'key': 'value2'},
                'integration_response': {'status': 'success'}
            }
        ]
        
        mutation = """
        mutation BulkInsertCompletedEvents($objects: [completed_integration_events_insert_input!]!) {
          insert_completed_integration_events(objects: $objects) {
            affected_rows
            returning {
              id
              event_id
              event_type
            }
          }
        }
        """
        
        result = query_graphql_api(mutation, variables={'objects': objects})
        insert_data = result.get('data', {}).get('insert_completed_integration_events', {})
        
        self.assertEqual(insert_data['affected_rows'], 2)
        self.assertEqual(len(insert_data['returning']), 2)
        mock_post.assert_called_once()
    
    @patch('pyairbyte.utils.graphql_util.requests.post')
    def test_delete_processing_log(self, mock_post):
        """Test the delete mutation for event_processed_logs."""
        # Mock response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'data': {
                'delete_event_processed_logs_by_pk': {
                    'id': 1
                }
            }
        }
        
        mutation = """
        mutation DeleteProcessingLog($logId: Int!) {
          delete_event_processed_logs_by_pk(id: $logId) {
            id
          }
        }
        """
        
        mock_response.status_code = 200
        mock_post.return_value = mock_response
        
        result = query_graphql_api(mutation, variables={'logId': 1})
        deleted = result.get('data', {}).get('delete_event_processed_logs_by_pk')
        
        self.assertIsNotNone(deleted)
        self.assertEqual(deleted['id'], 1)
        mock_post.assert_called_once()
    
    @patch('pyairbyte.utils.graphql_util.requests.post')
    def test_delete_event_store(self, mock_post):
        """Test the delete mutation for event_store."""
        # Mock response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'data': {
                'delete_event_store_by_pk': {
                    'id': 100
                }
            }
        }
        
        mutation = """
        mutation DeleteEvent($eventId: Int!) {
          delete_event_store_by_pk(id: $eventId) {
            id
          }
        }
        """
        
        mock_response.status_code = 200
        mock_post.return_value = mock_response
        
        result = query_graphql_api(mutation, variables={'eventId': 100})
        deleted = result.get('data', {}).get('delete_event_store_by_pk')
        
        self.assertIsNotNone(deleted)
        self.assertEqual(deleted['id'], 100)
        mock_post.assert_called_once()


class TestCleanupEventsIntegration(unittest.TestCase):
    """Integration tests for cleanup_events using real Hasura API calls.
    
    Note: These tests require migration V7 to be applied (integration fields in event_processed_logs).
    Set SKIP_CLEANUP_INTEGRATION_TESTS=true to skip if migration hasn't been applied yet.
    """
    
    def setUp(self):
        """Set up test fixtures for integration tests."""
        # Use real Hasura connection from environment
        self.hasura_url = os.getenv('HASURA_URL', 'http://hasura:8081/v1/graphql')
        self.admin_secret = os.getenv('HASURA_GRAPHQL_ADMIN_SECRET', 'admin')
        
        # Track created IDs for cleanup
        self.created_event_ids = []
        self.created_log_ids = []
        self.created_completed_event_ids = []
        
        # Test event type prefix to identify test data
        self.test_event_type = "TEST_CLEANUP_EVENT"
    
    def tearDown(self):
        """Clean up test data after each test."""
        from pyairbyte.utils.graphql_util import query_graphql_api
        
        # Delete completed events
        for completed_id in self.created_completed_event_ids:
            try:
                mutation = """
                mutation DeleteCompletedEvent($id: Int!) {
                  delete_completed_integration_events_by_pk(id: $id) {
                    id
                  }
                }
                """
                query_graphql_api(
                    mutation,
                    variables={"id": completed_id},
                    hasura_url=self.hasura_url,
                    admin_secret=self.admin_secret
                )
            except Exception as e:
                print(f"Warning: Failed to delete completed event {completed_id}: {e}")
        
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
        self.created_completed_event_ids = []
    
    def test_cleanup_workflow_integration(self):
        """Integration test: Full cleanup workflow with real Hasura API."""
        # Step 1: Create an event
        event_type = self.test_event_type
        event_data = {
            "test": "cleanup_integration_test",
            "user_id": 999,
            "timestamp": "2025-01-01T00:00:00"
        }
        
        write_result = write_event(
            event_type,
            event_data,
            hasura_url=self.hasura_url,
            admin_secret=self.admin_secret
        )
        
        self.assertEqual(write_result['status'], 'success')
        event_id = write_result['event_id']
        self.created_event_ids.append(event_id)
        
        # Step 2: Process the event (simulate API call and logging)
        api_result = {
            'status': 'success',
            'status_code': 200,
            'data': {'message': 'Success'},
            'error': None,
            'response_headers': {}
        }
        
        log_result = log_event_processing(
            event_id=event_id,
            api_call_result=api_result,
            hasura_url=self.hasura_url,
            admin_secret=self.admin_secret,
            integration_url='https://httpbin.org/post',
            integration_request_method='POST',
            integration_payload={'test': 'data'}
        )
        
        self.assertEqual(log_result['status'], 'success')
        log_id = log_result['log_id']
        self.created_log_ids.append(log_id)
        
        # Step 3: Query successfully processed events
        query = """
        query GetSuccessfullyProcessedEvents {
          event_processed_logs(
            where: {
              processed_status: {
                _eq: "SUCCESS"
              }
              event_id: {
                _eq: %d
              }
            }
            order_by: {
              processed_at: asc
            }
          ) {
            id
            event_id
            processed_at
            processed_status
            processed_result
            processed_result_error
            integration_url
            integration_request_method
            integration_payload
            event_store {
              id
              event_type
              event_created_at
              event_data
              event_hash
            }
          }
        }
        """ % event_id
        
        result = query_graphql_api(
            query,
            hasura_url=self.hasura_url,
            admin_secret=self.admin_secret
        )
        
        logs = result.get('data', {}).get('event_processed_logs', [])
        self.assertGreater(len(logs), 0, "Should find at least one successfully processed event")
        
        # Find our test log
        test_log = None
        for log in logs:
            if log['event_id'] == event_id:
                test_log = log
                break
        
        self.assertIsNotNone(test_log, "Should find our test log")
        self.assertEqual(test_log['processed_status'], 'SUCCESS')
        self.assertIsNotNone(test_log['event_store'])
        self.assertEqual(test_log['event_store']['id'], event_id)
        self.assertEqual(test_log['integration_url'], 'https://httpbin.org/post')
        self.assertEqual(test_log['integration_request_method'], 'POST')
        
        # Step 4: Insert into completed_integration_events
        event_store = test_log['event_store']
        event_to_insert = {
            'event_id': event_store['id'],
            'event_type': event_store['event_type'],
            'event_created_at': event_store['event_created_at'],
            'event_data': event_store['event_data'],
            'event_hash': event_store['event_hash'],
            'integration_completed_at': test_log['processed_at'],
            'integration_completed_status': test_log['processed_status'],
            'integration_url': test_log['integration_url'],
            'integration_request_method': test_log['integration_request_method'],
            'integration_payload': test_log['integration_payload'],
            'integration_response': test_log['processed_result']
        }
        
        bulk_insert_mutation = """
        mutation BulkInsertCompletedEvents($objects: [completed_integration_events_insert_input!]!) {
          insert_completed_integration_events(objects: $objects) {
            affected_rows
            returning {
              id
              event_id
              event_type
            }
          }
        }
        """
        
        insert_result = query_graphql_api(
            bulk_insert_mutation,
            variables={'objects': [event_to_insert]},
            hasura_url=self.hasura_url,
            admin_secret=self.admin_secret
        )
        
        insert_data = insert_result.get('data', {}).get('insert_completed_integration_events', {})
        self.assertEqual(insert_data['affected_rows'], 1)
        completed_event_id = insert_data['returning'][0]['id']
        self.created_completed_event_ids.append(completed_event_id)
        
        # Step 5: Verify the completed event exists
        verify_query = """
        query GetCompletedEvent($id: Int!) {
          completed_integration_events_by_pk(id: $id) {
            id
            event_id
            event_type
            integration_url
            integration_request_method
            integration_completed_status
          }
        }
        """
        
        verify_result = query_graphql_api(
            verify_query,
            variables={'id': completed_event_id},
            hasura_url=self.hasura_url,
            admin_secret=self.admin_secret
        )
        
        completed_event = verify_result.get('data', {}).get('completed_integration_events_by_pk')
        self.assertIsNotNone(completed_event)
        self.assertEqual(completed_event['event_id'], event_id)
        self.assertEqual(completed_event['event_type'], event_type)
        self.assertEqual(completed_event['integration_url'], 'https://httpbin.org/post')
        self.assertEqual(completed_event['integration_request_method'], 'POST')
        self.assertEqual(completed_event['integration_completed_status'], 'SUCCESS')
        
        # Step 6: Delete from event_processed_logs
        delete_log_mutation = """
        mutation DeleteProcessingLog($logId: Int!) {
          delete_event_processed_logs_by_pk(id: $logId) {
            id
          }
        }
        """
        
        delete_log_result = query_graphql_api(
            delete_log_mutation,
            variables={'logId': log_id},
            hasura_url=self.hasura_url,
            admin_secret=self.admin_secret
        )
        
        deleted_log = delete_log_result.get('data', {}).get('delete_event_processed_logs_by_pk')
        self.assertIsNotNone(deleted_log)
        self.assertEqual(deleted_log['id'], log_id)
        
        # Remove from tracking since we deleted it
        self.created_log_ids.remove(log_id)
        
        # Step 7: Delete from event_store
        delete_event_mutation = """
        mutation DeleteEvent($eventId: Int!) {
          delete_event_store_by_pk(id: $eventId) {
            id
          }
        }
        """
        
        delete_event_result = query_graphql_api(
            delete_event_mutation,
            variables={'eventId': event_id},
            hasura_url=self.hasura_url,
            admin_secret=self.admin_secret
        )
        
        deleted_event = delete_event_result.get('data', {}).get('delete_event_store_by_pk')
        self.assertIsNotNone(deleted_event)
        self.assertEqual(deleted_event['id'], event_id)
        
        # Remove from tracking since we deleted it
        self.created_event_ids.remove(event_id)
    
    @unittest.skipIf(SKIP_INTEGRATION_TESTS, "Skipping integration tests (migration V7 may not be applied)")
    def test_cleanup_with_missing_integration_fields(self):
        """Integration test: Cleanup should skip events with missing integration fields."""
        # Create an event
        event_type = self.test_event_type
        event_data = {'test': 'missing_fields_test'}
        
        write_result = write_event(
            event_type,
            event_data,
            hasura_url=self.hasura_url,
            admin_secret=self.admin_secret
        )
        
        event_id = write_result['event_id']
        self.created_event_ids.append(event_id)
        
        # Create a log without integration fields (simulating old data)
        # We'll need to insert directly via GraphQL since log_event_processing requires these fields
        mutation = """
        mutation InsertLogWithoutIntegrationFields($eventId: Int!, $processedResult: jsonb!) {
          insert_event_processed_logs_one(object: {
            event_id: $eventId
            processed_status: "SUCCESS"
            processed_result: $processedResult
          }) {
            id
            event_id
            processed_status
            integration_url
            integration_request_method
            integration_payload
          }
        }
        """
        
        result = query_graphql_api(
            mutation,
            variables={
                'eventId': event_id,
                'processedResult': {'status': 'success'}
            },
            hasura_url=self.hasura_url,
            admin_secret=self.admin_secret
        )
        
        log_data = result.get('data', {}).get('insert_event_processed_logs_one')
        self.assertIsNotNone(log_data)
        log_id = log_data['id']
        self.created_log_ids.append(log_id)
        
        # Verify integration fields are None
        self.assertIsNone(log_data.get('integration_url'))
        self.assertIsNone(log_data.get('integration_request_method'))
        self.assertIsNone(log_data.get('integration_payload'))
        
        # Query for successfully processed events
        query = """
        query GetSuccessfullyProcessedEvents($eventId: Int!) {
          event_processed_logs(
            where: {
              processed_status: {
                _eq: "SUCCESS"
              }
              event_id: {
                _eq: $eventId
              }
            }
          ) {
            id
            event_id
            integration_url
            integration_request_method
            integration_payload
            event_store {
              id
              event_type
              event_created_at
              event_data
              event_hash
            }
          }
        }
        """
        
        result = query_graphql_api(
            query,
            variables={'eventId': event_id},
            hasura_url=self.hasura_url,
            admin_secret=self.admin_secret
        )
        
        logs = result.get('data', {}).get('event_processed_logs', [])
        self.assertGreater(len(logs), 0)
        
        # Verify that this log would be skipped (missing integration fields)
        test_log = logs[0]
        self.assertIsNone(test_log.get('integration_url'))
        self.assertIsNone(test_log.get('integration_request_method'))
        self.assertIsNone(test_log.get('integration_payload'))


if __name__ == '__main__':
    unittest.main()

