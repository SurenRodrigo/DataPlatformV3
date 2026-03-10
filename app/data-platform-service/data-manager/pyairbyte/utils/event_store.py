import json
import hashlib
import logging
import os
from typing import Dict, Any, Optional, List

from .graphql_util import query_graphql_api

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _create_event_hash(event_type: str, event_data: Dict[str, Any]) -> str:
    """
    Creates a hash from event_type and event_data to detect duplicates.
    
    Args:
        event_type: The event type string
        event_data: The event data dictionary
    
    Returns:
        SHA256 hash string of the combined event_type and event_data
    """
    # Convert event_data to JSON string with sorted keys for consistent hashing
    event_data_json = json.dumps(event_data, sort_keys=True, ensure_ascii=False)
    
    # Combine event_type and event_data for hashing
    combined_string = f"{event_type}:{event_data_json}"
    
    # Create SHA256 hash
    hash_object = hashlib.sha256(combined_string.encode('utf-8'))
    return hash_object.hexdigest()


def _check_hash_exists(event_hash: str, hasura_url: Optional[str] = None, admin_secret: Optional[str] = None) -> bool:
    """
    Checks if an event with the given hash already exists in either:
    - event_store (pending events)
    - completed_integration_events (successfully processed events)
    
    This ensures we don't re-process events that have already been completed.
    
    Args:
        event_hash: The hash to check
        hasura_url: Optional Hasura URL (uses graphql_util defaults if not provided)
        admin_secret: Optional admin secret (uses graphql_util defaults if not provided)
    
    Returns:
        True if hash exists in either table, False otherwise
    """
    variables = {
        "eventHash": event_hash
    }
    
    # Query 1: Check event_store
    query_store = """
    query CheckEventHashInStore($eventHash: String!) {
      event_store(where: {event_hash: {_eq: $eventHash}}, limit: 1) {
        id
        event_hash
      }
    }
    """
    
    # Query 2: Check completed_integration_events
    query_completed = """
    query CheckEventHashInCompleted($eventHash: String!) {
      completed_integration_events(where: {event_hash: {_eq: $eventHash}}, limit: 1) {
        id
        event_hash
      }
    }
    """
    
    try:
        # Check event_store first
        result_store = query_graphql_api(query_store, variables=variables, hasura_url=hasura_url, admin_secret=admin_secret)
        events_store = result_store.get('data', {}).get('event_store', [])
        
        if len(events_store) > 0:
            logger.debug(f"Duplicate event hash {event_hash} found in event_store")
            return True
        
        # Check completed_integration_events
        result_completed = query_graphql_api(query_completed, variables=variables, hasura_url=hasura_url, admin_secret=admin_secret)
        events_completed = result_completed.get('data', {}).get('completed_integration_events', [])
        
        if len(events_completed) > 0:
            logger.debug(f"Duplicate event hash {event_hash} found in completed_integration_events (already processed)")
            return True
        
        return False
        
    except Exception as e:
        logger.error(f"Error checking event hash existence: {str(e)}")
        raise


def write_event(
    event_type: str,
    event_data: Dict[str, Any],
    hasura_url: Optional[str] = None,
    admin_secret: Optional[str] = None
) -> Dict[str, Any]:
    """
    Writes an event to the event_store table using Hasura GraphQL mutation.
    
    This function:
    1. Validates that event_data is valid JSON (by converting to JSON string)
    2. Creates an event_hash from event_type and event_data
    3. Checks if the hash already exists in either event_store or completed_integration_events (duplicate detection)
    4. If not a duplicate, inserts the event into event_store
    
    Args:
        event_type: The type of event (e.g., "UNIT4_DITIO_EVENT")
        event_data: Dictionary containing the event data (will be stored as JSONB)
        hasura_url: Optional Hasura GraphQL endpoint URL. If not provided, uses HASURA_URL env var or default
        admin_secret: Optional Hasura admin secret. If not provided, uses HASURA_GRAPHQL_ADMIN_SECRET env var or default
    
    Returns:
        Dictionary containing the result:
        {
            "status": "success" | "duplicate" | "error",
            "event_id": int | None,
            "event_hash": str,
            "message": str,
            "data": dict | None
        }
    
    Raises:
        ValueError: If event_data cannot be converted to valid JSON
        ValueError: If event_hash already exists (duplicate event)
    """
    # Validate event_data by converting to JSON (raises ValueError if invalid)
    try:
        event_data_json = json.dumps(event_data, ensure_ascii=False)
        # Parse back to ensure it's valid JSON
        json.loads(event_data_json)
    except (TypeError, ValueError) as e:
        error_msg = f"Invalid event_data: cannot be converted to JSON - {str(e)}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Create event hash
    event_hash = _create_event_hash(event_type, event_data)
    logger.info(f"Created event hash: {event_hash} for event_type: {event_type}")
    
    # Check if hash already exists (duplicate detection)
    try:
        hash_exists = _check_hash_exists(event_hash, hasura_url=hasura_url, admin_secret=admin_secret)
        if hash_exists:
            logger.warning(f"Duplicate event detected - hash {event_hash} already exists")
            return {
                "status": "duplicate",
                "event_id": None,
                "event_hash": event_hash,
                "message": "Event with this hash already exists in event_store or completed_integration_events",
                "data": None
            }
    except Exception as e:
        logger.error(f"Error checking for duplicate event: {str(e)}")
        raise
    
    # Insert event using GraphQL mutation
    mutation = """
    mutation InsertEvent($eventType: String!, $eventData: jsonb!, $eventHash: String!) {
      insert_event_store_one(object: {
        event_type: $eventType
        event_data: $eventData
        event_hash: $eventHash
      }) {
        id
        event_type
        event_created_at
        event_hash
        event_data
      }
    }
    """
    
    variables = {
        "eventType": event_type,
        "eventData": event_data,  # GraphQL will handle JSONB conversion
        "eventHash": event_hash
    }
    
    try:
        logger.info(f"Inserting event: event_type={event_type}, event_hash={event_hash}")
        result = query_graphql_api(mutation, variables=variables, hasura_url=hasura_url, admin_secret=admin_secret)
        
        # Extract the inserted event data
        inserted_event = result.get('data', {}).get('insert_event_store_one')
        
        if inserted_event:
            logger.info(f"Event successfully inserted with id: {inserted_event.get('id')}")
            return {
                "status": "success",
                "event_id": inserted_event.get('id'),
                "event_hash": event_hash,
                "message": "Event successfully inserted into event_store",
                "data": inserted_event
            }
        else:
            error_msg = "Event insertion returned no data"
            logger.error(error_msg)
            return {
                "status": "error",
                "event_id": None,
                "event_hash": event_hash,
                "message": error_msg,
                "data": None
            }
            
    except ValueError as e:
        # GraphQL errors (including unique constraint violations)
        error_msg = str(e)
        logger.error(f"GraphQL error inserting event: {error_msg}")
        
        # Check if it's a duplicate constraint violation
        if "duplicate" in error_msg.lower() or "unique" in error_msg.lower():
            return {
                "status": "duplicate",
                "event_id": None,
                "event_hash": event_hash,
                "message": "Event with this hash already exists (detected during insert)",
                "data": None
            }
        
        return {
            "status": "error",
            "event_id": None,
            "event_hash": event_hash,
            "message": error_msg,
            "data": None
        }
    except Exception as e:
        error_msg = f"Unexpected error inserting event: {str(e)}"
        logger.error(error_msg)
        return {
            "status": "error",
            "event_id": None,
            "event_hash": event_hash,
            "message": error_msg,
            "data": None
        }


def get_unprocessed_or_failed_events(
    event_type: str,
    hasura_url: Optional[str] = None,
    admin_secret: Optional[str] = None
) -> Dict[str, Any]:
    """
    Retrieves all events from event_store that are either:
    1. Not processed yet (no record in event_processed_logs)
    2. Previously failed (has a record in event_processed_logs with processed_status = 'FAILED')
    
    Filtered by the specified event_type.
    
    Args:
        event_type: The type of event to filter by (e.g., "UNIT4_DITIO_EVENT")
        hasura_url: Optional Hasura GraphQL endpoint URL. If not provided, uses HASURA_URL env var or default
        admin_secret: Optional Hasura admin secret. If not provided, uses HASURA_GRAPHQL_ADMIN_SECRET env var or default
    
    Returns:
        Dictionary containing the result:
        {
            "status": "success" | "error",
            "events": list,  # List of event objects
            "count": int,     # Number of events found
            "message": str,
            "data": dict | None  # Full GraphQL response data
        }
    
    Raises:
        ValueError: If GraphQL query fails
    """
    query = """
    query GetUnprocessedOrFailedEventsWithType($eventType: String!) {
      event_store(
        where: {
          _and: [
            {
              event_type: {
                _eq: $eventType
              }
            },
            {
              _or: [
                {
                  event_processed_logs_aggregate: {
                    count: {
                      predicate: {
                        _eq: 0
                      }
                    }
                  }
                },
                {
                  event_processed_logs: {
                    processed_status: {
                      _eq: "FAILED"
                    }
                  }
                }
              ]
            }
          ]
        }
        order_by: {
          event_created_at: asc
        }
      ) {
        id
        event_type
        event_created_at
        event_data
        event_hash
        event_processed_logs(
          order_by: {
            processed_at: desc
          }
          limit: 1
        ) {
          id
          processed_at
          processed_status
          processed_result
          processed_result_error
        }
      }
    }
    """
    
    variables = {
        "eventType": event_type
    }
    
    try:
        logger.info(f"Retrieving unprocessed or failed events for event_type: {event_type}")
        result = query_graphql_api(query, variables=variables, hasura_url=hasura_url, admin_secret=admin_secret)
        
        # Extract events from the response
        events = result.get('data', {}).get('event_store', [])
        event_count = len(events)
        
        logger.info(f"Found {event_count} unprocessed or failed events for event_type: {event_type}")
        
        return {
            "status": "success",
            "events": events,
            "count": event_count,
            "message": f"Retrieved {event_count} unprocessed or failed events",
            "data": result
        }
        
    except ValueError as e:
        # GraphQL errors
        error_msg = str(e)
        logger.error(f"GraphQL error retrieving events: {error_msg}")
        return {
            "status": "error",
            "events": [],
            "count": 0,
            "message": error_msg,
            "data": None
        }
    except Exception as e:
        error_msg = f"Unexpected error retrieving events: {str(e)}"
        logger.error(error_msg)
        return {
            "status": "error",
            "events": [],
            "count": 0,
            "message": error_msg,
            "data": None
        }


def _get_latest_processing_log(
    event_id: int,
    hasura_url: Optional[str] = None,
    admin_secret: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """
    Gets the latest processing log for an event_id.
    
    Args:
        event_id: The ID of the event
        hasura_url: Optional Hasura URL (uses graphql_util defaults if not provided)
        admin_secret: Optional admin secret (uses graphql_util defaults if not provided)
    
    Returns:
        Dictionary with the latest log data, or None if no log exists
    """
    query = """
    query GetLatestProcessingLog($eventId: Int!) {
      event_processed_logs(
        where: {event_id: {_eq: $eventId}}
        order_by: {processed_at: desc}
        limit: 1
      ) {
        id
        event_id
        processed_at
        processed_status
        processed_result
        processed_result_error
      }
    }
    """
    
    variables = {
        "eventId": event_id
    }
    
    try:
        result = query_graphql_api(query, variables=variables, hasura_url=hasura_url, admin_secret=admin_secret)
        logs = result.get('data', {}).get('event_processed_logs', [])
        return logs[0] if logs else None
    except Exception as e:
        logger.error(f"Error getting latest processing log: {str(e)}")
        raise


def log_event_processing(
    event_id: int,
    api_call_result: Dict[str, Any],
    hasura_url: Optional[str] = None,
    admin_secret: Optional[str] = None,
    integration_url: Optional[str] = None,
    integration_request_method: Optional[str] = None,
    integration_payload: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Logs the processing result of an event to the event_processed_logs table.
    
    This function uses an upsert pattern: if a processing log already exists for this event_id,
    it updates the latest record. Otherwise, it inserts a new record. This ensures we maintain
    only one record per event_id representing the latest processing status.
    
    This function should be called after making an API call using api_call.py to record
    whether the processing was successful or failed.
    
    Args:
        event_id: The ID of the event from event_store table
        api_call_result: The result dictionary from api_call.call_api_for_event_processing() method
        hasura_url: Optional Hasura GraphQL endpoint URL. If not provided, uses HASURA_URL env var or default
        admin_secret: Optional Hasura admin secret. If not provided, uses HASURA_GRAPHQL_ADMIN_SECRET env var or default
        integration_url: Optional API endpoint URL used for the integration call
        integration_request_method: Optional HTTP method used (GET, POST, PUT, PATCH, DELETE)
        integration_payload: Optional request body/payload dictionary sent to the integration API
    
    Returns:
        Dictionary containing the result:
        {
            "status": "success" | "error",
            "log_id": int | None,
            "processed_status": str,  # "SUCCESS" or "FAILED"
            "message": str,
            "data": dict | None,
            "action": str  # "inserted" or "updated"
        }
    
    Raises:
        ValueError: If event_id is invalid or api_call_result is malformed
    """
    # Validate event_id
    if not isinstance(event_id, int) or event_id <= 0:
        error_msg = f"Invalid event_id: must be a positive integer, got {event_id}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Validate api_call_result structure matches call_api_for_event_processing() return format
    if not isinstance(api_call_result, dict):
        error_msg = "Invalid api_call_result: must be a dictionary"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Validate required keys from call_api_for_event_processing() return format
    required_keys = ['status', 'status_code', 'data', 'error', 'response_headers']
    missing_keys = [key for key in required_keys if key not in api_call_result]
    if missing_keys:
        error_msg = f"Invalid api_call_result: missing required keys: {missing_keys}. Expected format from api_call.call_api_for_event_processing(): {{'status': str, 'status_code': int, 'data': dict|None, 'error': str|None, 'response_headers': dict|None}}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Validate status value
    api_status = api_call_result.get('status')
    if api_status not in ['success', 'error']:
        error_msg = f"Invalid api_call_result.status: must be 'success' or 'error', got '{api_status}'"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Validate status_code is an integer (can be None for connection errors)
    status_code = api_call_result.get('status_code')
    if status_code is not None and not isinstance(status_code, int):
        error_msg = f"Invalid api_call_result.status_code: must be int or None, got {type(status_code).__name__}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Determine processed_status based on api_call_result
    if api_status == 'success':
        processed_status = 'SUCCESS'
        processed_result_error = None
    else:
        processed_status = 'FAILED'
        # Extract error message from api_call_result
        processed_result_error = api_call_result.get('error', 'Unknown error')
    
    # Prepare processed_result (the full api_call_result as JSONB)
    try:
        # Validate that processed_result can be converted to JSON
        json.dumps(api_call_result)
        processed_result = api_call_result
    except (TypeError, ValueError) as e:
        error_msg = f"Invalid api_call_result: cannot be converted to JSON - {str(e)}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Check if a processing log already exists for this event_id
    try:
        existing_log = _get_latest_processing_log(event_id, hasura_url=hasura_url, admin_secret=admin_secret)
    except Exception as e:
        logger.error(f"Error checking for existing processing log: {str(e)}")
        raise
    
    try:
        if existing_log:
            # Update existing log
            log_id = existing_log.get('id')
            mutation = """
            mutation UpdateEventProcessingLog(
                $logId: Int!,
                $processedStatus: String!,
                $processedResult: jsonb!,
                $processedResultError: String,
                $integrationUrl: String,
                $integrationRequestMethod: String,
                $integrationPayload: jsonb
            ) {
              update_event_processed_logs_by_pk(
                pk_columns: {id: $logId}
                _set: {
                  processed_status: $processedStatus
                  processed_result: $processedResult
                  processed_result_error: $processedResultError
                  integration_url: $integrationUrl
                  integration_request_method: $integrationRequestMethod
                  integration_payload: $integrationPayload
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
              }
            }
            """
            
            variables = {
                "logId": log_id,
                "processedStatus": processed_status,
                "processedResult": processed_result,
                "processedResultError": processed_result_error,
                "integrationUrl": integration_url,
                "integrationRequestMethod": integration_request_method,
                "integrationPayload": integration_payload
            }
            
            try:
                logger.info(f"Updating event processing log: event_id={event_id}, log_id={log_id}, status={processed_status}")
                result = query_graphql_api(mutation, variables=variables, hasura_url=hasura_url, admin_secret=admin_secret)
                
                updated_log = result.get('data', {}).get('update_event_processed_logs_by_pk')
                
                if updated_log:
                    logger.info(f"Event processing log successfully updated with id: {updated_log.get('id')}")
                    return {
                        "status": "success",
                        "log_id": updated_log.get('id'),
                        "processed_status": processed_status,
                        "message": "Event processing log successfully updated",
                        "data": updated_log,
                        "action": "updated"
                    }
                else:
                    error_msg = "Event processing log update returned no data"
                    logger.error(error_msg)
                    return {
                        "status": "error",
                        "log_id": log_id,
                        "processed_status": processed_status,
                        "message": error_msg,
                        "data": None,
                        "action": "update_failed"
                    }
            except ValueError as e:
                error_msg = str(e)
                logger.error(f"GraphQL error updating event processing log: {error_msg}")
                return {
                    "status": "error",
                    "log_id": log_id,
                    "processed_status": processed_status,
                    "message": error_msg,
                    "data": None,
                    "action": "update_failed"
                }
        else:
            # Insert new log
            mutation = """
            mutation InsertEventProcessingLog(
                $eventId: Int!,
                $processedStatus: String!,
                $processedResult: jsonb!,
                $processedResultError: String,
                $integrationUrl: String,
                $integrationRequestMethod: String,
                $integrationPayload: jsonb
            ) {
              insert_event_processed_logs_one(object: {
                event_id: $eventId
                processed_status: $processedStatus
                processed_result: $processedResult
                processed_result_error: $processedResultError
                integration_url: $integrationUrl
                integration_request_method: $integrationRequestMethod
                integration_payload: $integrationPayload
              }) {
                id
                event_id
                processed_at
                processed_status
                processed_result
                processed_result_error
                integration_url
                integration_request_method
                integration_payload
              }
            }
            """
            
            variables = {
                "eventId": event_id,
                "processedStatus": processed_status,
                "processedResult": processed_result,
                "processedResultError": processed_result_error,
                "integrationUrl": integration_url,
                "integrationRequestMethod": integration_request_method,
                "integrationPayload": integration_payload
            }
            
            try:
                logger.info(f"Inserting event processing log: event_id={event_id}, status={processed_status}")
                result = query_graphql_api(mutation, variables=variables, hasura_url=hasura_url, admin_secret=admin_secret)
                
                inserted_log = result.get('data', {}).get('insert_event_processed_logs_one')
                
                if inserted_log:
                    logger.info(f"Event processing log successfully inserted with id: {inserted_log.get('id')}")
                    return {
                        "status": "success",
                        "log_id": inserted_log.get('id'),
                        "processed_status": processed_status,
                        "message": "Event processing log successfully inserted",
                        "data": inserted_log,
                        "action": "inserted"
                    }
                else:
                    error_msg = "Event processing log insertion returned no data"
                    logger.error(error_msg)
                    return {
                        "status": "error",
                        "log_id": None,
                        "processed_status": processed_status,
                        "message": error_msg,
                        "data": None,
                        "action": "insert_failed"
                    }
            except ValueError as e:
                error_msg = str(e)
                logger.error(f"GraphQL error inserting event processing log: {error_msg}")
                return {
                    "status": "error",
                    "log_id": None,
                    "processed_status": processed_status,
                    "message": error_msg,
                    "data": None,
                    "action": "insert_failed"
                }
    except Exception as e:
        # Fallback error handling
        error_msg = f"Unexpected error in event processing log: {str(e)}"
        logger.error(error_msg)
        return {
            "status": "error",
            "log_id": None,
            "processed_status": processed_status,
            "message": error_msg,
            "data": None,
            "action": "unknown"
        }


def _check_hashes_exist(
    event_hashes: List[str],
    hasura_url: Optional[str] = None,
    admin_secret: Optional[str] = None
) -> set:
    """
    Checks which event hashes already exist in either:
    - event_store (pending events)
    - completed_integration_events (successfully processed events)
    
    This ensures we don't re-process events that have already been completed.
    
    Args:
        event_hashes: List of hashes to check
        hasura_url: Optional Hasura URL (uses graphql_util defaults if not provided)
        admin_secret: Optional admin secret (uses graphql_util defaults if not provided)
    
    Returns:
        Set of hashes that exist in either table
    """
    if not event_hashes:
        return set()
    
    variables = {
        "hashes": event_hashes
    }
    
    # Query 1: Check event_store (bulk)
    query_store = """
    query CheckEventHashesInStore($hashes: [String!]!) {
      event_store(where: {event_hash: {_in: $hashes}}) {
        event_hash
      }
    }
    """
    
    # Query 2: Check completed_integration_events (bulk)
    query_completed = """
    query CheckEventHashesInCompleted($hashes: [String!]!) {
      completed_integration_events(where: {event_hash: {_in: $hashes}}) {
        event_hash
      }
    }
    """
    
    try:
        # Check event_store
        result_store = query_graphql_api(query_store, variables=variables, hasura_url=hasura_url, admin_secret=admin_secret)
        existing_store = result_store.get('data', {}).get('event_store', [])
        hashes_in_store = {item['event_hash'] for item in existing_store}
        
        # Check completed_integration_events
        result_completed = query_graphql_api(query_completed, variables=variables, hasura_url=hasura_url, admin_secret=admin_secret)
        existing_completed = result_completed.get('data', {}).get('completed_integration_events', [])
        hashes_in_completed = {item['event_hash'] for item in existing_completed}
        
        # Combine results from both tables
        all_existing_hashes = hashes_in_store | hashes_in_completed
        
        if hashes_in_completed:
            logger.debug(
                f"Found {len(hashes_in_completed)} duplicate hash(es) in completed_integration_events "
                f"(already processed), {len(hashes_in_store)} in event_store"
            )
        elif hashes_in_store:
            logger.debug(f"Found {len(hashes_in_store)} duplicate hash(es) in event_store")
        
        return all_existing_hashes
        
    except Exception as e:
        logger.error(f"Error checking event hashes existence: {str(e)}")
        raise


def _process_batch(
    events: List[Dict[str, Any]],
    hasura_url: Optional[str] = None,
    admin_secret: Optional[str] = None
) -> Dict[str, Any]:
    """
    Processes a single batch of events (internal helper for bulk_write_events).
    
    Args:
        events: List of validated event dictionaries with event_type and event_data
        hasura_url: Optional Hasura URL
        admin_secret: Optional admin secret
    
    Returns:
        Dictionary with batch processing results
    """
    if not events:
        return {
            "status": "success",
            "events_created": 0,
            "events_duplicate": 0,
            "events_failed": 0,
            "created_event_ids": [],
            "duplicate_hashes": [],
            "errors": []
        }
    
    # Step 1: Generate hashes for all events
    events_with_hashes = []
    hash_to_event_index = {}
    
    for idx, event in enumerate(events):
        try:
            event_type = event['event_type']
            event_data = event['event_data']
            
            # Validate event_data can be converted to JSON
            json.dumps(event_data)
            
            # Generate hash
            event_hash = _create_event_hash(event_type, event_data)
            
            events_with_hashes.append({
                'event_type': event_type,
                'event_data': event_data,
                'event_hash': event_hash,
                'original_index': idx
            })
            hash_to_event_index[event_hash] = idx
            
        except (KeyError, TypeError, ValueError) as e:
            logger.error(f"Error processing event at index {idx}: {str(e)}")
            continue
    
    if not events_with_hashes:
        return {
            "status": "error",
            "events_created": 0,
            "events_duplicate": 0,
            "events_failed": len(events),
            "created_event_ids": [],
            "duplicate_hashes": [],
            "errors": [{"index": i, "error": "Failed to process event"} for i in range(len(events))]
        }
    
    # Step 2: Bulk check for duplicates
    all_hashes = [e['event_hash'] for e in events_with_hashes]
    existing_hashes = _check_hashes_exist(all_hashes, hasura_url=hasura_url, admin_secret=admin_secret)
    
    # Step 3: Filter out duplicates
    events_to_insert = [e for e in events_with_hashes if e['event_hash'] not in existing_hashes]
    duplicate_hashes = list(existing_hashes)
    
    if not events_to_insert:
        # All events are duplicates
        return {
            "status": "success",
            "events_created": 0,
            "events_duplicate": len(events_with_hashes),
            "events_failed": 0,
            "created_event_ids": [],
            "duplicate_hashes": duplicate_hashes,
            "errors": []
        }
    
    # Step 4: Bulk insert non-duplicate events
    mutation = """
    mutation BulkInsertEvents($objects: [event_store_insert_input!]!) {
      insert_event_store(objects: $objects) {
        affected_rows
        returning {
          id
          event_type
          event_created_at
          event_hash
          event_data
        }
      }
    }
    """
    
    objects = [
        {
            "event_type": e['event_type'],
            "event_data": e['event_data'],
            "event_hash": e['event_hash']
        }
        for e in events_to_insert
    ]
    
    variables = {
        "objects": objects
    }
    
    try:
        logger.info(f"Bulk inserting {len(events_to_insert)} events")
        result = query_graphql_api(mutation, variables=variables, hasura_url=hasura_url, admin_secret=admin_secret)
        
        insert_result = result.get('data', {}).get('insert_event_store', {})
        affected_rows = insert_result.get('affected_rows', 0)
        returning = insert_result.get('returning', [])
        
        created_event_ids = [item['id'] for item in returning]
        
        logger.info(f"Successfully inserted {affected_rows} events")
        
        return {
            "status": "success",
            "events_created": affected_rows,
            "events_duplicate": len(duplicate_hashes),
            "events_failed": 0,
            "created_event_ids": created_event_ids,
            "duplicate_hashes": duplicate_hashes,
            "errors": []
        }
        
    except ValueError as e:
        # GraphQL errors
        error_msg = str(e)
        logger.error(f"GraphQL error in bulk insert: {error_msg}")
        
        # Check if it's a duplicate constraint violation
        if "duplicate" in error_msg.lower() or "unique" in error_msg.lower():
            # If we get here, it means some duplicates weren't caught (race condition)
            # Mark all as duplicates for safety
            return {
                "status": "error",
                "events_created": 0,
                "events_duplicate": len(events_to_insert),
                "events_failed": 0,
                "created_event_ids": [],
                "duplicate_hashes": duplicate_hashes + [e['event_hash'] for e in events_to_insert],
                "errors": [{"error": error_msg}]
            }
        
        return {
            "status": "error",
            "events_created": 0,
            "events_duplicate": len(duplicate_hashes),
            "events_failed": len(events_to_insert),
            "created_event_ids": [],
            "duplicate_hashes": duplicate_hashes,
            "errors": [{"error": error_msg}]
        }
    except Exception as e:
        error_msg = f"Unexpected error in bulk insert: {str(e)}"
        logger.error(error_msg)
        return {
            "status": "error",
            "events_created": 0,
            "events_duplicate": len(duplicate_hashes),
            "events_failed": len(events_to_insert),
            "created_event_ids": [],
            "duplicate_hashes": duplicate_hashes,
            "errors": [{"error": error_msg}]
        }


def bulk_write_events(
    events: List[Dict[str, Any]],
    hasura_url: Optional[str] = None,
    admin_secret: Optional[str] = None,
    batch_size: Optional[int] = None
) -> Dict[str, Any]:
    """
    Bulk writes multiple events to the event_store table using Hasura GraphQL mutation.
    
    Automatically chunks events into batches based on BULK_EVENTS_BATCH_SIZE environment variable
    (default: 1000) to avoid timeout and memory issues. Processes each batch sequentially.
    
    Duplicate detection checks both event_store and completed_integration_events tables to ensure
    events that have already been successfully processed are not re-inserted.
    
    Args:
        events: List of dictionaries, each containing:
            - event_type: str (required) - The type of event
            - event_data: Dict[str, Any] (required) - The event data dictionary
        hasura_url: Optional Hasura GraphQL endpoint URL. If not provided, uses HASURA_URL env var or default
        admin_secret: Optional Hasura admin secret. If not provided, uses HASURA_GRAPHQL_ADMIN_SECRET env var or default
        batch_size: Optional batch size override. If not provided, uses BULK_EVENTS_BATCH_SIZE env var (default: 1000)
    
    Returns:
        Dictionary containing aggregated results from all batches:
        {
            "status": "success" | "partial" | "error",
            "total_events": int,
            "events_created": int,
            "events_duplicate": int,
            "events_failed": int,
            "created_event_ids": List[int],
            "duplicate_hashes": List[str],
            "errors": List[Dict],
            "batches_processed": int,
            "data": Dict | None
        }
    
    Raises:
        ValueError: If events list is empty or batch_size is invalid
    """
    # Step 0: Batch size configuration
    if batch_size is None:
        batch_size = int(os.getenv('BULK_EVENTS_BATCH_SIZE', '1000'))
    
    if not isinstance(batch_size, int) or batch_size <= 0:
        error_msg = f"Invalid batch_size: must be a positive integer, got {batch_size}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Step 1: Input validation
    if not isinstance(events, list):
        error_msg = "Invalid events: must be a list"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    if len(events) == 0:
        error_msg = "Invalid events: list cannot be empty"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Validate each event structure
    validation_errors = []
    for idx, event in enumerate(events):
        if not isinstance(event, dict):
            validation_errors.append({
                "index": idx,
                "error": "Event must be a dictionary"
            })
            continue
        
        if 'event_type' not in event:
            validation_errors.append({
                "index": idx,
                "error": "Missing required field: event_type"
            })
            continue
        
        if 'event_data' not in event:
            validation_errors.append({
                "index": idx,
                "error": "Missing required field: event_data"
            })
            continue
        
        # Validate event_data can be converted to JSON
        try:
            json.dumps(event['event_data'])
        except (TypeError, ValueError) as e:
            validation_errors.append({
                "index": idx,
                "event_type": event.get('event_type'),
                "error": f"Invalid event_data: cannot be converted to JSON - {str(e)}"
            })
    
    total_events = len(events)
    valid_events = [e for i, e in enumerate(events) if not any(err['index'] == i for err in validation_errors)]
    
    if not valid_events:
        logger.error(f"All {total_events} events failed validation")
        return {
            "status": "error",
            "total_events": total_events,
            "events_created": 0,
            "events_duplicate": 0,
            "events_failed": total_events,
            "created_event_ids": [],
            "duplicate_hashes": [],
            "errors": validation_errors,
            "batches_processed": 0,
            "data": None
        }
    
    # Step 2: Chunking (Built-in)
    chunks = [valid_events[i:i+batch_size] for i in range(0, len(valid_events), batch_size)]
    total_chunks = len(chunks)
    
    logger.info(f"Processing {total_events} events in {total_chunks} batch(es) of size {batch_size}")
    
    # Step 3-6: Process each chunk and aggregate results
    all_created_ids = []
    all_duplicate_hashes = []
    all_errors = validation_errors.copy()
    total_created = 0
    total_duplicate = 0
    total_failed = len(validation_errors)
    batch_results = []
    
    for chunk_idx, chunk in enumerate(chunks, 1):
        try:
            logger.info(f"Processing batch {chunk_idx}/{total_chunks} ({len(chunk)} events)")
            batch_result = _process_batch(chunk, hasura_url=hasura_url, admin_secret=admin_secret)
            batch_results.append(batch_result)
            
            total_created += batch_result['events_created']
            total_duplicate += batch_result['events_duplicate']
            total_failed += batch_result['events_failed']
            all_created_ids.extend(batch_result['created_event_ids'])
            all_duplicate_hashes.extend(batch_result['duplicate_hashes'])
            all_errors.extend(batch_result['errors'])
            
        except Exception as e:
            error_msg = f"Error processing batch {chunk_idx}: {str(e)}"
            logger.error(error_msg)
            total_failed += len(chunk)
            all_errors.append({
                "batch": chunk_idx,
                "error": error_msg
            })
    
    # Step 7: Result aggregation
    # Determine overall status
    if total_failed == 0:
        if total_created > 0:
            overall_status = "success"
        elif total_duplicate > 0:
            # All events were duplicates (no failures, no new events)
            overall_status = "success"
        else:
            # No events processed (shouldn't happen, but handle edge case)
            overall_status = "error"
    elif total_created > 0 or total_duplicate > 0:
        # Some events succeeded/duplicated, but some failed
        overall_status = "partial"
    else:
        # All events failed
        overall_status = "error"
    
    logger.info(
        f"Bulk write complete: {total_created} created, "
        f"{total_duplicate} duplicates, {total_failed} failed "
        f"across {total_chunks} batch(es)"
    )
    
    return {
        "status": overall_status,
        "total_events": total_events,
        "events_created": total_created,
        "events_duplicate": total_duplicate,
        "events_failed": total_failed,
        "created_event_ids": all_created_ids,
        "duplicate_hashes": all_duplicate_hashes,
        "errors": all_errors,
        "batches_processed": total_chunks,
        "data": {
            "batch_results": batch_results
        }
    }

