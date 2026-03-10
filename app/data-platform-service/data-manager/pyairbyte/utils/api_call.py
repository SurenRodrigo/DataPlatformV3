import os
import json
import requests
import logging
from typing import Optional, Dict, Any, Literal

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Supported HTTP methods
HTTPMethod = Literal['GET', 'POST', 'PUT', 'PATCH', 'DELETE']

# Import log_event_processing for automatic logging
try:
    from pyairbyte.utils.event_store import log_event_processing
except ImportError:
    # Handle case where event_store is not available
    log_event_processing = None
    logger.warning("log_event_processing not available - automatic logging will be disabled")


def _log_api_result(
    result: Dict[str, Any],
    event_id: int,
    auto_log: bool,
    hasura_url: Optional[str],
    admin_secret: Optional[str],
    integration_url: Optional[str] = None,
    integration_request_method: Optional[str] = None,
    integration_payload: Optional[Dict[str, Any]] = None
) -> None:
    """
    Helper function to automatically log API call result to event store.
    
    Args:
        result: The API call result dictionary (will be modified in-place)
        event_id: The event ID
        auto_log: Whether to enable automatic logging
        hasura_url: Optional Hasura URL for logging
        admin_secret: Optional admin secret for logging
        integration_url: Optional API endpoint URL used for the integration call
        integration_request_method: Optional HTTP method used (GET, POST, PUT, PATCH, DELETE)
        integration_payload: Optional request body/payload dictionary sent to the integration API
    """
    # Validate event_id is positive (edge case handling)
    if auto_log and event_id and event_id > 0 and log_event_processing is not None:
        try:
            log_result = log_event_processing(
                event_id=event_id,
                api_call_result=result,
                hasura_url=hasura_url,
                admin_secret=admin_secret,
                integration_url=integration_url,
                integration_request_method=integration_request_method,
                integration_payload=integration_payload
            )
            
            # Add logging info to result
            result['log_result'] = {
                'status': log_result.get('status'),
                'log_id': log_result.get('log_id'),
                'action': log_result.get('action'),
                'processed_status': log_result.get('processed_status')
            }
            
            logger.info(
                f"Event {event_id} processing logged: {log_result.get('action')} "
                f"(log_id: {log_result.get('log_id')}, status: {log_result.get('processed_status')})"
            )
            
        except Exception as e:
            # Logging failure should not affect API call result
            error_msg = f"Failed to log event processing for event_id {event_id}: {str(e)}"
            logger.warning(error_msg)
            result['log_error'] = error_msg


def call_api_for_event_processing(
    event_id: int,
    method: HTTPMethod,
    url: str,
    body: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: Optional[int] = None,
    auto_log: bool = True,
    hasura_url: Optional[str] = None,
    admin_secret: Optional[str] = None
) -> Dict[str, Any]:
    """
    Makes an HTTP API call (POST/PATCH/DELETE/PUT/GET) to a given URL and returns standardized results.
    
    Automatically logs the API call result to the event_processed_logs table if auto_log=True (default).
    Logging failures do not affect the API call result - they are logged as warnings.
    
    Args:
        event_id: The ID of the event from event_store table (required for logging)
        method: HTTP method to use (GET, POST, PUT, PATCH, DELETE)
        url: The API endpoint URL
        body: Optional request body as a dictionary (will be JSON serialized)
        headers: Optional custom headers dictionary. If not provided, defaults to Content-Type: application/json
        timeout: Optional request timeout in seconds. Defaults to 30 seconds
        auto_log: If True (default), automatically log the API call result to event_processed_logs table.
                  Set to False to disable automatic logging.
        hasura_url: Optional Hasura GraphQL endpoint URL for logging. If not provided, uses HASURA_URL env var or default
        admin_secret: Optional Hasura admin secret for logging. If not provided, uses HASURA_GRAPHQL_ADMIN_SECRET env var or default
    
    Returns:
        Dictionary with standardized format:
        {
            "status": "success" | "error",
            "status_code": int | None,
            "data": dict | None,
            "error": str | None,
            "response_headers": dict | None,
            "log_result": {  # Only present if auto_log=True and logging succeeded
                "status": "success" | "error",
                "log_id": int | None,
                "action": "inserted" | "updated" | None,
                "processed_status": "SUCCESS" | "FAILED" | None
            } | None,
            "log_error": str | None  # Only present if auto_log=True and logging failed
        }
    
    Note:
        When auto_log=True (default), the integration request details (url, method, body)
        are automatically stored in the event_processed_logs table along with the API call result.
    
    Examples:
        # Basic usage with automatic logging (default)
        result = call_api_for_event_processing(
            event_id=123,
            method="POST",
            url="https://api.example.com/endpoint",
            body={"key": "value"}
        )
        # Logging happens automatically
        if result.get('log_result'):
            print(f"Logged with ID: {result['log_result']['log_id']}")
        
        # Disable automatic logging
        result = call_api_for_event_processing(
            event_id=123,
            method="POST",
            url="https://api.example.com/endpoint",
            auto_log=False
        )
        # No logging occurs
    """
    # Default timeout
    if timeout is None:
        timeout = int(os.getenv('API_CALL_TIMEOUT', '30'))
    
    # Default headers
    if headers is None:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
    else:
        # Ensure Content-Type is set if not provided
        if "Content-Type" not in headers and body is not None:
            headers["Content-Type"] = "application/json"
        if "Accept" not in headers:
            headers["Accept"] = "application/json"
    
    # Prepare request parameters
    request_kwargs = {
        "headers": headers,
        "timeout": timeout
    }
    
    # Add body for methods that support it
    if body is not None and method in ['POST', 'PUT', 'PATCH']:
        request_kwargs["json"] = body
    elif body is not None and method == 'GET':
        # For GET requests, body should be passed as params
        request_kwargs["params"] = body
    
    result = None
    
    try:
        logger.info(f"Making {method} request to {url}")
        if body:
            logger.debug(f"Request body: {json.dumps(body, indent=2)}")
        
        # Make the request based on method
        if method == 'GET':
            response = requests.get(url, **request_kwargs)
        elif method == 'POST':
            response = requests.post(url, **request_kwargs)
        elif method == 'PUT':
            response = requests.put(url, **request_kwargs)
        elif method == 'PATCH':
            response = requests.patch(url, **request_kwargs)
        elif method == 'DELETE':
            response = requests.delete(url, **request_kwargs)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")
        
        # Parse response
        response_data = None
        try:
            # Try to parse as JSON
            response_data = response.json()
        except (ValueError, json.JSONDecodeError):
            # If not JSON, return text
            response_data = {"text": response.text}
        
        # Determine if request was successful (2xx status codes)
        is_success = 200 <= response.status_code < 300
        
        result = {
            "status": "success" if is_success else "error",
            "status_code": response.status_code,
            "data": response_data,
            "error": None if is_success else f"HTTP {response.status_code}: {response.reason}",
            "response_headers": dict(response.headers)
        }
        
        if is_success:
            logger.info(f"API call successful: {method} {url} returned {response.status_code}")
        else:
            logger.warning(f"API call returned error status: {method} {url} returned {response.status_code}")
        
    except requests.exceptions.Timeout:
        error_msg = f"Timeout connecting to {url} (timeout: {timeout}s)"
        logger.error(error_msg)
        result = {
            "status": "error",
            "status_code": None,
            "data": None,
            "error": error_msg,
            "response_headers": None
        }
    except requests.exceptions.ConnectionError as e:
        error_msg = f"Connection error to {url}: {str(e)}"
        logger.error(error_msg)
        result = {
            "status": "error",
            "status_code": None,
            "data": None,
            "error": error_msg,
            "response_headers": None
        }
    except requests.exceptions.HTTPError as e:
        error_msg = f"HTTP error from {url}: {e.response.status_code if e.response else 'Unknown'}"
        logger.error(error_msg)
        
        error_data = None
        if e.response is not None:
            try:
                error_data = e.response.json()
            except:
                error_data = {"text": e.response.text}
        
        result = {
            "status": "error",
            "status_code": e.response.status_code if e.response else None,
            "data": error_data,
            "error": error_msg,
            "response_headers": dict(e.response.headers) if e.response else None
        }
    except requests.exceptions.RequestException as e:
        error_msg = f"Unexpected error making API call to {url}: {str(e)}"
        logger.error(error_msg)
        result = {
            "status": "error",
            "status_code": None,
            "data": None,
            "error": error_msg,
            "response_headers": None
        }
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(error_msg)
        result = {
            "status": "error",
            "status_code": None,
            "data": None,
            "error": error_msg,
            "response_headers": None
        }
    
    # Automatic logging to event store (single point of logging for all paths)
    # Extract integration request details for logging
    _log_api_result(
        result, 
        event_id, 
        auto_log, 
        hasura_url, 
        admin_secret,
        integration_url=url,
        integration_request_method=method,
        integration_payload=body
    )
    
    return result

