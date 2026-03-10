import os
import json
import requests
import logging
from typing import Optional, Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def query_graphql_api(
    query: str,
    variables: Optional[Dict[str, Any]] = None,
    hasura_url: Optional[str] = None,
    admin_secret: Optional[str] = None
) -> Dict[str, Any]:
    """
    Executes a GraphQL query against the Hasura GraphQL API.
    
    Args:
        query: The GraphQL query string to execute
        variables: Optional dictionary of variables to pass with the query
        hasura_url: Optional Hasura GraphQL endpoint URL. If not provided, uses HASURA_URL env var or default
        admin_secret: Optional Hasura admin secret. If not provided, uses HASURA_GRAPHQL_ADMIN_SECRET env var or default
    
    Returns:
        Dictionary containing the GraphQL response data
        
    Raises:
        requests.exceptions.RequestException: If the GraphQL request fails
        ValueError: If the response contains errors
    """
    # Get Hasura configuration from environment variables with defaults
    if hasura_url is None:
        hasura_url = os.getenv('HASURA_URL', 'http://hasura:8080')
    
    # Ensure the URL includes the /v1/graphql path
    if not hasura_url.endswith('/v1/graphql'):
        hasura_url = f"{hasura_url.rstrip('/')}/v1/graphql"
    
    if admin_secret is None:
        admin_secret = os.getenv('HASURA_GRAPHQL_ADMIN_SECRET', 'admin')
    
    # Prepare the request
    headers = {
        "Content-Type": "application/json",
        "x-hasura-admin-secret": admin_secret
    }
    
    payload = {
        "query": query
    }
    
    # Add variables if provided
    if variables:
        payload["variables"] = variables
    
    try:
        logger.info(f"Executing GraphQL query against {hasura_url}")
        response = requests.post(hasura_url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        
        # Get the JSON response
        result = response.json()
        
        # Check for GraphQL errors in the response
        if 'errors' in result and result['errors']:
            error_messages = [error.get('message', str(error)) for error in result['errors']]
            error_str = '; '.join(error_messages)
            logger.error(f"GraphQL query returned errors: {error_str}")
            raise ValueError(f"GraphQL query failed: {error_str}")
        
        logger.info("GraphQL query executed successfully")
        return result
        
    except requests.exceptions.Timeout:
        logger.error(f"Timeout connecting to Hasura at {hasura_url}")
        raise
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Connection error to Hasura at {hasura_url}: {str(e)}")
        raise
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error from Hasura: {e.response.status_code if e.response else 'Unknown'}")
        if e.response is not None:
            try:
                error_body = e.response.json()
                logger.error(f"Error response: {json.dumps(error_body, indent=2)}")
            except:
                logger.error(f"Error response body: {e.response.text}")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"Unexpected error executing GraphQL query: {str(e)}")
        raise

