import json
import os
import time
from typing import Dict

import requests
from dagster import AssetExecutionContext


def _get_ditio_auth_token(
    context: AssetExecutionContext,
    *,
    cache_key: str,
    auth_url_env_var: str,
    client_id_env_var: str,
    client_secret_env_var: str,
    client_label: str,
) -> str:
    """Generic OAuth token retriever with per-client caching."""
    _DITIO_AUTH_TOKEN_CACHE: Dict[str, Dict[str, float]] = getattr(
        _get_ditio_auth_token, "_cache", {}
    )
    _DITIO_AUTH_TOKEN_CACHE_DURATION: float = getattr(
        _get_ditio_auth_token, "_cache_duration", 3600
    )

    # Persist cache on the function object to avoid module-level globals while still
    # enabling reuse across callers.
    setattr(_get_ditio_auth_token, "_cache", _DITIO_AUTH_TOKEN_CACHE)
    setattr(_get_ditio_auth_token, "_cache_duration", _DITIO_AUTH_TOKEN_CACHE_DURATION)

    current_time = time.time()
    cache_entry = _DITIO_AUTH_TOKEN_CACHE.get(cache_key)

    if cache_entry and current_time < cache_entry.get("expires_at", 0):
        context.log.debug(f"Using cached Ditio auth token for {client_label} client")
        return cache_entry["token"]

    ditio_auth_token_url = os.getenv(auth_url_env_var)
    client_id = os.getenv(client_id_env_var)
    client_secret = os.getenv(client_secret_env_var)

    if not ditio_auth_token_url:
        raise ValueError(f"{auth_url_env_var} environment variable is not set")
    if not client_id:
        raise ValueError(f"{client_id_env_var} environment variable is not set")
    if not client_secret:
        raise ValueError(f"{client_secret_env_var} environment variable is not set")

    context.log.info(
        f"Fetching new Ditio auth token for {client_label} client using client credentials flow"
    )

    try:
        payload = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        context.log.debug(f"Requesting token from: {ditio_auth_token_url}")
        response = requests.post(
            ditio_auth_token_url,
            data=payload,
            headers=headers,
            timeout=30,
        )

        if response.status_code == 400:
            try:
                error_response = response.json()
                error_details = json.dumps(error_response, indent=2)
                context.log.error(
                    f"Ditio auth token request failed with 400 Bad Request for {client_label} client:"
                )
                context.log.error(f"Response: {error_details}")
            except Exception:
                context.log.error(
                    f"Ditio auth token request failed with 400 Bad Request for {client_label} client:"
                )
                context.log.error(f"Response text: {response.text}")

        response.raise_for_status()

        auth_response = response.json()
        token = auth_response.get("access_token")

        if not token:
            raise ValueError("No access_token found in Ditio token response")

        _DITIO_AUTH_TOKEN_CACHE[cache_key] = {
            "token": token,
            "expires_at": current_time + _DITIO_AUTH_TOKEN_CACHE_DURATION,
        }

        context.log.info(
            f"Successfully obtained Ditio auth token for {client_label} client"
        )
        return token

    except requests.exceptions.HTTPError as e:
        context.log.error(
            f"HTTP error getting Ditio auth token for {client_label} client: {e}"
        )
        if hasattr(e, "response") and e.response is not None:
            try:
                error_response = e.response.json()
                error_details = json.dumps(error_response, indent=2)
                context.log.error(f"Error response details: {error_details}")
            except Exception:
                context.log.error(f"Error response text: {e.response.text}")
        raise
    except Exception as e:
        context.log.error(
            f"Failed to get Ditio auth token for {client_label} client: {e}"
        )
        raise


def get_ditio_auth_token(context: AssetExecutionContext) -> str:
    """Return the cached Ditio OAuth token for the default NRC client."""
    return _get_ditio_auth_token(
        context,
        cache_key="nrc",
        auth_url_env_var="DITIO_AUTH_TOKEN_URL",
        client_id_env_var="DITIO_AUTH_CLIENT_ID",
        client_secret_env_var="DITIO_AUTH_CLIENT_SECRET",
        client_label="NRC",
    )


def get_ditio_auth_token_kept(context: AssetExecutionContext) -> str:
    """Return the cached Ditio OAuth token for the KEPT client."""
    return _get_ditio_auth_token(
        context,
        cache_key="kept",
        auth_url_env_var="DITIO_AUTH_TOKEN_URL",
        client_id_env_var="DITIO_AUTH_CLIENT_ID_KEPT",
        client_secret_env_var="DITIO_AUTH_CLIENT_SECRET_KEPT",
        client_label="KEPT",
    )


def get_ditio_auth_token_aoe(context: AssetExecutionContext) -> str:
    """Return the cached Ditio OAuth token for the AØE client."""
    return _get_ditio_auth_token(
        context,
        cache_key="aoe",
        auth_url_env_var="AOE_DITIO_AUTH_TOKEN_URL",
        client_id_env_var="AOE_DITIO_CLIENT_ID",
        client_secret_env_var="AOE_DITIO_CLIENT_SECRET",
        client_label="AOE",
    )

