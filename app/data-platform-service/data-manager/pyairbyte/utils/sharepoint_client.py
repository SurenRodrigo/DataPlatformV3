"""
SharePoint/Graph download utilities.

This module provides two clients:
 - SharePointClient (legacy) using SharePoint REST API (ClientContext)
 - SharePointGraphClient (recommended) using Microsoft Graph with MSAL

Use SharePointGraphClient to avoid 403 errors in tenants enforcing Microsoft Graph.
"""

from __future__ import annotations

import io
import os
import time
from typing import Callable, Optional

import requests
from msal import ConfidentialClientApplication
from office365.graph_client import GraphClient
from office365.runtime.auth.client_credential import ClientCredential
from office365.sharepoint.client_context import ClientContext


class SharePointClient:
    """Legacy SharePoint REST client (kept for backward compatibility).

    Note: Many tenants now require Microsoft Graph. Prefer SharePointGraphClient.
    """

    def __init__(self, site_url: str, client_id: str, client_secret: str):
        if not (site_url and client_id and client_secret):
            raise ValueError("Missing SharePoint credentials or site URL")

        self.ctx = ClientContext(site_url).with_credentials(
            ClientCredential(client_id, client_secret)
        )

        web = self.ctx.web
        self.ctx.load(web)
        self.ctx.execute_query()
        # store web and server-relative url for later use
        self.web = web
        self.site_relative_url = web.properties.get("ServerRelativeUrl", "")
        print(f"Connected to SharePoint site: {web.properties['Title']}")

    def download_file(self, relative_url: str) -> bytes:
        """Download a file via SharePoint REST and return content as bytes."""
        try:
            if not relative_url.startswith("/"):
                prefix = (
                    self.site_relative_url.rstrip("/") if hasattr(self, "site_relative_url") else ""
                )
                relative_url = (
                    f"{prefix}/{relative_url.lstrip('/')}" if prefix else f"/{relative_url.lstrip('/')}"
                )

            file = self.ctx.web.get_file_by_server_relative_url(relative_url)
            file_obj = io.BytesIO()
            file.download(file_obj)
            self.ctx.execute_query()

            file_obj.seek(0)
            content = file_obj.read()
            if not isinstance(content, (bytes, bytearray)):
                raise TypeError(f"Downloaded content is not bytes but {type(content)}")

            print(f"Downloaded file: {relative_url} (Size: {len(content)} bytes)")
            return content

        except Exception as e:
            print(f"Error downloading file: {str(e)}")
            raise


class SharePointGraphClient:
    """SharePoint client backed by Microsoft Graph using MSAL client credentials.

    Typical usage:
        client = SharePointGraphClient(tenant_id, client_id, client_secret)
        content = client.download_file_bytes(hostname, site_path, drive_name, item_path)

    Where:
      - hostname: e.g., "contoso.sharepoint.com"
      - site_path: e.g., "MySite" (for /sites/MySite)
      - drive_name: the document library name (e.g., "Documents" or custom)
      - item_path: folder and file path within the drive (e.g., "Folder1/Report.xlsx")
    """

    GRAPH_BASE = "https://graph.microsoft.com/v1.0"

    def __init__(
        self,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        scope: str = "https://graph.microsoft.com/.default",
    ):
        if not (tenant_id and client_id and client_secret):
            raise ValueError("tenant_id, client_id, and client_secret are required")

        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.scopes = [scope]

        # MSAL app for token acquisition
        self._msal_app = ConfidentialClientApplication(
            client_id=self.client_id,
            client_credential=self.client_secret,
            authority=f"https://login.microsoftonline.com/{self.tenant_id}",
        )

        # GraphClient from Office365 library using token acquisition callback
        def _acquire_token() -> dict:
            token = self._acquire_access_token()
            return {"access_token": token}

        self._graph = GraphClient(_acquire_token)

    # -----------------------------
    # Public API
    # -----------------------------
    def download_file_bytes(
        self,
        hostname: str,
        site_path: str,
        drive_name: str,
        item_path: str,
        *,
        max_retries: int = 5,
    ) -> bytes:
        """Download a file from SharePoint via Graph and return raw bytes.

        Implements: GET /sites/{hostname}:/sites/{site_path} -> id
                    GET /sites/{site_id}/drives -> pick by name
                    GET /drives/{drive_id}/root:/{item_path}
                    GET /drives/{drive_id}/items/{item_id}/content
        """
        token = self._acquire_access_token()

        site_id = self._resolve_site_id(token, hostname, site_path)
        drive_id = self._resolve_drive_id(token, site_id, drive_name)
        item_id = self._resolve_item_id_by_path(token, drive_id, item_path)

        # Prefer Office365 GraphClient for the actual content download
        # Fallback to raw request if needed
        buf = io.BytesIO()
        try:
            # Use GraphClient to stream bytes
            # Equivalent to: GET /drives/{drive_id}/items/{item_id}/content
            self._graph.drives[drive_id].items[item_id].download(buf).execute_query()
            buf.seek(0)
            return buf.read()
        except Exception:
            # Fallback to raw HTTP request for reliability
            content_url = f"{self.GRAPH_BASE}/drives/{drive_id}/items/{item_id}/content"
            return self._download_with_retries(content_url, token, max_retries)

    # -----------------------------
    # Internals
    # -----------------------------
    def _acquire_access_token(self) -> str:
        result = self._msal_app.acquire_token_silent(self.scopes, account=None)
        if not result:
            result = self._msal_app.acquire_token_for_client(scopes=self.scopes)
        if "access_token" not in result:
            raise RuntimeError(f"Failed to obtain access token: {result}")
        return result["access_token"]

    def _download_with_retries(self, url: str, token: str, max_retries: int) -> bytes:
        backoff = 1.0
        last_exc: Optional[Exception] = None
        headers = {
            "Authorization": f"Bearer {token}",
        }
        for _ in range(max_retries):
            try:
                resp = requests.get(url, headers=headers, timeout=30)
                if resp.status_code in (429, 500, 502, 503, 504):
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 10)
                    continue
                resp.raise_for_status()
                return resp.content
            except Exception as e:
                last_exc = e
                time.sleep(backoff)
                backoff = min(backoff * 2, 10)
        raise RuntimeError(f"Content download failed after retries: {url} ({last_exc})")

    def _resolve_site_id(self, token: str, hostname: str, site_path: str) -> str:
        # GET /sites/{hostname}:/sites/{sitePath}
        url = f"{self.GRAPH_BASE}/sites/{hostname}:/sites/{site_path}"
        js = self._req("GET", url, token)
        site_id = js.get("id")
        if not site_id:
            raise RuntimeError(f"Could not resolve site id for {hostname}/sites/{site_path}")
        return site_id

    def _resolve_drive_id(self, token: str, site_id: str, drive_name: str) -> str:
        # GET /sites/{site-id}/drives
        url = f"{self.GRAPH_BASE}/sites/{site_id}/drives"
        js = self._req("GET", url, token)
        for d in js.get("value", []):
            if d.get("name") == drive_name:
                return d.get("id")
        available = [d.get("name") for d in js.get("value", [])]
        raise RuntimeError(f"Drive '{drive_name}' not found under site {site_id}. Available: {available}")

    def _resolve_item_id_by_path(self, token: str, drive_id: str, item_path: str) -> str:
        # GET /drives/{drive-id}/root:/{item_path}
        url = f"{self.GRAPH_BASE}/drives/{drive_id}/root:/{item_path}"
        js = self._req("GET", url, token)
        item_id = js.get("id")
        if not item_id:
            raise RuntimeError(f"Could not resolve item id for path '{item_path}'")
        return item_id

    def _req(self, method: str, url: str, token: str) -> dict:
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        resp = requests.request(method, url, headers=headers, timeout=30)
        if not resp.ok:
            # Enrich 4xx/5xx with response body for easier debugging
            detail = None
            try:
                detail = resp.json()
            except ValueError:
                # Fall back to raw text (truncate to avoid noisy logs)
                detail = (resp.text or "").strip()[:1500]
            raise requests.HTTPError(
                f"{resp.status_code} {resp.reason} for {url} - {detail}", response=resp
            )
        # Normal path
        try:
            return resp.json()
        except ValueError:
            # Unexpected content type
            raise RuntimeError(f"Expected JSON but received non-JSON response from {url}")
    

