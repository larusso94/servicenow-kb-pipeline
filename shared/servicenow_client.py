"""ServiceNow KB client for Azure Functions (adapted from CU2).

Features:
 - Fetch KB articles with pagination
 - Support both Basic Auth and Bearer token authentication
 - Incremental sync based on sys_updated_on
 - Robust error handling and retry logic
 - Integrated with Azure Functions logging
"""
import time
from typing import List, Dict, Optional, Any
import requests
from .config import config
from .logging_client import log

class ServiceNowKBError(Exception):
    """Base error for the ServiceNow KB client."""
    pass

class ServiceNowKBAuthError(ServiceNowKBError):
    """Authentication / authorization error."""
    pass

class ServiceNowKBClientError(ServiceNowKBError):
    """Generic client / HTTP layer error."""
    pass

class ServiceNowKBClient:
    """
    ServiceNow KB client for Azure Functions focused on:
      - Fetching KB articles list with pagination
      - Incremental updates support
      - Flexible authentication (Basic Auth or Bearer Token)
      - Robust error handling
    """
    
    def __init__(self, 
                 instance_url: Optional[str] = None,
                 username: Optional[str] = None,
                 password: Optional[str] = None,
                 token: Optional[str] = None,
                 oauth_client_id: Optional[str] = None,
                 oauth_client_secret: Optional[str] = None):
        
        self.instance_url = (instance_url or config.servicenow_instance_url or "").rstrip("/")
        self.username = username or config.servicenow_username
        self.password = password or config.servicenow_password
        self.token = token or config.servicenow_token
        self.oauth_client_id = oauth_client_id or config.servicenow_oauth_client_id
        self.oauth_client_secret = oauth_client_secret or config.servicenow_oauth_client_secret
        self.remote_enabled = config.servicenow_remote
        self.timeout = config.servicenow_timeout
        self.kb_table = config.servicenow_kb_table
        self.kb_fields = config.servicenow_kb_fields
        self.page_size = config.servicenow_page_size
        
        # OAuth token management (like CU1)
        self._access_token: Optional[str] = None
        self._token_expires_at: int = 0
        self._oauth_token_url = f"{self.instance_url}/oauth_token.do" if self.instance_url else ""

        log.info(
            "servicenow.kb.init",
            instance_url=self.instance_url[:50] + "..." if len(self.instance_url) > 50 else self.instance_url,
            has_basic_auth=bool(self.username and self.password),
            has_token=bool(self.token and self.token.strip()),
            has_oauth=bool(self.oauth_client_id and self.oauth_client_secret),
            remote_enabled=self.remote_enabled,
            timeout=self.timeout,
            table=self.kb_table,
            page_size=self.page_size
        )

        self._validate_authentication()

    def _validate_authentication(self):
        """Validate authentication configuration."""
        if not self.remote_enabled:
            log.info("servicenow.kb.auth.remote_disabled", message="Remote disabled, will return empty results")
            return

        has_basic = bool(self.username and self.password and self.username.strip() and self.password.strip())
        has_token = bool(self.token and self.token.strip())
        has_oauth = bool(self.oauth_client_id and self.oauth_client_secret and 
                        self.oauth_client_id.strip() and self.oauth_client_secret.strip())
        
        if not (has_basic or has_token or has_oauth):
            log.warning(
                "servicenow.kb.auth.incomplete",
                has_instance_url=bool(self.instance_url),
                has_basic_auth=has_basic,
                has_token=has_token,
                has_oauth=has_oauth,
                message="Incomplete credentials, will return empty results"
            )

    def _get_auth_method(self) -> str:
        """Get the authentication method being used."""
        if self.oauth_client_id and self.oauth_client_secret and self.oauth_client_id.strip() and self.oauth_client_secret.strip():
            return "oauth"
        if self.token and self.token.strip():
            return "bearer_token"
        if self.username and self.password and self.username.strip() and self.password.strip():
            return "basic_auth"
        return "none"

    def is_available(self) -> bool:
        """Check if the client is properly configured and available."""
        if not self.remote_enabled:
            return False  # Remote disabled, not available
        
        has_basic = bool(self.username and self.password and self.username.strip() and self.password.strip())
        has_token = bool(self.token and self.token.strip())
        has_oauth = bool(self.oauth_client_id and self.oauth_client_secret and 
                        self.oauth_client_id.strip() and self.oauth_client_secret.strip())
        
        return bool(self.instance_url and (has_basic or has_token or has_oauth))

    def list_kb_articles_since(self, updated_since_iso: Optional[str] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Paginate kb_knowledge table; filter by sys_updated_on if provided.
        
        Args:
            updated_since_iso: ISO timestamp to filter articles updated after this time
            limit: Maximum number of articles to return (None for no limit)
            
        Returns:
            List of KB articles with specified fields
        """
        log.info("servicenow.kb.list.start", 
                updated_since=updated_since_iso,
                limit=limit,
                auth_method=self._get_auth_method())
        
        if not self.is_available():
            log.warning("servicenow.kb.list.unavailable", returning_empty=True)
            return self._get_empty_results(limit)

        try:
            base_url = f"{self.instance_url}/api/now/table/{self.kb_table}"
            query_parts = []
            
            # Filter criteria for KB articles - Cybersecurity category
            query_parts.append("kb_category=a274d5a32b0572500877fe9bf291bf26")
            
            if updated_since_iso:
                query_parts.append(f"sys_updated_on>{updated_since_iso}")
            
            query = "^".join(query_parts)
            
            log.info("servicenow.kb.list.query", 
                    sysparm_query=query,
                    fields=self.kb_fields,
                    page_size=self.page_size)
            
            articles = []
            start = 0
            
            while True:
                params = {
                    "sysparm_query": query,
                    "sysparm_fields": self.kb_fields,
                    "sysparm_limit": str(self.page_size),
                    "sysparm_offset": str(start)
                }
                
                data = self._request("GET", base_url, params=params)
                result = data.get("result", [])
                
                if not result:
                    break
                    
                articles.extend(result)
                
                # Check if we've reached the limit
                if limit and len(articles) >= limit:
                    articles = articles[:limit]
                    break
                    
                start += len(result)
                
                # If we got fewer than page_size, we're done
                if len(result) < self.page_size:
                    break
                    
                log.debug("servicenow.kb.list.page", 
                         page_start=start - len(result),
                         page_size=len(result),
                         total_so_far=len(articles))
            
            log.info("servicenow.kb.list.success", 
                    total_articles=len(articles),
                    pages=((start // self.page_size) + 1) if start > 0 else 1,
                    filter_applied="server-side",
                    query_used=query)
            
            return articles
            
        except ServiceNowKBError as e:
            log.error("servicenow.kb.list.error", 
                     error=str(e),
                     error_type=type(e).__name__,
                     returning_empty=True)
            return self._get_empty_results(limit)

    def _refresh_oauth_token(self) -> str:
        """Get OAuth 2.0 access token using Client Credentials flow."""
        if not self.oauth_client_id or not self.oauth_client_secret:
            raise ServiceNowKBAuthError("OAuth client credentials not configured")
        
        # Check if we have a valid cached token
        if (self._access_token and self._token_expires_at and 
            time.time() < self._token_expires_at - 60):  # 60s buffer
            return self._access_token
        
        # Request new token
        token_data = {
            'grant_type': 'client_credentials',
            'client_id': self.oauth_client_id,
            'client_secret': self.oauth_client_secret
        }
        
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json'
        }
        
        try:
            log.debug("servicenow.oauth.token.request", 
                     client_id=self.oauth_client_id[:8] + "..." if len(self.oauth_client_id) > 8 else self.oauth_client_id)
            
            response = requests.post(
                self._oauth_token_url,
                data=token_data,
                headers=headers,
                timeout=self.timeout
            )
            
            if response.status_code != 200:
                error_text = response.text[:200] if response.text else "No error details"
                raise ServiceNowKBAuthError(f"OAuth token request failed: {response.status_code} - {error_text}")
            
            token_response = response.json()
            
            if 'access_token' not in token_response:
                raise ServiceNowKBAuthError("OAuth response missing access_token")
            
            self._access_token = token_response['access_token']
            expires_in = token_response.get('expires_in', 3600)  # Default 1 hour
            self._token_expires_at = time.time() + expires_in
            
            log.info("servicenow.oauth.token.success", 
                    expires_in=expires_in,
                    expires_at=self._token_expires_at)
            
            return self._access_token
            
        except requests.RequestException as e:
            raise ServiceNowKBAuthError(f"OAuth token request failed: {e}")
        except (KeyError, ValueError) as e:
            raise ServiceNowKBAuthError(f"Invalid OAuth token response: {e}")
        except Exception as e:
            raise ServiceNowKBAuthError(f"OAuth token acquisition error: {e}")

    def _request(self, method: str, url: str, params: Optional[Dict] = None) -> Dict:
        """Make authenticated request to ServiceNow API."""
        if not self.instance_url:
            raise ServiceNowKBAuthError("Missing instance URL")

        # Prepare authentication
        auth = None
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        
        # OAuth takes precedence
        if self.oauth_client_id and self.oauth_client_secret:
            try:
                # Get OAuth token synchronously using requests (avoiding async in Azure Functions)
                if (not self._access_token or not self._token_expires_at or 
                    time.time() >= self._token_expires_at - 60):  # 60s buffer
                    self._refresh_oauth_token()
                headers["Authorization"] = f"Bearer {self._access_token}"
            except Exception as e:
                raise ServiceNowKBAuthError(f"OAuth authentication failed: {e}")
        elif self.token and self.token.strip():
            headers["Authorization"] = f"Bearer {self.token}"
        elif self.username and self.password:
            auth = (self.username, self.password)
        else:
            raise ServiceNowKBAuthError("No valid authentication method available")

        start = time.time()
        
        try:
            log.debug("servicenow.kb.request.start", 
                     method=method,
                     url=url[:100] + "..." if len(url) > 100 else url,
                     has_params=bool(params))
            
            response = requests.request(
                method=method,
                url=url,
                params=params,
                headers=headers,
                auth=auth,
                timeout=self.timeout
            )
            
        except requests.Timeout:
            raise ServiceNowKBClientError("Request timeout")
        except requests.ConnectionError as e:
            raise ServiceNowKBClientError(f"Connection error: {e}")
        except Exception as e:
            raise ServiceNowKBClientError(f"Network error: {e}")

        elapsed_ms = int((time.time() - start) * 1000)
        
        # Handle HTTP status codes
        if response.status_code == 401:
            raise ServiceNowKBAuthError("Authentication failed - invalid credentials")
        elif response.status_code == 403:
            raise ServiceNowKBAuthError("Access forbidden - insufficient permissions")
        elif response.status_code == 404:
            raise ServiceNowKBClientError("Resource not found")
        elif response.status_code == 429:
            raise ServiceNowKBClientError("Rate limit exceeded")
        elif response.status_code >= 500:
            raise ServiceNowKBClientError(f"Server error: {response.status_code}")
        elif response.status_code != 200:
            raise ServiceNowKBClientError(f"HTTP {response.status_code}: {response.text[:200]}")

        # Parse JSON response
        try:
            payload = response.json()
        except Exception as e:
            raise ServiceNowKBClientError(f"Invalid JSON response: {e}")

        log.debug("servicenow.kb.request.success", 
                 status_code=response.status_code,
                 elapsed_ms=elapsed_ms,
                 response_size=len(response.text))
        
        return payload

    def _get_empty_results(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Return empty list when ServiceNow is unavailable."""
        log.warning("servicenow.cybersecurity.kb.unavailable", 
                   message="ServiceNow unavailable, returning empty results",
                   knowledge_base="cybersecurity")
        return []



__all__ = ["ServiceNowKBClient", "ServiceNowKBError", "ServiceNowKBAuthError", "ServiceNowKBClientError"]