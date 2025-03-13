#!/usr/bin/env python3
# components/data_sources/http_api.py

import json
import urllib.request
import urllib.error
import urllib.parse
import ssl
import hashlib
import gzip
import base64
from typing import Any, Dict, List, Optional, Union

import attrs

from pyvider.hub.decorators import register_data_source
from pyvider.telemetry import logger
from pyvider.exceptions import DataSourceError
from pyvider.resources.context import ResourceContext

from pyvider.schema.pvfactory import (
    a_str, a_num, a_bool, a_map, a_list, a_obj, s_data_source
)
from pyvider.schema import Schema, Block, StringKind, Attribute, AttributeType, SchemaType

from pyvider.cty import (
    CtyBool, CtyNumber, CtyString,
    CtyList, CtyMap,
    CtyDynamic,
)

@attrs.define(frozen=True)
class HTTPAPIConfig:
    """Configuration for HTTP API data source with advanced options."""
    url: str = attrs.field()
    method: str = attrs.field(default="GET")
    headers: Dict[str, str] = attrs.field(factory=dict)
    body: Optional[str] = attrs.field(default=None)
    query_params: Dict[str, str] = attrs.field(factory=dict)
    timeout: int = attrs.field(default=30)
    follow_redirects: bool = attrs.field(default=True)
    max_redirects: int = attrs.field(default=5)
    verify_ssl: bool = attrs.field(default=True)
    auth_username: Optional[str] = attrs.field(default=None)
    auth_password: Optional[str] = attrs.field(default=None)
    retry_count: int = attrs.field(default=0)
    retry_backoff: int = attrs.field(default=1)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "url": self.url,
            "method": self.method,
            "headers": self.headers,
            "body": self.body,
            "query_params": self.query_params,
            "timeout": self.timeout,
            "follow_redirects": self.follow_redirects,
            "max_redirects": self.max_redirects,
            "verify_ssl": self.verify_ssl,
            "auth_username": self.auth_username,
            "auth_password": self.auth_password,
            "retry_count": self.retry_count,
            "retry_backoff": self.retry_backoff
        }

@attrs.define(frozen=True)
class HTTPAPIState:
    """State representation of HTTP API data source with detailed response information."""
    url: str = attrs.field()
    status_code: int = attrs.field()
    response_body: str = attrs.field()
    response_headers: Dict[str, str] = attrs.field()
    response_time_ms: int = attrs.field(default=0)
    content_type: str = attrs.field(default="")
    content_length: int = attrs.field(default=0)
    response_hash: str = attrs.field(default="")
    redirected_url: Optional[str] = attrs.field(default=None)
    error_message: Optional[str] = attrs.field(default=None)
    parsed_body: Optional[Dict[str, Any]] = attrs.field(default=None)

    def to_dict(self) -> Dict[str, Any]:
        """Convert state to dictionary."""
        return {
            "url": self.url,
            "status_code": self.status_code,
            "response_body": self.response_body,
            "response_headers": self.response_headers,
            "response_time_ms": self.response_time_ms,
            "content_type": self.content_type,
            "content_length": self.content_length,
            "response_hash": self.response_hash,
            "redirected_url": self.redirected_url,
            "error_message": self.error_message,
            "parsed_body": self.parsed_body
        }

@register_data_source("pyvider_http_api")
class HTTPAPIDataSource:

    @staticmethod
    def get_schema():
        """Create the schema for the HTTP API data source with extended capabilities."""
        return s_data_source({
            # Base request parameters
            "url": a_str(required=True, description="URL to make the request to"),
            "method": a_str(default="GET", description="HTTP method (GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS)"),
            "headers": a_map(a_str(), description="HTTP headers to include in the request"),
            "body": a_str(description="Request body for POST/PUT/PATCH requests"),
            "query_params": a_map(a_str(), description="Query parameters to append to the URL"),
            
            # Request behavior options
            "timeout": a_num(default=30, description="Request timeout in seconds"),
            "follow_redirects": a_bool(default=True, description="Whether to follow HTTP redirects"),
            "max_redirects": a_num(default=5, description="Maximum number of redirects to follow"),
            "verify_ssl": a_bool(default=True, description="Whether to verify SSL certificates"),
            
            # Authentication
            "auth_username": a_str(description="Username for basic authentication"),
            "auth_password": a_str(sensitive=True, description="Password for basic authentication"),
            
            # Retry behavior
            "retry_count": a_num(default=0, description="Number of times to retry failed requests"),
            "retry_backoff": a_num(default=1, description="Seconds to wait between retries, doubles after each retry"),
            
            # Response data (computed)
            "status_code": a_num(computed=True, description="HTTP status code from the response"),
            "response_body": a_str(computed=True, description="Response body as text"),
            "response_headers": a_map(a_str(), computed=True, description="Response headers"),
            "response_time_ms": a_num(computed=True, description="Response time in milliseconds"),
            "content_type": a_str(computed=True, description="Content-Type of the response"),
            "content_length": a_num(computed=True, description="Content length in bytes"),
            "response_hash": a_str(computed=True, description="SHA-256 hash of the response body"),
            "redirected_url": a_str(computed=True, description="Final URL after redirects"),
            "error_message": a_str(computed=True, description="Error message if request failed"),
            "parsed_body": a_map(a_str(), computed=True, description="Parsed JSON response (if applicable)")
        })

    async def read(self, ctx: ResourceContext) -> HTTPAPIState:
        """Make HTTP request with advanced options and return detailed response."""
        logger.debug(f"📡📖✅ Reading HTTP API. Context: {ctx}")

        try:
            # Extract config from context
            if isinstance(ctx.config, dict):
                config = ctx.config
            else:
                config = ctx.config.to_dict() if hasattr(ctx.config, "to_dict") else vars(ctx.config)

            # Prepare URL with query parameters
            base_url = config.get("url")
            logger.debug(f"📡📖🔍 Making HTTP request to URL: {base_url}")
            
            query_params = config.get("query_params", {})
            if query_params:
                # Parse the URL
                url_parts = list(urllib.parse.urlparse(base_url))
                # Parse the query string
                query = dict(urllib.parse.parse_qsl(url_parts[4]))
                # Update with our parameters
                query.update(query_params)
                # Rebuild the query string
                url_parts[4] = urllib.parse.urlencode(query)
                # Rebuild the URL
                url = urllib.parse.urlunparse(url_parts)
            else:
                url = base_url

            # Prepare headers
            headers = config.get("headers", {})
            
            # Add Basic Auth if provided
            auth_username = config.get("auth_username")
            auth_password = config.get("auth_password")
            if auth_username and auth_password:
                auth_string = f"{auth_username}:{auth_password}"
                auth_header = f"Basic {base64.b64encode(auth_string.encode()).decode()}"
                headers["Authorization"] = auth_header

            # Prepare request body
            body = config.get("body")
            data = body.encode("utf-8") if body else None
            
            # Create request with all options
            request = urllib.request.Request(
                url=url,
                data=data,
                headers=headers,
                method=config.get("method", "GET")
            )
            
            # Set up SSL context
            ssl_context = None
            if not config.get("verify_ssl", True):
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
                
            # Set up redirect handling
            opener = urllib.request.build_opener()
            if not config.get("follow_redirects", True):
                opener.handler_order = 999  # Don't handle redirects
            else:
                max_redirects = config.get("max_redirects", 5)
                opener.max_redirects = max_redirects
                
            urllib.request.install_opener(opener)
            
            # Get timeout
            timeout = config.get("timeout", 30)
            
            # Prepare for retry logic
            retry_count = config.get("retry_count", 0)
            retry_backoff = config.get("retry_backoff", 1)
            current_retry = 0
            response_time_ms = 0
            redirected_url = None
            error_message = None
            
            # Execute request with retry logic
            while True:
                try:
                    import time
                    start_time = time.time()
                    
                    with urllib.request.urlopen(request, timeout=timeout, context=ssl_context) as response:
                        response_time_ms = int((time.time() - start_time) * 1000)
                        status_code = response.status
                        response_body = response.read()
                        response_headers = dict(response.getheaders())
                        redirected_url = response.url if response.url != url else None
                        
                        # Handle compression
                        if response_headers.get("Content-Encoding") == "gzip":
                            response_body = gzip.decompress(response_body)
                            
                        # Convert to text
                        content_type = response_headers.get("Content-Type", "")
                        charset = "utf-8"
                        if "charset=" in content_type:
                            charset = content_type.split("charset=")[1].split(";")[0].strip()
                            
                        response_body_text = response_body.decode(charset, errors="replace")
                        break  # Success, exit retry loop
                        
                except urllib.error.HTTPError as e:
                    # Handle HTTP errors (4xx, 5xx)
                    response_time_ms = int((time.time() - start_time) * 1000)
                    status_code = e.code
                    response_body = e.read()
                    response_headers = dict(e.headers)
                    
                    # Convert to text
                    try:
                        content_type = response_headers.get("Content-Type", "")
                        charset = "utf-8"
                        if "charset=" in content_type:
                            charset = content_type.split("charset=")[1].split(";")[0].strip()
                        response_body_text = response_body.decode(charset, errors="replace")
                    except Exception:
                        response_body_text = response_body.decode("utf-8", errors="replace")
                        
                    # No retry on 4xx errors
                    if 400 <= status_code < 500:
                        error_message = f"HTTP Error {status_code}: {e.reason}"
                        break
                        
                    # Retry on 5xx errors if retry_count > 0
                    if current_retry < retry_count:
                        current_retry += 1
                        backoff_time = retry_backoff * (2 ** (current_retry - 1))
                        logger.debug(f"📡📖⚠️ HTTP {status_code} error, retrying in {backoff_time}s ({current_retry}/{retry_count})")
                        time.sleep(backoff_time)
                    else:
                        error_message = f"HTTP Error {status_code}: {e.reason} (after {current_retry} retries)"
                        break
                        
                except (urllib.error.URLError, TimeoutError) as e:
                    # Connection or timeout errors
                    if current_retry < retry_count:
                        current_retry += 1
                        backoff_time = retry_backoff * (2 ** (current_retry - 1))
                        logger.debug(f"📡📖⚠️ Connection error: {e}, retrying in {backoff_time}s ({current_retry}/{retry_count})")
                        time.sleep(backoff_time)
                    else:
                        # No more retries
                        status_code = 0
                        response_body_text = ""
                        response_headers = {}
                        error_message = f"Connection error: {e} (after {current_retry} retries)"
                        break
            
            # Calculate hash of response body
            try:
                response_hash = hashlib.sha256(response_body).hexdigest() if 'response_body' in locals() else ""
            except Exception:
                response_hash = ""
                
            # Get content length
            try:
                content_length = int(response_headers.get("Content-Length", len(response_body)))
            except Exception:
                content_length = 0
                
            # Parse JSON response if applicable
            parsed_body = None
            if "application/json" in response_headers.get("Content-Type", "").lower():
                try:
                    parsed_body = json.loads(response_body_text)
                except json.JSONDecodeError:
                    parsed_body = None
            
            # Keep the response headers as a simple map/dict
            # Ensure all header values are strings
            formatted_headers = {
                str(k): str(v) if v is not None else ""
                for k, v in response_headers.items()
            }

            # Create state
            state = HTTPAPIState(
                url=url,
                status_code=status_code,
                response_body=response_body_text,
                response_headers=formatted_headers,
                response_time_ms=response_time_ms,
                content_type=response_headers.get("Content-Type", ""),
                content_length=content_length,
                response_hash=response_hash,
                redirected_url=redirected_url,
                error_message=error_message,
                parsed_body=parsed_body
            )

            logger.debug(f"📡📖✅ HTTP request complete. Status: {status_code}, Time: {response_time_ms}ms")
            return state

        except Exception as e:
            logger.error(f"📡📖❌ Error making HTTP request: {e}", exc_info=True)
            raise DataSourceError(f"Failed to make HTTP request: {e}")

    async def validate(self, config) -> List[str]:
        """Validate data source configuration with detailed checks."""
        logger.debug(f"📡🔍✅ Validating config: {config}")
        diagnostics = []

        # Extract config values
        if isinstance(config, dict):
            url = config.get('url', '')
            method = config.get('method', 'GET')
            timeout = config.get('timeout', 30)
            max_redirects = config.get('max_redirects', 5)
            retry_count = config.get('retry_count', 0)
            retry_backoff = config.get('retry_backoff', 1)
        else:
            url = getattr(config, 'url', '')
            method = getattr(config, 'method', 'GET')
            timeout = getattr(config, 'timeout', 30)
            max_redirects = getattr(config, 'max_redirects', 5)
            retry_count = getattr(config, 'retry_count', 0)
            retry_backoff = getattr(config, 'retry_backoff', 1)

        # Validate URL
        if not url:
            diagnostics.append("URL cannot be empty")
            logger.error("📡🔍❌ URL cannot be empty")
        else:
            try:
                # Check if URL is valid
                result = urllib.parse.urlparse(url)
                if not all([result.scheme, result.netloc]):
                    diagnostics.append(f"Invalid URL format: {url}")
                    logger.error(f"📡🔍❌ Invalid URL format: {url}")
            except Exception as e:
                diagnostics.append(f"Invalid URL: {e}")
                logger.error(f"📡🔍❌ Invalid URL: {e}")

        # Validate HTTP method
        allowed_methods = ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'HEAD', 'OPTIONS']
        if method.upper() not in allowed_methods:
            diagnostics.append(f"Method must be one of: {', '.join(allowed_methods)}")
            logger.error(f"📡🔍❌ Invalid method: {method}")

        # Validate timeout
        if timeout <= 0:
            diagnostics.append("Timeout must be positive")
            logger.error(f"📡🔍❌ Invalid timeout: {timeout}")

        # Validate max_redirects
        if max_redirects < 0:
            diagnostics.append("max_redirects cannot be negative")
            logger.error(f"📡🔍❌ Invalid max_redirects: {max_redirects}")

        # Validate retry settings
        if retry_count < 0:
            diagnostics.append("retry_count cannot be negative")
            logger.error(f"📡🔍❌ Invalid retry_count: {retry_count}")
            
        if retry_backoff <= 0:
            diagnostics.append("retry_backoff must be positive")
            logger.error(f"📡🔍❌ Invalid retry_backoff: {retry_backoff}")

        logger.debug(f"📡🔍✅ Validation complete: {len(diagnostics)} issues found")
        return diagnostics
