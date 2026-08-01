"""
HttpConnector — read from authenticated REST APIs using DuckDB HTTP secrets.

Supports:
- Bearer token authentication
- Custom headers (API keys, etc.)
- Basic auth
- Response path extraction (for nested JSON)
- Format auto-detection (json, csv, parquet) with explicit override
"""

import time

import httpx

from connectors import Connector, ConnectionStatus, register


@register
class HttpConnector(Connector):
    """Read from REST APIs with authentication via DuckDB HTTP secrets."""

    @classmethod
    def connector_type(cls) -> str:
        return "http"

    @classmethod
    def metadata(cls) -> dict:
        return {
            "name": "HTTP API",
            "description": "Read from REST APIs with authentication (Bearer, API key, Basic)",
            "icon": "globe",
            "version": "0.1.0",
            "author": "Dataflow",
            "capabilities": {"source"},
            "config_schema": {
                "url": {
                    "type": "string",
                    "required": True,
                    "label": "API URL",
                    "placeholder": "https://api.example.com/v1/data",
                    "description": "Full URL of the API endpoint",
                },
                "auth": {
                    "type": "object",
                    "required": False,
                    "label": "Authentication",
                    "properties": {
                        "type": {
                            "type": "enum",
                            "options": ["none", "bearer", "basic", "headers"],
                            "default": "none",
                            "label": "Auth Type",
                        },
                        "token": {
                            "type": "secret",
                            "label": "Bearer Token",
                            "description": "Used when auth type is 'bearer'",
                            "show_when": {"type": "bearer"},
                        },
                        "username": {
                            "type": "string",
                            "label": "Username",
                            "show_when": {"type": "basic"},
                        },
                        "password": {
                            "type": "secret",
                            "label": "Password",
                            "show_when": {"type": "basic"},
                        },
                        "headers": {
                            "type": "key_value",
                            "label": "Custom Headers",
                            "description": "Key-value pairs sent as HTTP headers",
                            "show_when": {"type": "headers"},
                        },
                    },
                },
                "format": {
                    "type": "enum",
                    "options": ["json", "csv", "parquet"],
                    "default": "json",
                    "required": False,
                    "label": "Response Format",
                    "description": "Format of the API response. Auto-detected from URL if not set.",
                },
                "response_path": {
                    "type": "string",
                    "required": False,
                    "label": "Response Path",
                    "placeholder": "data.items",
                    "description": "Dot-notation path to the array in the JSON response (e.g. 'data.items'). "
                    "Leave empty if the response is a top-level array.",
                },
            },
        }

    def read(self, conn, params: dict) -> str:
        """Create DuckDB HTTP secret and return a read expression."""
        url = params["url"]
        auth = params.get("auth", {})
        fmt = params.get("format", self._detect_format(url))
        response_path = params.get("response_path")

        conn.execute("INSTALL httpfs; LOAD httpfs;")

        # Create DuckDB HTTP secret based on auth config
        auth_type = auth.get("type", "none") if auth else "none"
        self._create_secret(conn, url, auth_type, auth)

        # Build reader expression
        reader = self._reader_for_format(fmt)
        base_expr = f"{reader}('{url}')"

        # If response_path is specified, extract nested data
        if response_path and fmt == "json":
            # Build nested access: "data.items" -> response.data.items
            # Use unnest to flatten the array into rows
            nested_fields = response_path.split(".")
            access_chain = base_expr
            for field in nested_fields:
                access_chain = f"(SELECT unnest({field}) FROM {access_chain})"
            return access_chain

        return base_expr

    def test_connection(self, params: dict) -> ConnectionStatus:
        """Make a HEAD/GET request to validate the endpoint is reachable."""
        url = params.get("url", "")
        auth = params.get("auth", {})

        if not url:
            return ConnectionStatus(connected=False, message="No URL provided")

        headers = self._build_request_headers(auth)

        start = time.time()
        try:
            # Try HEAD first (cheaper), fall back to GET
            response = httpx.head(url, headers=headers, timeout=10, follow_redirects=True)
            if response.status_code == 405:  # Method not allowed, try GET
                response = httpx.get(url, headers=headers, timeout=10, follow_redirects=True)

            latency = round((time.time() - start) * 1000, 1)

            if response.status_code < 400:
                return ConnectionStatus(
                    connected=True,
                    message=f"HTTP {response.status_code} OK",
                    latency_ms=latency,
                    details={
                        "status_code": response.status_code,
                        "content_type": response.headers.get("content-type", "unknown"),
                    },
                )
            else:
                return ConnectionStatus(
                    connected=False,
                    message=f"HTTP {response.status_code}: {response.reason_phrase}",
                    latency_ms=latency,
                    details={"status_code": response.status_code},
                )
        except httpx.TimeoutException:
            latency = round((time.time() - start) * 1000, 1)
            return ConnectionStatus(
                connected=False, message="Connection timed out (10s)", latency_ms=latency
            )
        except httpx.ConnectError as e:
            latency = round((time.time() - start) * 1000, 1)
            return ConnectionStatus(
                connected=False, message=f"Connection failed: {e}", latency_ms=latency
            )
        except Exception as e:
            latency = round((time.time() - start) * 1000, 1)
            return ConnectionStatus(
                connected=False, message=f"Error: {e}", latency_ms=latency
            )

    # --- Private helpers ---

    @staticmethod
    def _create_secret(conn, url: str, auth_type: str, auth: dict):
        """Create a DuckDB HTTP secret for the given URL and auth config."""
        if auth_type == "bearer":
            token = auth.get("token", "")
            conn.execute(f"""
                CREATE OR REPLACE SECRET http_auth (
                    TYPE HTTP,
                    BEARER_TOKEN '{token}',
                    SCOPE '{url}'
                )
            """)
        elif auth_type == "basic":
            username = auth.get("username", "")
            password = auth.get("password", "")
            # Basic auth is sent as a header via EXTRA_HTTP_HEADERS
            import base64
            credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
            conn.execute(f"""
                CREATE OR REPLACE SECRET http_auth (
                    TYPE HTTP,
                    EXTRA_HTTP_HEADERS MAP {{
                        'Authorization': 'Basic {credentials}'
                    }},
                    SCOPE '{url}'
                )
            """)
        elif auth_type == "headers":
            headers = auth.get("headers", {})
            if headers:
                header_entries = ", ".join(
                    f"'{k}': '{v}'" for k, v in headers.items()
                )
                conn.execute(f"""
                    CREATE OR REPLACE SECRET http_auth (
                        TYPE HTTP,
                        EXTRA_HTTP_HEADERS MAP {{
                            {header_entries}
                        }},
                        SCOPE '{url}'
                    )
                """)
        # auth_type == "none" — no secret needed

    @staticmethod
    def _build_request_headers(auth: dict) -> dict:
        """Build headers dict for httpx test requests (mirrors DuckDB secret logic)."""
        headers = {}
        auth_type = auth.get("type", "none") if auth else "none"

        if auth_type == "bearer":
            headers["Authorization"] = f"Bearer {auth.get('token', '')}"
        elif auth_type == "basic":
            import base64
            username = auth.get("username", "")
            password = auth.get("password", "")
            credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
            headers["Authorization"] = f"Basic {credentials}"
        elif auth_type == "headers":
            headers.update(auth.get("headers", {}))

        return headers

    @staticmethod
    def _detect_format(url: str) -> str:
        """Detect format from URL extension, default to json."""
        lower = url.lower().split("?")[0].split("#")[0]
        if lower.endswith(".parquet"):
            return "parquet"
        if lower.endswith((".csv", ".tsv")):
            return "csv"
        return "json"

    @staticmethod
    def _reader_for_format(fmt: str) -> str:
        """Return the DuckDB reader function for a given format."""
        if fmt == "parquet":
            return "read_parquet"
        if fmt == "csv":
            return "read_csv"
        return "read_json"
