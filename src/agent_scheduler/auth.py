"""API key authentication and rate limiting for agent-scheduler.

Provides middleware for the REST API to:
- Authenticate requests via API keys (header or query param)
- Enforce rate limits per key
- Manage API key lifecycle (create, list, revoke, toggle)
"""

from __future__ import annotations

import hashlib
import secrets
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class ApiKeyScope(str, Enum):
    """Permission scopes for API keys."""

    JOBS_READ = "jobs:read"
    JOBS_WRITE = "jobs:write"
    EXECUTIONS_READ = "executions:read"
    EXECUTIONS_WRITE = "executions:write"
    WEBHOOKS_READ = "webhooks:read"
    WEBHOOKS_WRITE = "webhooks:write"
    TEMPLATES_READ = "templates:read"
    TEMPLATES_WRITE = "templates:write"
    ADMIN = "admin"
    ALL = "*"  # Shortcut for all scopes


# Map of scope to what it allows
ALL_SCOPES = [s.value for s in ApiKeyScope if s != ApiKeyScope.ALL]


class ApiKey(BaseModel):
    """An API key for authenticating with the scheduler REST API."""

    key: str = Field(..., description="The full API key (only shown once at creation)")
    name: str = Field(..., min_length=1, description="Human-readable key name")
    scopes: list[str] = Field(
        default_factory=lambda: [ApiKeyScope.ALL.value],
        description="Permission scopes for this key",
    )
    enabled: bool = Field(default=True, description="Whether the key is active")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    last_used_at: Optional[datetime] = Field(default=None)
    request_count: int = Field(default=0, ge=0)

    def has_scope(self, required_scope: str) -> bool:
        """Check if this key has the required scope."""
        if ApiKeyScope.ALL.value in self.scopes or ApiKeyScope.ADMIN.value in self.scopes:
            return True
        return required_scope in self.scopes

    def has_any_scope(self, *scopes: str) -> bool:
        """Check if this key has any of the given scopes."""
        if ApiKeyScope.ALL.value in self.scopes or ApiKeyScope.ADMIN.value in self.scopes:
            return True
        return any(s in self.scopes for s in scopes)


class RateLimitConfig(BaseModel):
    """Rate limiting configuration."""

    max_requests: int = Field(default=100, ge=1, description="Max requests per window")
    window_seconds: int = Field(default=60, ge=1, description="Window duration in seconds")
    enabled: bool = Field(default=True, description="Whether rate limiting is active")


class ApiKeyManager:
    """Manages API key authentication and rate limiting."""

    def __init__(self, store: Any = None, rate_limit_config: Optional[RateLimitConfig] = None) -> None:
        self._store = store
        self.rate_limit = rate_limit_config or RateLimitConfig()

    def set_store(self, store: Any) -> None:
        """Set the backing store (must be SQLiteJobStore for full support)."""
        self._store = store

    def generate_key(self, prefix: str = "ask") -> str:
        """Generate a new API key string.

        Format: {prefix}_{random_hex}
        Example: ask_a1b2c3d4e5f6g7h8
        """
        random_part = secrets.token_hex(24)
        return f"{prefix}_{random_part}"

    def create_api_key(
        self,
        name: str,
        scopes: Optional[list[str]] = None,
        enabled: bool = True,
    ) -> ApiKey:
        """Create a new API key.

        Args:
            name: Human-readable name for the key
            scopes: Permission scopes (default: all access)
            enabled: Whether the key starts active

        Returns:
            ApiKey with the full key value (store this securely!)
        """
        if scopes is None:
            scopes = [ApiKeyScope.ALL.value]

        # Validate scopes
        valid_scopes = set(ALL_SCOPES + [ApiKeyScope.ALL.value])
        for scope in scopes:
            if scope not in valid_scopes:
                raise ValueError(f"Invalid scope: {scope}. Valid scopes: {sorted(valid_scopes)}")

        key_string = self.generate_key()
        api_key = ApiKey(
            key=key_string,
            name=name,
            scopes=scopes,
            enabled=enabled,
        )

        if self._store is not None:
            self._store.save_api_key(
                key=key_string,
                name=name,
                scopes=scopes,
                enabled=enabled,
            )

        return api_key

    def authenticate(self, key_string: str) -> Optional[ApiKey]:
        """Authenticate a request using an API key.

        Args:
            key_string: The API key from the request

        Returns:
            ApiKey if valid and enabled, None otherwise
        """
        if self._store is None:
            return None

        record = self._store.get_api_key(key_string)
        if record is None:
            return None

        if not record["enabled"]:
            return None

        # Update usage stats
        self._store.update_api_key_usage(key_string)

        return ApiKey(
            key=record["key"],
            name=record["name"],
            scopes=record["scopes"],
            enabled=record["enabled"],
            created_at=datetime.fromisoformat(record["created_at"]) if isinstance(record["created_at"], str) else record["created_at"],
            last_used_at=datetime.fromisoformat(record["last_used_at"]) if record["last_used_at"] and isinstance(record["last_used_at"], str) else record["last_used_at"],
            request_count=record["request_count"],
        )

    def check_rate_limit(self, key_string: str) -> tuple[bool, int]:
        """Check if a request is within rate limits.

        Args:
            key_string: The API key to check limits for

        Returns:
            Tuple of (allowed, remaining_requests)
        """
        if not self.rate_limit.enabled:
            return (True, self.rate_limit.max_requests)

        if self._store is None:
            return (True, self.rate_limit.max_requests)

        # Hash the key for storage (don't store raw keys in rate limit table)
        key_hash = hashlib.sha256(key_string.encode()).hexdigest()[:16]

        return self._store.check_rate_limit(
            key_hash=key_hash,
            max_requests=self.rate_limit.max_requests,
            window_seconds=self.rate_limit.window_seconds,
        )

    def list_api_keys(self) -> list[dict]:
        """List all API keys (with masked key values)."""
        if self._store is None:
            return []
        return self._store.list_api_keys()

    def revoke_api_key(self, key_string: str) -> bool:
        """Revoke (delete) an API key."""
        if self._store is None:
            return False
        return self._store.delete_api_key(key_string)

    def toggle_api_key(self, key_string: str, enabled: bool) -> bool:
        """Enable or disable an API key without deleting it."""
        if self._store is None:
            return False
        return self._store.toggle_api_key(key_string, enabled)


def extract_api_key(
    headers: dict[str, str],
    query_params: Optional[dict[str, str]] = None,
) -> Optional[str]:
    """Extract an API key from request headers or query parameters.

    Checks in order:
    1. Authorization: Bearer {key} header
    2. X-API-Key header
    3. api_key query parameter
    """
    # Check Authorization header
    auth_header = headers.get("authorization", "") or headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        key = auth_header[7:].strip()
        if key:
            return key

    # Check X-API-Key header
    api_key = headers.get("x-api-key", "") or headers.get("X-API-Key", "")
    if api_key:
        return api_key.strip()

    # Check query parameter
    if query_params:
        api_key = query_params.get("api_key", "")
        if api_key:
            return api_key.strip()

    return None


# Scope requirements for API endpoints
ENDPOINT_SCOPES: dict[str, list[str]] = {
    # Job endpoints
    "GET /api/v1/jobs": [ApiKeyScope.JOBS_READ.value, ApiKeyScope.ALL.value],
    "POST /api/v1/jobs": [ApiKeyScope.JOBS_WRITE.value, ApiKeyScope.ALL.value],
    "GET /api/v1/jobs/{id}": [ApiKeyScope.JOBS_READ.value, ApiKeyScope.ALL.value],
    "PATCH /api/v1/jobs/{id}": [ApiKeyScope.JOBS_WRITE.value, ApiKeyScope.ALL.value],
    "DELETE /api/v1/jobs/{id}": [ApiKeyScope.JOBS_WRITE.value, ApiKeyScope.ALL.value],
    "POST /api/v1/jobs/{id}/pause": [ApiKeyScope.JOBS_WRITE.value, ApiKeyScope.ALL.value],
    "POST /api/v1/jobs/{id}/resume": [ApiKeyScope.JOBS_WRITE.value, ApiKeyScope.ALL.value],
    "POST /api/v1/jobs/{id}/run": [ApiKeyScope.EXECUTIONS_WRITE.value, ApiKeyScope.ALL.value],
    "GET /api/v1/jobs/{id}/next-run": [ApiKeyScope.JOBS_READ.value, ApiKeyScope.ALL.value],
    "GET /api/v1/jobs/{id}/dependencies": [ApiKeyScope.JOBS_READ.value, ApiKeyScope.ALL.value],

    # Execution endpoints
    "GET /api/v1/history": [ApiKeyScope.EXECUTIONS_READ.value, ApiKeyScope.ALL.value],
    "POST /api/v1/run-due": [ApiKeyScope.EXECUTIONS_WRITE.value, ApiKeyScope.ALL.value],

    # Stats & tags
    "GET /api/v1/stats": [ApiKeyScope.JOBS_READ.value, ApiKeyScope.ALL.value],
    "GET /api/v1/tags": [ApiKeyScope.JOBS_READ.value, ApiKeyScope.ALL.value],
    "GET /api/v1/tags/{tag}": [ApiKeyScope.JOBS_READ.value, ApiKeyScope.ALL.value],

    # Dependencies
    "POST /api/v1/dependencies": [ApiKeyScope.JOBS_WRITE.value, ApiKeyScope.ALL.value],

    # Webhooks
    "GET /api/v1/webhooks": [ApiKeyScope.WEBHOOKS_READ.value, ApiKeyScope.ALL.value],
    "POST /api/v1/webhooks": [ApiKeyScope.WEBHOOKS_WRITE.value, ApiKeyScope.ALL.value],
    "DELETE /api/v1/webhooks/{id}": [ApiKeyScope.WEBHOOKS_WRITE.value, ApiKeyScope.ALL.value],
    "GET /api/v1/webhook-deliveries": [ApiKeyScope.WEBHOOKS_READ.value, ApiKeyScope.ALL.value],

    # Templates
    "GET /api/v1/templates": [ApiKeyScope.TEMPLATES_READ.value, ApiKeyScope.ALL.value],
    "POST /api/v1/templates": [ApiKeyScope.TEMPLATES_WRITE.value, ApiKeyScope.ALL.value],
}


def get_required_scopes(method: str, path: str) -> list[str]:
    """Get the required scopes for an API endpoint.

    Returns a list of scopes, any of which grants access.
    """
    # Normalize path (remove IDs)
    normalized = path
    parts = path.split("/")
    for i, part in enumerate(parts):
        if i > 3 and part and not part.startswith("v"):  # After /api/v1/
            # This might be an ID — replace with placeholder
            if not part in ("jobs", "webhooks", "templates", "tags", "history",
                          "stats", "dependencies", "run-due", "webhook-deliveries",
                          "pause", "resume", "run", "next-run"):
                parts[i] = "{id}"
    normalized = "/".join(parts)

    endpoint_key = f"{method} {normalized}"
    return ENDPOINT_SCOPES.get(endpoint_key, [ApiKeyScope.ALL.value])
