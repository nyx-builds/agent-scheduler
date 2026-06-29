"""Tests for API key authentication and rate limiting."""

import pytest

from agent_scheduler.auth import (
    ApiKey,
    ApiKeyManager,
    ApiKeyScope,
    RateLimitConfig,
    extract_api_key,
    get_required_scopes,
)
from agent_scheduler.sqlite_store import SQLiteJobStore


@pytest.fixture
def sqlite_store():
    import tempfile, os
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")
        s = SQLiteJobStore(db_path=db_path)
        yield s
        s.close()


@pytest.fixture
def auth_manager(sqlite_store):
    return ApiKeyManager(store=sqlite_store, rate_limit_config=RateLimitConfig(max_requests=10, window_seconds=60))


class TestApiKey:
    def test_has_scope_wildcard(self):
        key = ApiKey(key="test", name="test", scopes=["*"])
        assert key.has_scope("jobs:read")
        assert key.has_scope("admin")
        assert key.has_scope("anything")

    def test_has_scope_admin(self):
        key = ApiKey(key="test", name="test", scopes=["admin"])
        assert key.has_scope("jobs:read")

    def test_has_scope_specific(self):
        key = ApiKey(key="test", name="test", scopes=["jobs:read", "jobs:write"])
        assert key.has_scope("jobs:read")
        assert not key.has_scope("webhooks:read")

    def test_has_any_scope(self):
        key = ApiKey(key="test", name="test", scopes=["jobs:read"])
        assert key.has_any_scope("jobs:read", "jobs:write")
        assert not key.has_any_scope("webhooks:read", "executions:read")


class TestApiKeyManager:
    def test_create_api_key(self, auth_manager):
        api_key = auth_manager.create_api_key(name="test-key")
        assert api_key.key.startswith("ask_")
        assert api_key.name == "test-key"
        assert "*" in api_key.scopes

    def test_create_api_key_with_scopes(self, auth_manager):
        api_key = auth_manager.create_api_key(
            name="limited-key",
            scopes=["jobs:read", "jobs:write"],
        )
        assert "jobs:read" in api_key.scopes
        assert "jobs:write" in api_key.scopes

    def test_create_api_key_invalid_scope(self, auth_manager):
        with pytest.raises(ValueError, match="Invalid scope"):
            auth_manager.create_api_key(name="bad", scopes=["invalid:scope"])

    def test_authenticate_valid_key(self, auth_manager):
        api_key = auth_manager.create_api_key(name="test")
        authenticated = auth_manager.authenticate(api_key.key)
        assert authenticated is not None
        assert authenticated.name == "test"

    def test_authenticate_invalid_key(self, auth_manager):
        assert auth_manager.authenticate("invalid_key") is None

    def test_authenticate_disabled_key(self, auth_manager):
        api_key = auth_manager.create_api_key(name="disabled", enabled=False)
        assert auth_manager.authenticate(api_key.key) is None

    def test_authenticate_updates_usage(self, auth_manager):
        api_key = auth_manager.create_api_key(name="usage-test")
        auth_manager.authenticate(api_key.key)
        auth_manager.authenticate(api_key.key)
        keys = auth_manager.list_api_keys()
        key = [k for k in keys if k["name"] == "usage-test"][0]
        assert key["request_count"] == 2
        assert key["last_used_at"] is not None

    def test_list_api_keys(self, auth_manager):
        auth_manager.create_api_key(name="key1")
        auth_manager.create_api_key(name="key2")
        keys = auth_manager.list_api_keys()
        assert len(keys) == 2
        # All keys should be masked
        for k in keys:
            assert "..." in k["key"]

    def test_revoke_api_key(self, auth_manager):
        api_key = auth_manager.create_api_key(name="revoke-me")
        assert auth_manager.revoke_api_key(api_key.key) is True
        assert auth_manager.authenticate(api_key.key) is None

    def test_toggle_api_key(self, auth_manager):
        api_key = auth_manager.create_api_key(name="toggle-me")
        auth_manager.toggle_api_key(api_key.key, enabled=False)
        assert auth_manager.authenticate(api_key.key) is None

    def test_no_auth_manager(self):
        manager = ApiKeyManager(store=None)
        assert manager.list_api_keys() == []
        assert manager.revoke_api_key("any") is False


class TestRateLimiting:
    def test_rate_limit_allows_under_limit(self, auth_manager):
        api_key = auth_manager.create_api_key(name="limited")
        for _ in range(10):
            allowed, remaining = auth_manager.check_rate_limit(api_key.key)
            assert allowed is True

    def test_rate_limit_blocks_over_limit(self, auth_manager):
        api_key = auth_manager.create_api_key(name="limited")
        for _ in range(10):
            auth_manager.check_rate_limit(api_key.key)
        allowed, remaining = auth_manager.check_rate_limit(api_key.key)
        assert allowed is False

    def test_rate_limit_disabled(self, sqlite_store):
        config = RateLimitConfig(enabled=False)
        manager = ApiKeyManager(store=sqlite_store, rate_limit_config=config)
        api_key = manager.create_api_key(name="unlimited")
        for _ in range(100):
            allowed, _ = manager.check_rate_limit(api_key.key)
            assert allowed is True

    def test_no_store_always_allows(self):
        manager = ApiKeyManager(store=None)
        allowed, _ = manager.check_rate_limit("any_key")
        assert allowed is True


class TestExtractApiKey:
    def test_from_bearer_header(self):
        headers = {"authorization": "Bearer ask_test123"}
        assert extract_api_key(headers) == "ask_test123"

    def test_from_api_key_header(self):
        headers = {"x-api-key": "ask_test123"}
        assert extract_api_key(headers) == "ask_test123"

    def test_from_query_params(self):
        params = {"api_key": "ask_test123"}
        assert extract_api_key({}, params) == "ask_test123"

    def test_no_key_returns_none(self):
        assert extract_api_key({}) is None

    def test_bearer_takes_priority(self):
        headers = {"authorization": "Bearer ask_bearer", "x-api-key": "ask_header"}
        assert extract_api_key(headers) == "ask_bearer"

    def test_empty_bearer_falls_through(self):
        headers = {"authorization": "Bearer ", "x-api-key": "ask_header"}
        assert extract_api_key(headers) == "ask_header"

    def test_case_insensitive_headers(self):
        headers = {"Authorization": "Bearer ask_test", "X-API-Key": "ask_other"}
        result = extract_api_key(headers)
        assert result in ("ask_test", "ask_other")


class TestEndpointScopes:
    def test_jobs_read_scope(self):
        scopes = get_required_scopes("GET", "/api/v1/jobs")
        assert "jobs:read" in scopes or "*" in scopes

    def test_jobs_write_scope(self):
        scopes = get_required_scopes("POST", "/api/v1/jobs")
        assert "jobs:write" in scopes or "*" in scopes

    def test_webhooks_scope(self):
        scopes = get_required_scopes("GET", "/api/v1/webhooks")
        assert "webhooks:read" in scopes or "*" in scopes

    def test_unknown_endpoint_default(self):
        scopes = get_required_scopes("GET", "/api/v1/unknown")
        assert "*" in scopes
