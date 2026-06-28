"""Tests for REST API server."""

import pytest
import json
from datetime import datetime, timezone
from typing import Any, Optional

from agent_scheduler.models import Job, JobStatus, Priority
from agent_scheduler.scheduler import Scheduler
from agent_scheduler.store import JSONJobStore
from agent_scheduler.webhook import WebhookManager
from agent_scheduler.api import create_app


@pytest.fixture
def scheduler(tmp_path):
    store = JSONJobStore(data_dir=str(tmp_path / "api-test"))
    return Scheduler(store=store)


@pytest.fixture
def app(scheduler):
    webhook_manager = scheduler.webhooks
    return create_app(scheduler, webhook_manager)


class TestAPIAppCreation:
    """Test that the API app can be created properly."""

    def test_create_app_returns_something(self, scheduler):
        app = create_app(scheduler)
        assert app is not None

    def test_create_app_with_webhook_manager(self, scheduler):
        wm = scheduler.webhooks
        app = create_app(scheduler, wm)
        assert app is not None

    def test_create_app_without_webhook_manager(self, scheduler):
        app = create_app(scheduler, None)
        assert app is not None


class TestRawASGILowLevel:
    """Test the raw ASGI fallback layer directly."""

    @pytest.fixture
    def raw_app(self, tmp_path):
        """Force the raw ASGI app (no starlette)."""
        from agent_scheduler.api import _create_raw_asgi_app
        store = JSONJobStore(data_dir=str(tmp_path / "raw-api-test"))
        scheduler = Scheduler(store=store)
        return _create_raw_asgi_app(scheduler, scheduler.webhooks)

    @pytest.mark.asyncio
    async def test_health_endpoint(self, raw_app):
        scope = {
            "type": "http",
            "path": "/health",
            "method": "GET",
            "query_string": b"",
        }

        async def receive():
            return {"body": b"", "more_body": False}

        responses = []

        async def send(message):
            responses.append(message)

        await raw_app(scope, receive, send)
        assert len(responses) == 2
        assert responses[0]["status"] == 200
        body = json.loads(responses[1]["body"])
        assert body["status"] == "ok"
        assert body["service"] == "agent-scheduler"

    @pytest.mark.asyncio
    async def test_create_job(self, raw_app):
        job_data = {"name": "api-job", "handler": "test.handler"}
        body_bytes = json.dumps(job_data).encode()

        scope = {
            "type": "http",
            "path": "/api/v1/jobs",
            "method": "POST",
            "query_string": b"",
        }

        async def receive():
            return {"body": body_bytes, "more_body": False}

        responses = []

        async def send(message):
            responses.append(message)

        await raw_app(scope, receive, send)
        assert responses[0]["status"] == 201
        body = json.loads(responses[1]["body"])
        assert "job" in body
        assert body["job"]["name"] == "api-job"

    @pytest.mark.asyncio
    async def test_list_jobs_empty(self, raw_app):
        scope = {
            "type": "http",
            "path": "/api/v1/jobs",
            "method": "GET",
            "query_string": b"",
        }

        async def receive():
            return {"body": b"", "more_body": False}

        responses = []

        async def send(message):
            responses.append(message)

        await raw_app(scope, receive, send)
        assert responses[0]["status"] == 200
        body = json.loads(responses[1]["body"])
        assert body["count"] == 0

    @pytest.mark.asyncio
    async def test_list_jobs_with_data(self, raw_app):
        # The raw app has its own scheduler — add a job via API
        job_data = {"name": "test-job", "handler": "h"}
        body_bytes = json.dumps(job_data).encode()

        # First create a job
        scope = {
            "type": "http",
            "path": "/api/v1/jobs",
            "method": "POST",
            "query_string": b"",
        }

        async def receive():
            return {"body": body_bytes, "more_body": False}

        responses = []

        async def send(message):
            responses.append(message)

        await raw_app(scope, receive, send)

        # Then list
        scope2 = {
            "type": "http",
            "path": "/api/v1/jobs",
            "method": "GET",
            "query_string": b"",
        }

        async def receive2():
            return {"body": b"", "more_body": False}

        responses2 = []

        async def send2(message):
            responses2.append(message)

        await raw_app(scope2, receive2, send2)
        assert responses2[0]["status"] == 200
        body = json.loads(responses2[1]["body"])
        assert body["count"] == 1

    @pytest.mark.asyncio
    async def test_get_stats(self, raw_app):
        scope = {
            "type": "http",
            "path": "/api/v1/stats",
            "method": "GET",
            "query_string": b"",
        }

        async def receive():
            return {"body": b"", "more_body": False}

        responses = []

        async def send(message):
            responses.append(message)

        await raw_app(scope, receive, send)
        assert responses[0]["status"] == 200
        body = json.loads(responses[1]["body"])
        assert "stats" in body

    @pytest.mark.asyncio
    async def test_list_tags(self, raw_app):
        scope = {
            "type": "http",
            "path": "/api/v1/tags",
            "method": "GET",
            "query_string": b"",
        }

        async def receive():
            return {"body": b"", "more_body": False}

        responses = []

        async def send(message):
            responses.append(message)

        await raw_app(scope, receive, send)
        assert responses[0]["status"] == 200
        body = json.loads(responses[1]["body"])
        assert "tags" in body

    @pytest.mark.asyncio
    async def test_not_found(self, raw_app):
        scope = {
            "type": "http",
            "path": "/api/v1/nonexistent",
            "method": "GET",
            "query_string": b"",
        }

        async def receive():
            return {"body": b"", "more_body": False}

        responses = []

        async def send(message):
            responses.append(message)

        await raw_app(scope, receive, send)
        assert responses[0]["status"] == 404

    @pytest.mark.asyncio
    async def test_cors_headers_in_response(self, raw_app):
        scope = {
            "type": "http",
            "path": "/health",
            "method": "GET",
            "query_string": b"",
        }

        async def receive():
            return {"body": b"", "more_body": False}

        responses = []

        async def send(message):
            responses.append(message)

        await raw_app(scope, receive, send)
        # Check CORS header in response start
        headers = dict(responses[0]["headers"])
        assert headers.get(b"access-control-allow-origin") == b"*"

    @pytest.mark.asyncio
    async def test_run_due(self, raw_app):
        # Create a job first
        job_data = {"name": "due-job", "handler": "h"}
        body_bytes = json.dumps(job_data).encode()

        scope = {
            "type": "http",
            "path": "/api/v1/jobs",
            "method": "POST",
            "query_string": b"",
        }

        async def receive():
            return {"body": body_bytes, "more_body": False}

        responses = []

        async def send(message):
            responses.append(message)

        await raw_app(scope, receive, send)

        # Run due
        scope2 = {
            "type": "http",
            "path": "/api/v1/run-due",
            "method": "POST",
            "query_string": b"",
        }

        async def receive2():
            return {"body": b"", "more_body": False}

        responses2 = []

        async def send2(message):
            responses2.append(message)

        await raw_app(scope2, receive2, send2)
        assert responses2[0]["status"] == 200
        body = json.loads(responses2[1]["body"])
        assert body["count"] == 1

    @pytest.mark.asyncio
    async def test_history_endpoint(self, raw_app):
        # Create and run a job
        job_data = {"name": "hist-job", "handler": "h"}
        body_bytes = json.dumps(job_data).encode()

        scope = {
            "type": "http",
            "path": "/api/v1/jobs",
            "method": "POST",
            "query_string": b"",
        }

        async def receive():
            return {"body": body_bytes, "more_body": False}

        responses = []

        async def send(message):
            responses.append(message)

        await raw_app(scope, receive, send)

        # Run due first
        scope_run = {
            "type": "http",
            "path": "/api/v1/run-due",
            "method": "POST",
            "query_string": b"",
        }

        async def receive_run():
            return {"body": b"", "more_body": False}

        responses_run = []

        async def send_run(message):
            responses_run.append(message)

        await raw_app(scope_run, receive_run, send_run)

        # Now check history
        scope_hist = {
            "type": "http",
            "path": "/api/v1/history",
            "method": "GET",
            "query_string": b"",
        }

        async def receive_hist():
            return {"body": b"", "more_body": False}

        responses_hist = []

        async def send_hist(message):
            responses_hist.append(message)

        await raw_app(scope_hist, receive_hist, send_hist)
        assert responses_hist[0]["status"] == 200
        body = json.loads(responses_hist[1]["body"])
        assert body["count"] >= 1

    @pytest.mark.asyncio
    async def test_list_jobs_with_tag_filter(self, raw_app):
        # Create a tagged job
        job_data = {"name": "tagged-job", "handler": "h", "tags": ["monitoring"]}
        body_bytes = json.dumps(job_data).encode()

        scope = {
            "type": "http",
            "path": "/api/v1/jobs",
            "method": "POST",
            "query_string": b"",
        }

        async def receive():
            return {"body": body_bytes, "more_body": False}

        responses = []

        async def send(message):
            responses.append(message)

        await raw_app(scope, receive, send)

        # List with tag filter
        scope2 = {
            "type": "http",
            "path": "/api/v1/jobs",
            "method": "GET",
            "query_string": b"tag=monitoring",
        }

        async def receive2():
            return {"body": b"", "more_body": False}

        responses2 = []

        async def send2(message):
            responses2.append(message)

        await raw_app(scope2, receive2, send2)
        assert responses2[0]["status"] == 200
        body = json.loads(responses2[1]["body"])
        assert body["count"] == 1
