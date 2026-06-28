"""Tests for MCP server."""

import pytest
from datetime import datetime, timezone

from agent_scheduler.models import Job, JobStatus, Priority, RetryPolicy
from agent_scheduler.mcp_server import MCPServer, TOOLS
from agent_scheduler.scheduler import Scheduler
from agent_scheduler.store import JSONJobStore


@pytest.fixture
def mcp_server(tmp_path):
    store = JSONJobStore(data_dir=str(tmp_path / "mcp-test"))
    scheduler = Scheduler(store=store)
    return MCPServer(scheduler)


class TestMCPServerTools:
    def test_tool_definitions(self):
        assert len(TOOLS) >= 15
        tool_names = [t["name"] for t in TOOLS]
        assert "scheduler_create_job" in tool_names
        assert "scheduler_list_jobs" in tool_names
        assert "scheduler_get_job" in tool_names
        assert "scheduler_update_job" in tool_names
        assert "scheduler_delete_job" in tool_names
        assert "scheduler_pause_job" in tool_names
        assert "scheduler_resume_job" in tool_names
        assert "scheduler_run_job" in tool_names
        assert "scheduler_get_history" in tool_names
        assert "scheduler_get_next_run" in tool_names
        assert "scheduler_get_stats" in tool_names
        assert "scheduler_list_tags" in tool_names
        assert "scheduler_get_jobs_by_tag" in tool_names
        assert "scheduler_create_dependency" in tool_names
        assert "scheduler_get_dependencies" in tool_names

    @pytest.mark.asyncio
    async def test_create_job(self, mcp_server):
        result = await mcp_server.handle_tool_call("scheduler_create_job", {
            "name": "test-job",
            "handler": "test.handler",
            "cron": "0 9 * * *",
            "priority": "high",
            "tags": ["test"],
        })
        assert "job" in result
        assert result["job"]["name"] == "test-job"
        assert result["job"]["handler"] == "test.handler"
        assert result["job"]["priority"] == "high"
        assert "test" in result["job"]["tags"]

    @pytest.mark.asyncio
    async def test_create_job_with_delay(self, mcp_server):
        result = await mcp_server.handle_tool_call("scheduler_create_job", {
            "name": "delayed-job",
            "handler": "h",
            "delay": 3600,
        })
        assert "job" in result
        assert result["job"]["delay"] == 3600

    @pytest.mark.asyncio
    async def test_list_jobs(self, mcp_server):
        await mcp_server.handle_tool_call("scheduler_create_job", {"name": "job1", "handler": "h1"})
        await mcp_server.handle_tool_call("scheduler_create_job", {"name": "job2", "handler": "h2"})
        result = await mcp_server.handle_tool_call("scheduler_list_jobs", {})
        assert result["count"] == 2

    @pytest.mark.asyncio
    async def test_list_jobs_with_filter(self, mcp_server):
        await mcp_server.handle_tool_call("scheduler_create_job", {
            "name": "tagged", "handler": "h", "tags": ["monitoring"],
        })
        await mcp_server.handle_tool_call("scheduler_create_job", {
            "name": "untagged", "handler": "h",
        })
        result = await mcp_server.handle_tool_call("scheduler_list_jobs", {"tag": "monitoring"})
        assert result["count"] == 1

    @pytest.mark.asyncio
    async def test_get_job(self, mcp_server):
        create_result = await mcp_server.handle_tool_call("scheduler_create_job", {
            "name": "test-job", "handler": "h",
        })
        job_id = create_result["job"]["id"]
        result = await mcp_server.handle_tool_call("scheduler_get_job", {"job_identifier": job_id})
        assert "job" in result
        assert result["job"]["name"] == "test-job"

    @pytest.mark.asyncio
    async def test_get_job_by_name(self, mcp_server):
        await mcp_server.handle_tool_call("scheduler_create_job", {
            "name": "my-job", "handler": "h",
        })
        result = await mcp_server.handle_tool_call("scheduler_get_job", {"job_identifier": "my-job"})
        assert "job" in result
        assert result["job"]["name"] == "my-job"

    @pytest.mark.asyncio
    async def test_get_job_not_found(self, mcp_server):
        result = await mcp_server.handle_tool_call("scheduler_get_job", {"job_identifier": "nonexistent"})
        assert "error" in result

    @pytest.mark.asyncio
    async def test_update_job(self, mcp_server):
        create_result = await mcp_server.handle_tool_call("scheduler_create_job", {
            "name": "test-job", "handler": "h",
        })
        job_id = create_result["job"]["id"]
        result = await mcp_server.handle_tool_call("scheduler_update_job", {
            "job_identifier": job_id,
            "priority": "high",
            "tags": ["important"],
        })
        assert "job" in result
        assert result["job"]["priority"] == "high"

    @pytest.mark.asyncio
    async def test_delete_job(self, mcp_server):
        create_result = await mcp_server.handle_tool_call("scheduler_create_job", {
            "name": "test-job", "handler": "h",
        })
        job_id = create_result["job"]["id"]
        result = await mcp_server.handle_tool_call("scheduler_delete_job", {"job_identifier": job_id})
        assert "message" in result

    @pytest.mark.asyncio
    async def test_pause_and_resume_job(self, mcp_server):
        create_result = await mcp_server.handle_tool_call("scheduler_create_job", {
            "name": "test-job", "handler": "h",
        })
        job_id = create_result["job"]["id"]

        pause_result = await mcp_server.handle_tool_call("scheduler_pause_job", {"job_identifier": job_id})
        assert "paused" in pause_result["message"].lower() or "pause" in pause_result["message"].lower()

        resume_result = await mcp_server.handle_tool_call("scheduler_resume_job", {"job_identifier": job_id})
        assert "resume" in resume_result["message"].lower()

    @pytest.mark.asyncio
    async def test_run_job(self, mcp_server):
        create_result = await mcp_server.handle_tool_call("scheduler_create_job", {
            "name": "test-job", "handler": "h",
        })
        job_id = create_result["job"]["id"]
        result = await mcp_server.handle_tool_call("scheduler_run_job", {"job_identifier": job_id})
        assert "execution" in result
        assert result["execution"]["status"] == "success"

    @pytest.mark.asyncio
    async def test_get_history(self, mcp_server):
        create_result = await mcp_server.handle_tool_call("scheduler_create_job", {
            "name": "test-job", "handler": "h",
        })
        job_id = create_result["job"]["id"]
        await mcp_server.handle_tool_call("scheduler_run_job", {"job_identifier": job_id})
        result = await mcp_server.handle_tool_call("scheduler_get_history", {"job_identifier": job_id})
        assert result["count"] >= 1

    @pytest.mark.asyncio
    async def test_get_next_run(self, mcp_server):
        create_result = await mcp_server.handle_tool_call("scheduler_create_job", {
            "name": "test-job", "handler": "h", "cron": "0 9 * * *",
        })
        job_id = create_result["job"]["id"]
        result = await mcp_server.handle_tool_call("scheduler_get_next_run", {"job_identifier": job_id})
        assert result["next_run_at"] is not None

    @pytest.mark.asyncio
    async def test_get_stats(self, mcp_server):
        await mcp_server.handle_tool_call("scheduler_create_job", {"name": "job1", "handler": "h"})
        result = await mcp_server.handle_tool_call("scheduler_get_stats", {})
        assert "stats" in result
        assert result["stats"]["total_jobs"] == 1

    @pytest.mark.asyncio
    async def test_list_tags(self, mcp_server):
        await mcp_server.handle_tool_call("scheduler_create_job", {
            "name": "job1", "handler": "h", "tags": ["monitoring", "health"],
        })
        result = await mcp_server.handle_tool_call("scheduler_list_tags", {})
        assert "monitoring" in result["tags"]
        assert "health" in result["tags"]

    @pytest.mark.asyncio
    async def test_get_jobs_by_tag(self, mcp_server):
        await mcp_server.handle_tool_call("scheduler_create_job", {
            "name": "job1", "handler": "h", "tags": ["monitoring"],
        })
        result = await mcp_server.handle_tool_call("scheduler_get_jobs_by_tag", {"tag": "monitoring"})
        assert result["count"] == 1

    @pytest.mark.asyncio
    async def test_create_dependency(self, mcp_server):
        job1 = await mcp_server.handle_tool_call("scheduler_create_job", {"name": "first", "handler": "h1"})
        job2 = await mcp_server.handle_tool_call("scheduler_create_job", {"name": "second", "handler": "h2"})
        result = await mcp_server.handle_tool_call("scheduler_create_dependency", {
            "job_identifier": "second",
            "depends_on_identifier": "first",
        })
        assert "dependency" in result

    @pytest.mark.asyncio
    async def test_get_dependencies(self, mcp_server):
        await mcp_server.handle_tool_call("scheduler_create_job", {"name": "first", "handler": "h1"})
        await mcp_server.handle_tool_call("scheduler_create_job", {"name": "second", "handler": "h2"})
        await mcp_server.handle_tool_call("scheduler_create_dependency", {
            "job_identifier": "second",
            "depends_on_identifier": "first",
        })
        result = await mcp_server.handle_tool_call("scheduler_get_dependencies", {"job_identifier": "second"})
        assert len(result["dependencies"]) == 1

    @pytest.mark.asyncio
    async def test_unknown_tool(self, mcp_server):
        result = await mcp_server.handle_tool_call("nonexistent_tool", {})
        assert "error" in result

    # ── Webhook Tools ───────────────────────────────────────

    @pytest.mark.asyncio
    async def test_create_webhook(self, mcp_server):
        result = await mcp_server.handle_tool_call("scheduler_create_webhook", {
            "name": "test-hook",
            "url": "https://example.com/webhook",
            "events": ["job.completed", "job.failed"],
            "tags": ["monitoring"],
        })
        assert "webhook" in result
        assert result["webhook"]["name"] == "test-hook"

    @pytest.mark.asyncio
    async def test_list_webhooks(self, mcp_server):
        await mcp_server.handle_tool_call("scheduler_create_webhook", {
            "name": "hook1", "url": "https://a.com/hook",
        })
        await mcp_server.handle_tool_call("scheduler_create_webhook", {
            "name": "hook2", "url": "https://b.com/hook",
        })
        result = await mcp_server.handle_tool_call("scheduler_list_webhooks", {})
        assert result["count"] == 2

    @pytest.mark.asyncio
    async def test_delete_webhook(self, mcp_server):
        create_result = await mcp_server.handle_tool_call("scheduler_create_webhook", {
            "name": "deletable", "url": "https://a.com/hook",
        })
        webhook_id = create_result["webhook"]["id"]
        result = await mcp_server.handle_tool_call("scheduler_delete_webhook", {
            "webhook_id": webhook_id,
        })
        assert "message" in result

    @pytest.mark.asyncio
    async def test_delete_webhook_not_found(self, mcp_server):
        result = await mcp_server.handle_tool_call("scheduler_delete_webhook", {
            "webhook_id": "nonexistent",
        })
        assert "error" in result

    @pytest.mark.asyncio
    async def test_get_webhook_deliveries(self, mcp_server):
        result = await mcp_server.handle_tool_call("scheduler_get_webhook_deliveries", {})
        assert "deliveries" in result
        assert result["count"] == 0

    # ── Template Tools ──────────────────────────────────────

    @pytest.mark.asyncio
    async def test_list_templates(self, mcp_server):
        result = await mcp_server.handle_tool_call("scheduler_list_templates", {})
        assert "templates" in result
        assert result["count"] >= 5  # Built-in templates
        names = [t["name"] for t in result["templates"]]
        assert "health-check" in names

    @pytest.mark.asyncio
    async def test_list_templates_by_category(self, mcp_server):
        result = await mcp_server.handle_tool_call("scheduler_list_templates", {
            "category": "monitoring",
        })
        assert result["count"] >= 1
        for t in result["templates"]:
            assert t["category"] == "monitoring"

    @pytest.mark.asyncio
    async def test_get_template(self, mcp_server):
        result = await mcp_server.handle_tool_call("scheduler_get_template", {
            "template_name": "health-check",
        })
        assert "template" in result
        assert result["template"]["name"] == "health-check"

    @pytest.mark.asyncio
    async def test_get_template_not_found(self, mcp_server):
        result = await mcp_server.handle_tool_call("scheduler_get_template", {
            "template_name": "nonexistent",
        })
        assert "error" in result

    @pytest.mark.asyncio
    async def test_instantiate_template(self, mcp_server):
        result = await mcp_server.handle_tool_call("scheduler_instantiate_template", {
            "template_name": "health-check",
            "payload": {"endpoint": "https://api.example.com/health"},
        })
        assert "job" in result
        assert result["job"]["handler"] == "health.check"
        assert result["job"]["priority"] == "high"

    @pytest.mark.asyncio
    async def test_instantiate_template_not_found(self, mcp_server):
        result = await mcp_server.handle_tool_call("scheduler_instantiate_template", {
            "template_name": "nonexistent",
        })
        assert "error" in result

    @pytest.mark.asyncio
    async def test_create_template(self, mcp_server):
        result = await mcp_server.handle_tool_call("scheduler_create_template", {
            "name": "custom-template",
            "handler": "custom.handler",
            "description": "My custom template",
            "category": "custom",
        })
        assert "template" in result
        assert result["template"]["name"] == "custom-template"

    @pytest.mark.asyncio
    async def test_tool_count(self):
        """Verify we have at least 20 tools (15 original + webhook + template)."""
        assert len(TOOLS) >= 20
