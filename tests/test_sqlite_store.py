"""Tests for SQLite persistence backend."""

import os
import tempfile
from datetime import datetime, timezone

import pytest

from agent_scheduler.models import (
    ExecutionStatus,
    Job,
    JobDependency,
    JobExecution,
    JobStatus,
    Priority,
    RetryPolicy,
)
from agent_scheduler.sqlite_store import SQLiteJobStore
from agent_scheduler.webhook import Webhook, WebhookDelivery, WebhookEvent, WebhookStatus
from agent_scheduler.templates import JobTemplate, TemplateCategory


@pytest.fixture
def store():
    """Create a temporary SQLite store."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")
        s = SQLiteJobStore(db_path=db_path)
        yield s
        s.close()


class TestSQLiteJobStoreJobs:
    def test_save_and_get_job(self, store):
        job = Job(name="test-job", handler="test.handler", cron="0 9 * * *")
        store.save_job(job)
        retrieved = store.get_job(job.id)
        assert retrieved is not None
        assert retrieved.name == "test-job"
        assert retrieved.handler == "test.handler"
        assert retrieved.cron == "0 9 * * *"

    def test_get_nonexistent_job(self, store):
        assert store.get_job("nonexistent") is None

    def test_update_job(self, store):
        job = Job(name="original", handler="test.handler")
        store.save_job(job)
        job.name = "updated"
        store.save_job(job)
        retrieved = store.get_job(job.id)
        assert retrieved.name == "updated"

    def test_delete_job(self, store):
        job = Job(name="to-delete", handler="test.handler")
        store.save_job(job)
        assert store.delete_job(job.id) is True
        assert store.get_job(job.id) is None

    def test_delete_nonexistent_job(self, store):
        assert store.delete_job("nonexistent") is False

    def test_list_jobs(self, store):
        for i in range(5):
            store.save_job(Job(name=f"job-{i}", handler="test.handler"))
        jobs = store.list_jobs()
        assert len(jobs) == 5

    def test_list_jobs_enabled_only(self, store):
        job1 = Job(name="enabled", handler="test.handler", enabled=True)
        job2 = Job(name="disabled", handler="test.handler", enabled=False)
        store.save_job(job1)
        store.save_job(job2)
        jobs = store.list_jobs(enabled_only=True)
        assert len(jobs) == 1
        assert jobs[0].name == "enabled"

    def test_list_jobs_by_status(self, store):
        job1 = Job(name="scheduled", handler="test.handler", status=JobStatus.SCHEDULED)
        job2 = Job(name="paused", handler="test.handler", status=JobStatus.PAUSED)
        store.save_job(job1)
        store.save_job(job2)
        jobs = store.list_jobs(status="paused")
        assert len(jobs) == 1
        assert jobs[0].name == "paused"

    def test_get_job_by_name(self, store):
        job = Job(name="unique-name", handler="test.handler")
        store.save_job(job)
        retrieved = store.get_job_by_name("unique-name")
        assert retrieved is not None
        assert retrieved.id == job.id

    def test_job_with_retry_policy(self, store):
        job = Job(
            name="retry-job",
            handler="test.handler",
            retry_policy=RetryPolicy(max_retries=5, backoff_seconds=60),
        )
        store.save_job(job)
        retrieved = store.get_job(job.id)
        assert retrieved.retry_policy is not None
        assert retrieved.retry_policy.max_retries == 5
        assert retrieved.retry_policy.backoff_seconds == 60

    def test_job_with_tags(self, store):
        job = Job(name="tagged", handler="test.handler", tags=["monitoring", "daily"])
        store.save_job(job)
        retrieved = store.get_job(job.id)
        assert "monitoring" in retrieved.tags
        assert "daily" in retrieved.tags


class TestSQLiteJobStoreExecutions:
    def test_save_and_get_executions(self, store):
        job = Job(name="test", handler="test.handler")
        store.save_job(job)
        execution = JobExecution(
            job_id=job.id,
            job_name=job.name,
            status=ExecutionStatus.SUCCESS,
            duration_seconds=1.5,
        )
        store.save_execution(execution)
        executions = store.get_executions(job.id)
        assert len(executions) == 1
        assert executions[0].status == ExecutionStatus.SUCCESS
        assert executions[0].duration_seconds == 1.5

    def test_get_all_executions(self, store):
        job1 = Job(name="j1", handler="h1")
        job2 = Job(name="j2", handler="h2")
        store.save_job(job1)
        store.save_job(job2)
        store.save_execution(JobExecution(job_id=job1.id, job_name=job1.name, status=ExecutionStatus.SUCCESS))
        store.save_execution(JobExecution(job_id=job2.id, job_name=job2.name, status=ExecutionStatus.FAILED))
        all_exec = store.get_all_executions()
        assert len(all_exec) == 2

    def test_count_executions(self, store):
        job = Job(name="test", handler="test.handler")
        store.save_job(job)
        for _ in range(5):
            store.save_execution(JobExecution(job_id=job.id, job_name=job.name, status=ExecutionStatus.SUCCESS))
        assert store.count_executions(job.id) == 5
        assert store.count_executions() == 5

    def test_executions_with_pagination(self, store):
        job = Job(name="test", handler="test.handler")
        store.save_job(job)
        for i in range(10):
            store.save_execution(JobExecution(job_id=job.id, job_name=job.name, status=ExecutionStatus.SUCCESS))
        page1 = store.get_executions(job.id, limit=5, offset=0)
        page2 = store.get_executions(job.id, limit=5, offset=5)
        assert len(page1) == 5
        assert len(page2) == 5


class TestSQLiteJobStoreDependencies:
    def test_save_and_get_dependencies(self, store):
        job1 = Job(name="parent", handler="h1")
        job2 = Job(name="child", handler="h2")
        store.save_job(job1)
        store.save_job(job2)
        dep = JobDependency(job_id=job2.id, depends_on_id=job1.id, on_status=ExecutionStatus.SUCCESS)
        store.save_dependency(dep)
        deps = store.get_dependencies(job2.id)
        assert len(deps) == 1
        assert deps[0].depends_on_id == job1.id

    def test_list_dependencies(self, store):
        job1 = Job(name="a", handler="h1")
        job2 = Job(name="b", handler="h2")
        store.save_job(job1)
        store.save_job(job2)
        store.save_dependency(JobDependency(job_id=job2.id, depends_on_id=job1.id))
        all_deps = store.list_dependencies()
        assert len(all_deps) == 1

    def test_delete_dependency(self, store):
        job1 = Job(name="a", handler="h1")
        job2 = Job(name="b", handler="h2")
        store.save_job(job1)
        store.save_job(job2)
        dep = JobDependency(job_id=job2.id, depends_on_id=job1.id)
        store.save_dependency(dep)
        assert store.delete_dependency(dep.id) is True
        assert store.get_dependencies(job2.id) == []


class TestSQLiteJobStoreWebhooks:
    def test_save_and_get_webhook(self, store):
        webhook = Webhook(name="test-hook", url="https://example.com/hook")
        store.save_webhook(webhook)
        retrieved = store.get_webhook(webhook.id)
        assert retrieved is not None
        assert retrieved.name == "test-hook"
        assert retrieved.url == "https://example.com/hook"

    def test_list_webhooks(self, store):
        for i in range(3):
            store.save_webhook(Webhook(name=f"hook-{i}", url=f"https://example.com/{i}"))
        webhooks = store.list_webhooks()
        assert len(webhooks) == 3

    def test_delete_webhook(self, store):
        webhook = Webhook(name="to-delete", url="https://example.com/hook")
        store.save_webhook(webhook)
        assert store.delete_webhook(webhook.id) is True
        assert store.get_webhook(webhook.id) is None

    def test_webhook_deliveries(self, store):
        webhook = Webhook(name="test", url="https://example.com/hook")
        store.save_webhook(webhook)
        job = Job(name="test-job", handler="h1")
        store.save_job(job)
        delivery = WebhookDelivery(
            webhook_id=webhook.id,
            event=WebhookEvent.JOB_COMPLETED,
            job_id=job.id,
            job_name=job.name,
            payload={"event": "job.completed"},
            status=WebhookStatus.DELIVERED,
            status_code=200,
        )
        store.save_webhook_delivery(delivery)
        deliveries = store.get_webhook_deliveries(webhook_id=webhook.id)
        assert len(deliveries) == 1
        assert deliveries[0].status_code == 200


class TestSQLiteJobStoreTemplates:
    def test_save_and_get_template(self, store):
        template = JobTemplate(name="my-template", handler="test.handler", category=TemplateCategory.MONITORING)
        store.save_template(template)
        retrieved = store.get_template(template.id)
        assert retrieved is not None
        assert retrieved.name == "my-template"

    def test_get_template_by_name(self, store):
        template = JobTemplate(name="unique-tpl", handler="test.handler")
        store.save_template(template)
        retrieved = store.get_template("unique-tpl")
        assert retrieved is not None

    def test_list_templates(self, store):
        for cat in [TemplateCategory.MONITORING, TemplateCategory.BACKUP, TemplateCategory.REPORTING]:
            store.save_template(JobTemplate(name=f"tpl-{cat.value}", handler="h", category=cat))
        templates = store.list_templates()
        assert len(templates) == 3
        monitoring = store.list_templates(category="monitoring")
        assert len(monitoring) == 1

    def test_delete_template(self, store):
        template = JobTemplate(name="to-delete", handler="h")
        store.save_template(template)
        assert store.delete_template(template.id) is True
        assert store.get_template(template.id) is None


class TestSQLiteJobStoreApiKeys:
    def test_save_and_get_api_key(self, store):
        store.save_api_key("ask_testkey123", "test-key", ["jobs:read", "jobs:write"])
        record = store.get_api_key("ask_testkey123")
        assert record is not None
        assert record["name"] == "test-key"
        assert "jobs:read" in record["scopes"]
        assert record["enabled"] is True

    def test_get_nonexistent_key(self, store):
        assert store.get_api_key("nonexistent") is None

    def test_list_api_keys(self, store):
        store.save_api_key("ask_key1", "Key 1", ["*"])
        store.save_api_key("ask_key2", "Key 2", ["jobs:read"])
        keys = store.list_api_keys()
        assert len(keys) == 2
        # Keys should be masked
        for k in keys:
            assert "..." in k["key"]

    def test_delete_api_key(self, store):
        store.save_api_key("ask_delete_me", "delete-me", ["*"])
        assert store.delete_api_key("ask_delete_me") is True
        assert store.get_api_key("ask_delete_me") is None

    def test_update_api_key_usage(self, store):
        store.save_api_key("ask_usage", "usage-test", ["*"])
        store.update_api_key_usage("ask_usage")
        record = store.get_api_key("ask_usage")
        assert record["request_count"] == 1
        assert record["last_used_at"] is not None

    def test_toggle_api_key(self, store):
        store.save_api_key("ask_toggle", "toggle-test", ["*"], enabled=True)
        assert store.toggle_api_key("ask_toggle", enabled=False) is True
        record = store.get_api_key("ask_toggle")
        assert record["enabled"] is False


class TestSQLiteJobStoreRateLimits:
    def test_rate_limit_allows_under_limit(self, store):
        for _ in range(5):
            allowed, remaining = store.check_rate_limit("hash123", max_requests=10, window_seconds=60)
            assert allowed is True
        assert remaining == 5

    def test_rate_limit_blocks_over_limit(self, store):
        for _ in range(10):
            store.check_rate_limit("hash456", max_requests=10, window_seconds=60)
        allowed, remaining = store.check_rate_limit("hash456", max_requests=10, window_seconds=60)
        assert allowed is False
        assert remaining == 0

    def test_different_keys_independent(self, store):
        for _ in range(10):
            store.check_rate_limit("key_a", max_requests=10, window_seconds=60)
        allowed, _ = store.check_rate_limit("key_b", max_requests=10, window_seconds=60)
        assert allowed is True
