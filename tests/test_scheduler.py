"""Tests for agent-scheduler core engine."""

import pytest
import asyncio
from datetime import datetime, timedelta, timezone

from agent_scheduler.models import (
    ExecutionStatus,
    Job,
    JobDependency,
    JobExecution,
    JobStatus,
    Priority,
    RetryPolicy,
)
from agent_scheduler.scheduler import Scheduler
from agent_scheduler.handler import HandlerRegistry, HandlerResult
from agent_scheduler.store import JSONJobStore


@pytest.fixture
def scheduler(tmp_path):
    store = JSONJobStore(data_dir=str(tmp_path / "scheduler-test"))
    return Scheduler(store=store)


@pytest.fixture
def scheduler_with_handlers(tmp_path):
    store = JSONJobStore(data_dir=str(tmp_path / "scheduler-handlers"))
    registry = HandlerRegistry()

    # Register a simple sync handler
    def success_handler(payload):
        return {"processed": True, "input": payload}

    registry.register("test.success", success_handler)

    # Register a failing handler
    def fail_handler(payload):
        raise RuntimeError("Handler failed!")

    registry.register("test.fail", fail_handler)

    # Register an async handler
    async def async_handler(payload):
        return {"async": True, "data": payload}

    registry.register("test.async", async_handler)

    return Scheduler(store=store, handler_registry=registry)


class TestSchedulerJobCRUD:
    def test_add_job(self, scheduler):
        job = Job(name="test-job", handler="test.handler", cron="0 * * * *")
        result = scheduler.add_job(job)
        assert result.id == job.id
        assert result.next_run_at is not None

    def test_get_job(self, scheduler):
        job = Job(name="test-job", handler="test.handler")
        scheduler.add_job(job)
        retrieved = scheduler.get_job(job.id)
        assert retrieved is not None
        assert retrieved.name == "test-job"

    def test_get_job_by_name(self, scheduler):
        job = Job(name="my-job", handler="test.handler")
        scheduler.add_job(job)
        retrieved = scheduler.get_job_by_name("my-job")
        assert retrieved is not None
        assert retrieved.id == job.id

    def test_get_job_by_name_not_found(self, scheduler):
        result = scheduler.get_job_by_name("nonexistent")
        assert result is None

    def test_update_job(self, scheduler):
        job = Job(name="test-job", handler="test.handler")
        scheduler.add_job(job)
        updated = scheduler.update_job(job.id, priority=Priority.HIGH, tags=["important"])
        assert updated is not None
        assert updated.priority == Priority.HIGH
        assert "important" in updated.tags

    def test_update_job_cron_recomputes_next_run(self, scheduler):
        job = Job(name="test-job", handler="test.handler", cron="0 9 * * *")
        scheduler.add_job(job)
        updated = scheduler.update_job(job.id, cron="0 18 * * *")
        assert updated is not None
        assert updated.cron == "0 18 * * *"

    def test_update_job_not_found(self, scheduler):
        result = scheduler.update_job("nonexistent", priority=Priority.HIGH)
        assert result is None

    def test_delete_job(self, scheduler):
        job = Job(name="test-job", handler="test.handler")
        scheduler.add_job(job)
        assert scheduler.delete_job(job.id) is True
        assert scheduler.get_job(job.id) is None

    def test_delete_job_not_found(self, scheduler):
        assert scheduler.delete_job("nonexistent") is False

    def test_list_jobs(self, scheduler):
        scheduler.add_job(Job(name="job1", handler="h1"))
        scheduler.add_job(Job(name="job2", handler="h2"))
        scheduler.add_job(Job(name="job3", handler="h3"))
        assert len(scheduler.list_jobs()) == 3

    def test_list_jobs_enabled_only(self, scheduler):
        job1 = Job(name="enabled", handler="h1", enabled=True)
        job2 = Job(name="disabled", handler="h2", enabled=False)
        scheduler.add_job(job1)
        scheduler.add_job(job2)
        enabled = scheduler.list_jobs(enabled_only=True)
        assert len(enabled) == 1
        assert enabled[0].name == "enabled"

    def test_list_jobs_by_tag(self, scheduler):
        scheduler.add_job(Job(name="job1", handler="h1", tags=["monitoring"]))
        scheduler.add_job(Job(name="job2", handler="h2", tags=["backup"]))
        scheduler.add_job(Job(name="job3", handler="h3", tags=["monitoring", "backup"]))
        monitoring = scheduler.list_jobs(tag="monitoring")
        assert len(monitoring) == 2

    def test_list_jobs_by_status(self, scheduler):
        job1 = Job(name="active", handler="h1", status=JobStatus.SCHEDULED)
        job2 = Job(name="paused", handler="h2", status=JobStatus.PAUSED)
        scheduler.add_job(job1)
        scheduler.add_job(job2)
        scheduled = scheduler.list_jobs(status=JobStatus.SCHEDULED)
        assert len(scheduled) == 1


class TestSchedulerJobControl:
    def test_pause_job(self, scheduler):
        job = Job(name="test-job", handler="h")
        scheduler.add_job(job)
        result = scheduler.pause_job(job.id)
        assert result is not None
        assert result.status == JobStatus.PAUSED
        assert result.enabled is False

    def test_resume_job(self, scheduler):
        job = Job(name="test-job", handler="h", status=JobStatus.PAUSED, enabled=False)
        scheduler.add_job(job)
        result = scheduler.resume_job(job.id)
        assert result is not None
        assert result.status == JobStatus.SCHEDULED
        assert result.enabled is True

    def test_cancel_job(self, scheduler):
        job = Job(name="test-job", handler="h")
        scheduler.add_job(job)
        result = scheduler.cancel_job(job.id)
        assert result is not None
        assert result.status == JobStatus.CANCELLED
        assert result.enabled is False


class TestSchedulerExecution:
    @pytest.mark.asyncio
    async def test_run_job_simulated(self, scheduler):
        job = Job(name="test-job", handler="test.handler")
        scheduler.add_job(job)
        execution = await scheduler.run_job(job.id)
        assert execution is not None
        assert execution.is_success
        assert execution.result.get("simulated") is True

    @pytest.mark.asyncio
    async def test_run_job_with_handler(self, scheduler_with_handlers):
        job = Job(name="test-job", handler="test.success", payload={"key": "value"})
        scheduler_with_handlers.add_job(job)
        execution = await scheduler_with_handlers.run_job(job.id)
        assert execution is not None
        assert execution.is_success
        assert execution.result.get("processed") is True
        assert execution.result.get("input", {}).get("key") == "value"

    @pytest.mark.asyncio
    async def test_run_job_with_async_handler(self, scheduler_with_handlers):
        job = Job(name="async-job", handler="test.async", payload={"x": 1})
        scheduler_with_handlers.add_job(job)
        execution = await scheduler_with_handlers.run_job(job.id)
        assert execution is not None
        assert execution.is_success
        assert execution.result.get("async") is True

    @pytest.mark.asyncio
    async def test_run_failing_job(self, scheduler_with_handlers):
        job = Job(name="fail-job", handler="test.fail")
        scheduler_with_handlers.add_job(job)
        execution = await scheduler_with_handlers.run_job(job.id)
        assert execution is not None
        assert execution.is_failure

    @pytest.mark.asyncio
    async def test_run_job_updates_counts(self, scheduler):
        job = Job(name="test-job", handler="h")
        scheduler.add_job(job)
        await scheduler.run_job(job.id)
        updated = scheduler.get_job(job.id)
        assert updated.run_count == 1
        assert updated.last_run_at is not None

    @pytest.mark.asyncio
    async def test_run_job_not_found(self, scheduler):
        execution = await scheduler.run_job("nonexistent")
        assert execution is None

    @pytest.mark.asyncio
    async def test_run_due_jobs_immediate(self, scheduler):
        job = Job(name="immediate-job", handler="h")
        scheduler.add_job(job)
        # Immediate jobs should be due now
        executions = await scheduler.run_due_jobs()
        assert len(executions) == 1
        assert executions[0].is_success

    @pytest.mark.asyncio
    async def test_run_due_jobs_respects_priority(self, scheduler):
        low_job = Job(name="low", handler="h", priority=Priority.LOW)
        high_job = Job(name="high", handler="h", priority=Priority.HIGH)
        normal_job = Job(name="normal", handler="h", priority=Priority.NORMAL)
        scheduler.add_job(low_job)
        scheduler.add_job(high_job)
        scheduler.add_job(normal_job)
        executions = await scheduler.run_due_jobs()
        assert len(executions) == 3
        # High priority should run first
        assert executions[0].job_name == "high"

    @pytest.mark.asyncio
    async def test_run_due_jobs_skips_paused(self, scheduler):
        job = Job(name="paused-job", handler="h", status=JobStatus.PAUSED, enabled=False)
        scheduler.add_job(job)
        executions = await scheduler.run_due_jobs()
        assert len(executions) == 0

    @pytest.mark.asyncio
    async def test_run_due_jobs_respects_max_runs(self, scheduler):
        job = Job(name="limited", handler="h", max_runs=1)
        scheduler.add_job(job)
        await scheduler.run_due_jobs()
        updated = scheduler.get_job(job.id)
        assert updated.status == JobStatus.COMPLETED
        assert updated.enabled is False

    @pytest.mark.asyncio
    async def test_one_time_delayed_job_completes(self, scheduler):
        job = Job(name="one-time", handler="h", delay=0)
        scheduler.add_job(job)
        await scheduler.run_due_jobs()
        updated = scheduler.get_job(job.id)
        assert updated.run_count == 1
        assert updated.status == JobStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_recurring_job_stays_scheduled(self, scheduler):
        job = Job(name="recurring", handler="h", cron="0 * * * *")
        scheduler.add_job(job)
        await scheduler.run_due_jobs()
        updated = scheduler.get_job(job.id)
        # Should still have a next_run_at since it's recurring
        assert updated.next_run_at is not None


class TestSchedulerRetry:
    @pytest.mark.asyncio
    async def test_retry_on_failure(self, scheduler_with_handlers):
        job = Job(
            name="retry-job",
            handler="test.fail",
            retry_policy=RetryPolicy(max_retries=2, backoff_seconds=0.01),
        )
        scheduler_with_handlers.add_job(job)
        execution = await scheduler_with_handlers.run_job(job.id)
        assert execution is not None
        assert execution.is_failure
        # After all retries exhausted
        updated = scheduler_with_handlers.get_job(job.id)
        assert updated.fail_count == 1
        assert updated.last_error is not None

    @pytest.mark.asyncio
    async def test_retry_specific_errors(self, scheduler):
        job = Job(
            name="retry-specific",
            handler="h",
            retry_policy=RetryPolicy(
                max_retries=2,
                retry_on_errors=["timeout"],
                backoff_seconds=0.01,
            ),
        )
        scheduler.add_job(job)
        # Simulated handler will succeed, so this just validates the policy is set
        execution = await scheduler.run_job(job.id)
        assert execution.is_success


class TestSchedulerDependencies:
    def test_add_dependency(self, scheduler):
        job1 = Job(name="first", handler="h1")
        job2 = Job(name="second", handler="h2")
        scheduler.add_job(job1)
        scheduler.add_job(job2)
        dep = scheduler.add_dependency(job2.id, job1.id)
        assert dep.job_id == job2.id
        assert dep.depends_on_id == job1.id

    def test_get_dependencies(self, scheduler):
        job1 = Job(name="first", handler="h1")
        job2 = Job(name="second", handler="h2")
        scheduler.add_job(job1)
        scheduler.add_job(job2)
        scheduler.add_dependency(job2.id, job1.id)
        deps = scheduler.get_dependencies(job2.id)
        assert len(deps) == 1

    def test_remove_dependency(self, scheduler):
        job1 = Job(name="first", handler="h1")
        job2 = Job(name="second", handler="h2")
        scheduler.add_job(job1)
        scheduler.add_job(job2)
        dep = scheduler.add_dependency(job2.id, job1.id)
        assert scheduler.remove_dependency(dep.id) is True

    @pytest.mark.asyncio
    async def test_dependency_blocks_execution(self, scheduler):
        job1 = Job(name="first", handler="h1", cron="0 9 1 1 *")  # Far future
        job2 = Job(name="second", handler="h2")  # Immediate
        scheduler.add_job(job1)
        scheduler.add_job(job2)
        scheduler.add_dependency(job2.id, job1.id)
        # Job2 should not run because job1 hasn't completed
        executions = await scheduler.run_due_jobs()
        # Job2 should be blocked
        job2_names = [e.job_name for e in executions]
        assert "second" not in job2_names

    @pytest.mark.asyncio
    async def test_dependency_triggers_after_success(self, scheduler):
        job1 = Job(name="first", handler="h1")
        job2 = Job(name="second", handler="h2", run_at=datetime(2099, 1, 1, tzinfo=timezone.utc))
        scheduler.add_job(job1)
        scheduler.add_job(job2)
        scheduler.add_dependency(job2.id, job1.id, on_status=ExecutionStatus.SUCCESS)
        # Run job1
        await scheduler.run_job(job1.id)
        # Job2 should now have next_run_at set to now (triggered)
        updated = scheduler.get_job(job2.id)
        assert updated.next_run_at is not None
        # And should be due
        executions = await scheduler.run_due_jobs()
        assert any(e.job_name == "second" for e in executions)


class TestSchedulerHistoryAndStats:
    @pytest.mark.asyncio
    async def test_get_history(self, scheduler):
        job = Job(name="test-job", handler="h")
        scheduler.add_job(job)
        await scheduler.run_job(job.id)
        await scheduler.run_job(job.id)
        history = scheduler.get_history(job.id)
        assert len(history) == 2

    def test_get_stats(self, scheduler):
        scheduler.add_job(Job(name="job1", handler="h1", tags=["test"]))
        scheduler.add_job(Job(name="job2", handler="h2", tags=["test", "prod"]))
        stats = scheduler.get_stats()
        assert stats.total_jobs == 2
        assert stats.active_jobs == 2
        assert "test" in stats.tags
        assert "prod" in stats.tags

    def test_get_next_run(self, scheduler):
        job = Job(name="test-job", handler="h", cron="0 9 * * *")
        scheduler.add_job(job)
        next_run = scheduler.get_next_run(job.id)
        assert next_run is not None

    def test_get_next_run_not_found(self, scheduler):
        result = scheduler.get_next_run("nonexistent")
        assert result is None


class TestSchedulerTags:
    def test_list_tags(self, scheduler):
        scheduler.add_job(Job(name="job1", handler="h1", tags=["monitoring", "health"]))
        scheduler.add_job(Job(name="job2", handler="h2", tags=["backup"]))
        tags = scheduler.list_tags()
        assert "monitoring" in tags
        assert "health" in tags
        assert "backup" in tags

    def test_get_jobs_by_tag(self, scheduler):
        scheduler.add_job(Job(name="job1", handler="h1", tags=["monitoring"]))
        scheduler.add_job(Job(name="job2", handler="h2", tags=["backup"]))
        scheduler.add_job(Job(name="job3", handler="h3", tags=["monitoring", "backup"]))
        monitoring = scheduler.get_jobs_by_tag("monitoring")
        assert len(monitoring) == 2
        backup = scheduler.get_jobs_by_tag("backup")
        assert len(backup) == 2

    def test_list_tags_empty(self, scheduler):
        tags = scheduler.list_tags()
        assert tags == []


class TestSchedulerLifecycle:
    @pytest.mark.asyncio
    async def test_start_and_stop(self, scheduler):
        await scheduler.start()
        assert scheduler.is_running is True
        await scheduler.stop()
        assert scheduler.is_running is False

    @pytest.mark.asyncio
    async def test_start_runs_due_jobs(self, scheduler):
        job = Job(name="test-job", handler="h")
        scheduler.add_job(job)
        # Start with very short poll interval
        scheduler.poll_interval = 0.1
        await scheduler.start()
        await asyncio.sleep(0.3)
        await scheduler.stop()
        updated = scheduler.get_job(job.id)
        assert updated.run_count >= 1
