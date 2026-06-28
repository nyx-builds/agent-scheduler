"""Tests for agent-scheduler models."""

import pytest
from datetime import datetime, timedelta, timezone

from agent_scheduler.models import (
    ExecutionStatus,
    Job,
    JobDependency,
    JobExecution,
    JobStatus,
    Priority,
    RetryPolicy,
    SchedulerStats,
)


class TestPriority:
    def test_priority_values(self):
        assert Priority.LOW.value == "low"
        assert Priority.NORMAL.value == "normal"
        assert Priority.HIGH.value == "high"

    def test_priority_weights(self):
        assert Priority.LOW.weight == 0
        assert Priority.NORMAL.weight == 1
        assert Priority.HIGH.weight == 2

    def test_priority_ordering(self):
        assert Priority.HIGH.weight > Priority.NORMAL.weight > Priority.LOW.weight


class TestRetryPolicy:
    def test_default_policy(self):
        policy = RetryPolicy()
        assert policy.max_retries == 3
        assert policy.backoff_seconds == 30
        assert policy.backoff_multiplier == 2.0
        assert policy.max_backoff == 3600

    def test_backoff_calculation(self):
        policy = RetryPolicy(backoff_seconds=10, backoff_multiplier=2.0, max_backoff=120)
        assert policy.get_backoff(1) == 10.0  # 10 * 2^0
        assert policy.get_backoff(2) == 20.0  # 10 * 2^1
        assert policy.get_backoff(3) == 40.0  # 10 * 2^2
        assert policy.get_backoff(4) == 80.0  # 10 * 2^3
        assert policy.get_backoff(5) == 120.0  # capped at max_backoff

    def test_should_retry_all(self):
        policy = RetryPolicy()
        assert policy.should_retry("connection failed") is True
        assert policy.should_retry("timeout") is True

    def test_should_retry_specific_errors(self):
        policy = RetryPolicy(retry_on_errors=["timeout", "connection"])
        assert policy.should_retry("connection failed") is True
        assert policy.should_retry("timeout after 30s") is True
        assert policy.should_retry("invalid input") is False

    def test_validation_max_retries(self):
        with pytest.raises(Exception):
            RetryPolicy(max_retries=-1)

    def test_validation_backoff_multiplier(self):
        with pytest.raises(Exception):
            RetryPolicy(backoff_multiplier=0.5)


class TestJob:
    def test_create_basic_job(self):
        job = Job(name="test-job", handler="test.handler")
        assert job.name == "test-job"
        assert job.handler == "test.handler"
        assert job.priority == Priority.NORMAL
        assert job.enabled is True
        assert job.status == JobStatus.SCHEDULED
        assert job.run_count == 0
        assert job.fail_count == 0
        assert job.tags == []

    def test_job_has_id(self):
        job = Job(name="test", handler="h")
        assert len(job.id) == 12
        assert isinstance(job.id, str)

    def test_job_unique_ids(self):
        job1 = Job(name="test1", handler="h")
        job2 = Job(name="test2", handler="h")
        assert job1.id != job2.id

    def test_cron_job_is_recurring(self):
        job = Job(name="cron-job", handler="h", cron="0 9 * * *")
        assert job.is_recurring is True
        assert job.is_one_time is False
        assert job.is_immediate is False

    def test_delayed_job_is_one_time(self):
        job = Job(name="delayed", handler="h", delay=60)
        assert job.is_recurring is False
        assert job.is_one_time is True
        assert job.is_immediate is False

    def test_run_at_job_is_one_time(self):
        job = Job(name="scheduled", handler="h", run_at=datetime(2030, 1, 1, tzinfo=timezone.utc))
        assert job.is_recurring is False
        assert job.is_one_time is True
        assert job.is_immediate is False

    def test_immediate_job(self):
        job = Job(name="now", handler="h")
        assert job.is_recurring is False
        assert job.is_one_time is False
        assert job.is_immediate is True

    def test_cron_validation_valid(self):
        job = Job(name="test", handler="h", cron="0 * * * *")
        assert job.cron == "0 * * * *"

    def test_cron_validation_invalid(self):
        with pytest.raises(Exception):
            Job(name="test", handler="h", cron="invalid cron")

    def test_name_validation_blank(self):
        with pytest.raises(Exception):
            Job(name="  ", handler="h")

    def test_name_validation_too_long(self):
        with pytest.raises(Exception):
            Job(name="x" * 129, handler="h")

    def test_compute_next_run_cron(self):
        job = Job(name="test", handler="h", cron="0 9 * * *")
        now = datetime(2026, 1, 1, 8, 0, tzinfo=timezone.utc)
        next_run = job.compute_next_run(now)
        assert next_run is not None
        assert next_run.hour == 9
        assert next_run.minute == 0

    def test_compute_next_run_paused(self):
        job = Job(name="test", handler="h", cron="0 9 * * *", status=JobStatus.PAUSED)
        now = datetime(2026, 1, 1, 8, 0, tzinfo=timezone.utc)
        next_run = job.compute_next_run(now)
        assert next_run is None

    def test_compute_next_run_disabled(self):
        job = Job(name="test", handler="h", cron="0 9 * * *", enabled=False)
        now = datetime(2026, 1, 1, 8, 0, tzinfo=timezone.utc)
        next_run = job.compute_next_run(now)
        assert next_run is None

    def test_compute_next_run_delay(self):
        job = Job(name="test", handler="h", delay=3600)
        now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
        next_run = job.compute_next_run(now)
        assert next_run is not None
        assert next_run == now + timedelta(hours=1)

    def test_compute_next_run_at_future(self):
        future = datetime(2030, 1, 1, tzinfo=timezone.utc)
        job = Job(name="test", handler="h", run_at=future)
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        next_run = job.compute_next_run(now)
        assert next_run == future

    def test_compute_next_run_at_past(self):
        past = datetime(2020, 1, 1, tzinfo=timezone.utc)
        job = Job(name="test", handler="h", run_at=past)
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        next_run = job.compute_next_run(now)
        assert next_run is None  # Past due, one-time job

    def test_compute_next_run_immediate(self):
        job = Job(name="test", handler="h")
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        next_run = job.compute_next_run(now)
        assert next_run == now

    def test_mark_updated(self):
        job = Job(name="test", handler="h")
        old_updated = job.updated_at
        job.mark_updated()
        assert job.updated_at >= old_updated

    def test_job_with_tags(self):
        job = Job(name="test", handler="h", tags=["monitoring", "health"])
        assert "monitoring" in job.tags
        assert "health" in job.tags

    def test_job_with_retry_policy(self):
        policy = RetryPolicy(max_retries=5)
        job = Job(name="test", handler="h", retry_policy=policy)
        assert job.retry_policy.max_retries == 5

    def test_job_with_payload(self):
        job = Job(
            name="test",
            handler="h",
            payload={"url": "https://example.com", "method": "GET"},
        )
        assert job.payload["url"] == "https://example.com"

    def test_job_max_runs(self):
        job = Job(name="test", handler="h", max_runs=3)
        assert job.max_runs == 3

    def test_job_priority_high(self):
        job = Job(name="test", handler="h", priority=Priority.HIGH)
        assert job.priority == Priority.HIGH
        assert job.priority.weight == 2


class TestJobExecution:
    def test_create_success_execution(self):
        exec_record = JobExecution(
            job_id="abc123",
            job_name="test-job",
            status=ExecutionStatus.SUCCESS,
            duration_seconds=1.5,
        )
        assert exec_record.is_success is True
        assert exec_record.is_failure is False

    def test_create_failed_execution(self):
        exec_record = JobExecution(
            job_id="abc123",
            job_name="test-job",
            status=ExecutionStatus.FAILED,
            error_message="Connection refused",
        )
        assert exec_record.is_success is False
        assert exec_record.is_failure is True
        assert exec_record.error_message == "Connection refused"

    def test_create_timeout_execution(self):
        exec_record = JobExecution(
            job_id="abc123",
            job_name="test-job",
            status=ExecutionStatus.TIMEOUT,
        )
        assert exec_record.is_failure is True

    def test_execution_has_id(self):
        exec_record = JobExecution(
            job_id="abc123",
            job_name="test-job",
            status=ExecutionStatus.SUCCESS,
        )
        assert len(exec_record.id) == 12


class TestJobDependency:
    def test_create_dependency(self):
        dep = JobDependency(job_id="job1", depends_on_id="job2")
        assert dep.job_id == "job1"
        assert dep.depends_on_id == "job2"
        assert dep.on_status == ExecutionStatus.SUCCESS

    def test_dependency_on_failure(self):
        dep = JobDependency(job_id="job1", depends_on_id="job2", on_status=ExecutionStatus.FAILED)
        assert dep.on_status == ExecutionStatus.FAILED

    def test_dependency_has_id(self):
        dep = JobDependency(job_id="job1", depends_on_id="job2")
        assert len(dep.id) == 12


class TestSchedulerStats:
    def test_default_stats(self):
        stats = SchedulerStats()
        assert stats.total_jobs == 0
        assert stats.active_jobs == 0
        assert stats.tags == []

    def test_custom_stats(self):
        stats = SchedulerStats(total_jobs=10, active_jobs=5, failed_jobs=2, tags=["monitoring"])
        assert stats.total_jobs == 10
        assert stats.active_jobs == 5
        assert stats.failed_jobs == 2
        assert stats.tags == ["monitoring"]
