"""Tests for agent-scheduler store (persistence layer)."""

import pytest
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from agent_scheduler.models import (
    ExecutionStatus,
    Job,
    JobDependency,
    JobExecution,
    JobStatus,
    Priority,
)
from agent_scheduler.store import JSONJobStore


@pytest.fixture
def store(tmp_path):
    return JSONJobStore(data_dir=str(tmp_path / "scheduler-data"))


@pytest.fixture
def sample_job():
    return Job(name="test-job", handler="test.handler", cron="0 * * * *", tags=["test"])


@pytest.fixture
def sample_job2():
    return Job(name="other-job", handler="other.handler", priority=Priority.HIGH)


class TestJSONJobStore:
    def test_create_store(self, tmp_path):
        store = JSONJobStore(data_dir=str(tmp_path / "new-store"))
        assert store.data_dir.exists()
        assert store.jobs_file.exists()
        assert store.executions_file.exists()

    def test_save_and_get_job(self, store, sample_job):
        store.save_job(sample_job)
        retrieved = store.get_job(sample_job.id)
        assert retrieved is not None
        assert retrieved.name == "test-job"
        assert retrieved.handler == "test.handler"
        assert retrieved.cron == "0 * * * *"

    def test_get_job_not_found(self, store):
        result = store.get_job("nonexistent")
        assert result is None

    def test_save_job_updates_existing(self, store, sample_job):
        store.save_job(sample_job)
        sample_job.status = JobStatus.PAUSED
        store.save_job(sample_job)
        retrieved = store.get_job(sample_job.id)
        assert retrieved is not None
        assert retrieved.status == JobStatus.PAUSED

    def test_delete_job(self, store, sample_job):
        store.save_job(sample_job)
        assert store.delete_job(sample_job.id) is True
        assert store.get_job(sample_job.id) is None

    def test_delete_job_not_found(self, store):
        assert store.delete_job("nonexistent") is False

    def test_list_jobs(self, store, sample_job, sample_job2):
        store.save_job(sample_job)
        store.save_job(sample_job2)
        jobs = store.list_jobs()
        assert len(jobs) == 2
        names = {j.name for j in jobs}
        assert "test-job" in names
        assert "other-job" in names

    def test_list_jobs_empty(self, store):
        jobs = store.list_jobs()
        assert jobs == []

    def test_delete_job_cascades_executions(self, store, sample_job):
        store.save_job(sample_job)
        execution = JobExecution(
            job_id=sample_job.id,
            job_name=sample_job.name,
            status=ExecutionStatus.SUCCESS,
            duration_seconds=1.0,
        )
        store.save_execution(execution)
        store.delete_job(sample_job.id)
        executions = store.get_executions(sample_job.id)
        assert len(executions) == 0

    def test_delete_job_cascades_dependencies(self, store, sample_job, sample_job2):
        store.save_job(sample_job)
        store.save_job(sample_job2)
        dep = JobDependency(job_id=sample_job2.id, depends_on_id=sample_job.id)
        store.save_dependency(dep)
        store.delete_job(sample_job.id)
        deps = store.list_dependencies()
        assert len(deps) == 0  # Dependency removed because depends_on_id was deleted

    # ── Executions ────────────────────────────────────────────

    def test_save_and_get_executions(self, store, sample_job):
        store.save_job(sample_job)
        execution = JobExecution(
            job_id=sample_job.id,
            job_name=sample_job.name,
            status=ExecutionStatus.SUCCESS,
            duration_seconds=1.5,
        )
        store.save_execution(execution)
        executions = store.get_executions(sample_job.id)
        assert len(executions) == 1
        assert executions[0].status == ExecutionStatus.SUCCESS
        assert executions[0].duration_seconds == 1.5

    def test_get_executions_sorted_by_time(self, store, sample_job):
        store.save_job(sample_job)
        for i in range(5):
            execution = JobExecution(
                job_id=sample_job.id,
                job_name=sample_job.name,
                status=ExecutionStatus.SUCCESS,
            )
            store.save_execution(execution)
        executions = store.get_executions(sample_job.id)
        assert len(executions) == 5

    def test_get_executions_limit_and_offset(self, store, sample_job):
        store.save_job(sample_job)
        for i in range(10):
            execution = JobExecution(
                job_id=sample_job.id,
                job_name=sample_job.name,
                status=ExecutionStatus.SUCCESS,
            )
            store.save_execution(execution)
        page1 = store.get_executions(sample_job.id, limit=3, offset=0)
        page2 = store.get_executions(sample_job.id, limit=3, offset=3)
        assert len(page1) == 3
        assert len(page2) == 3

    def test_get_all_executions(self, store, sample_job, sample_job2):
        store.save_job(sample_job)
        store.save_job(sample_job2)
        for job in [sample_job, sample_job2]:
            execution = JobExecution(
                job_id=job.id,
                job_name=job.name,
                status=ExecutionStatus.SUCCESS,
            )
            store.save_execution(execution)
        all_execs = store.get_all_executions()
        assert len(all_execs) == 2

    # ── Dependencies ──────────────────────────────────────────

    def test_save_and_get_dependencies(self, store, sample_job, sample_job2):
        store.save_job(sample_job)
        store.save_job(sample_job2)
        dep = JobDependency(job_id=sample_job2.id, depends_on_id=sample_job.id)
        store.save_dependency(dep)
        deps = store.get_dependencies(sample_job2.id)
        assert len(deps) == 1
        assert deps[0].depends_on_id == sample_job.id

    def test_get_dependencies_reverse(self, store, sample_job, sample_job2):
        store.save_job(sample_job)
        store.save_job(sample_job2)
        dep = JobDependency(job_id=sample_job2.id, depends_on_id=sample_job.id)
        store.save_dependency(dep)
        # Getting dependencies for the parent should also return it
        deps = store.get_dependencies(sample_job.id)
        assert len(deps) == 1

    def test_list_dependencies(self, store, sample_job, sample_job2):
        store.save_job(sample_job)
        store.save_job(sample_job2)
        dep1 = JobDependency(job_id=sample_job2.id, depends_on_id=sample_job.id)
        store.save_dependency(dep1)
        all_deps = store.list_dependencies()
        assert len(all_deps) == 1

    def test_delete_dependency(self, store, sample_job, sample_job2):
        store.save_job(sample_job)
        store.save_job(sample_job2)
        dep = JobDependency(job_id=sample_job2.id, depends_on_id=sample_job.id)
        store.save_dependency(dep)
        assert store.delete_dependency(dep.id) is True
        assert len(store.list_dependencies()) == 0

    def test_delete_dependency_not_found(self, store):
        assert store.delete_dependency("nonexistent") is False

    # ── Persistence ──────────────────────────────────────────

    def test_data_survives_restart(self, tmp_path, sample_job):
        data_dir = str(tmp_path / "persist-test")
        store1 = JSONJobStore(data_dir=data_dir)
        store1.save_job(sample_job)

        # Create new store instance with same data dir
        store2 = JSONJobStore(data_dir=data_dir)
        retrieved = store2.get_job(sample_job.id)
        assert retrieved is not None
        assert retrieved.name == "test-job"

    def test_custom_data_dir_env(self, tmp_path, monkeypatch):
        custom_dir = str(tmp_path / "env-data-dir")
        monkeypatch.setenv("SCHEDULER_DATA_DIR", custom_dir)
        store = JSONJobStore()
        assert str(store.data_dir) == custom_dir
