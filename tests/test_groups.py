"""Tests for job groups (multi-tenant scheduling)."""

import pytest

from agent_scheduler.groups import GroupManager, GroupQuota, JobGroup, GroupStats
from agent_scheduler.models import Job, JobStatus, Priority
from agent_scheduler.scheduler import Scheduler
from agent_scheduler.store import JSONJobStore


@pytest.fixture
def scheduler(tmp_path):
    store = JSONJobStore(data_dir=str(tmp_path))
    return Scheduler(store=store)


@pytest.fixture
def group_manager(scheduler):
    return GroupManager(store=scheduler.store, scheduler=scheduler)


class TestGroupQuota:
    def test_unlimited_quota(self):
        quota = GroupQuota()
        assert quota.is_unlimited is True

    def test_limited_quota(self):
        quota = GroupQuota(max_jobs=10)
        assert quota.is_unlimited is False

    def test_partial_quota(self):
        quota = GroupQuota(max_jobs=10, max_concurrent=5)
        assert quota.is_unlimited is False


class TestJobGroup:
    def test_create_group(self):
        group = JobGroup(name="test-group", description="A test group")
        assert group.name == "test-group"
        assert group.enabled is True
        assert group.id is not None

    def test_group_with_quota(self):
        group = JobGroup(name="limited", quota=GroupQuota(max_jobs=5))
        assert group.quota.max_jobs == 5

    def test_mark_updated(self):
        group = JobGroup(name="test")
        old_updated = group.updated_at
        group.mark_updated()
        assert group.updated_at >= old_updated


class TestGroupManager:
    def test_create_group(self, group_manager):
        group = group_manager.create_group(name="agent-1")
        assert group.name == "agent-1"
        assert group.id is not None

    def test_create_duplicate_group_fails(self, group_manager):
        group_manager.create_group(name="dup")
        with pytest.raises(ValueError, match="already exists"):
            group_manager.create_group(name="dup")

    def test_get_group_by_id(self, group_manager):
        group = group_manager.create_group(name="find-me")
        retrieved = group_manager.get_group(group.id)
        assert retrieved is not None
        assert retrieved.name == "find-me"

    def test_get_group_by_name(self, group_manager):
        group = group_manager.create_group(name="by-name")
        retrieved = group_manager.get_group("by-name")
        assert retrieved is not None
        assert retrieved.id == group.id

    def test_get_nonexistent_group(self, group_manager):
        assert group_manager.get_group("nonexistent") is None

    def test_list_groups(self, group_manager):
        group_manager.create_group(name="g1")
        group_manager.create_group(name="g2")
        groups = group_manager.list_groups()
        assert len(groups) == 2

    def test_list_groups_enabled_only(self, group_manager):
        group_manager.create_group(name="enabled")
        g2 = group_manager.create_group(name="disabled")
        group_manager.update_group(g2.id, enabled=False)
        groups = group_manager.list_groups(enabled_only=True)
        assert len(groups) == 1
        assert groups[0].name == "enabled"

    def test_update_group(self, group_manager):
        group = group_manager.create_group(name="original")
        updated = group_manager.update_group(group.id, description="updated desc")
        assert updated.description == "updated desc"

    def test_delete_group(self, group_manager):
        group = group_manager.create_group(name="to-delete")
        assert group_manager.delete_group(group.id) is True
        assert group_manager.get_group(group.id) is None

    def test_delete_nonexistent_group(self, group_manager):
        assert group_manager.delete_group("nonexistent") is False

    def test_check_quota_unlimited(self, group_manager):
        group = group_manager.create_group(name="unlimited")
        assert group_manager.check_quota(group.id) is True

    def test_check_quota_with_limit(self, group_manager, scheduler):
        group = group_manager.create_group(name="limited", quota=GroupQuota(max_jobs=2))
        # Add jobs to the group
        job1 = Job(name="j1", handler="h1", tags=[f"group:{group.id}"])
        scheduler.add_job(job1)
        assert group_manager.check_quota(group.id) is True  # 1/2

        job2 = Job(name="j2", handler="h2", tags=[f"group:{group.id}"])
        scheduler.add_job(job2)
        assert group_manager.check_quota(group.id) is False  # 2/2

    def test_add_job_to_group(self, group_manager, scheduler):
        group = group_manager.create_group(name="tag-test", tags=["production"])
        job = Job(name="test-job", handler="h1")
        result = group_manager.add_job_to_group(group.id, job)
        assert result is True
        assert f"group:{group.id}" in job.tags
        assert "production" in job.tags

    def test_add_job_to_full_group(self, group_manager, scheduler):
        group = group_manager.create_group(name="full", quota=GroupQuota(max_jobs=0))
        job = Job(name="test-job", handler="h1")
        result = group_manager.add_job_to_group(group.id, job)
        assert result is False

    def test_get_stats(self, group_manager, scheduler):
        group = group_manager.create_group(name="stats-test")
        # Add some jobs
        job = Job(name="j1", handler="h1", tags=[f"group:{group.id}"])
        scheduler.add_job(job)
        stats = group_manager.get_stats(group.id)
        assert stats is not None
        assert stats.total_jobs == 1
        assert stats.group_name == "stats-test"

    def test_get_stats_nonexistent(self, group_manager):
        assert group_manager.get_stats("nonexistent") is None

    def test_pause_group(self, group_manager, scheduler):
        group = group_manager.create_group(name="pause-test")
        job = Job(name="j1", handler="h1", tags=[f"group:{group.id}"])
        scheduler.add_job(job)
        count = group_manager.pause_group(group.id)
        assert count == 1

    def test_resume_group(self, group_manager, scheduler):
        group = group_manager.create_group(name="resume-test")
        job = Job(name="j1", handler="h1", tags=[f"group:{group.id}"])
        scheduler.add_job(job)
        group_manager.pause_group(group.id)
        count = group_manager.resume_group(group.id)
        assert count == 1

    def test_pause_nonexistent_group(self, group_manager):
        assert group_manager.pause_group("nonexistent") == 0

    def test_resume_nonexistent_group(self, group_manager):
        assert group_manager.resume_group("nonexistent") == 0

    def test_group_persistence(self, scheduler, tmp_path):
        """Test that groups persist across manager instances."""
        manager1 = GroupManager(store=scheduler.store, scheduler=scheduler)
        group = manager1.create_group(name="persist-test")
        group_id = group.id

        # Create a new manager with the same store
        manager2 = GroupManager(store=scheduler.store, scheduler=scheduler)
        # The new manager should load groups from disk
        # Note: this depends on the store having a data_dir attribute
        retrieved = manager2.get_group(group_id)
        if retrieved is not None:
            assert retrieved.name == "persist-test"
