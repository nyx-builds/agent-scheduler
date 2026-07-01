"""Tests for Dead Letter Queue (DLQ) — v0.5.0 feature."""

import pytest
import asyncio
from datetime import datetime, timezone

from agent_scheduler.dlq import (
    DLQEntry,
    DLQReason,
    DLQStats,
    DeadLetterQueue,
)
from agent_scheduler.models import Job, JobStatus, Priority, RetryPolicy
from agent_scheduler.scheduler import Scheduler
from agent_scheduler.store import JSONJobStore


# ── Fixtures ──────────────────────────────────────────────────


@pytest.fixture
def store(tmp_path):
    return JSONJobStore(data_dir=str(tmp_path / "dlq-test"))


@pytest.fixture
def scheduler(store):
    return Scheduler(store=store)


@pytest.fixture
def dlq(scheduler):
    return DeadLetterQueue(scheduler=scheduler)


@pytest.fixture
def standalone_dlq():
    """DLQ without a scheduler — for testing basic operations."""
    return DeadLetterQueue(scheduler=None)


# ── DLQEntry Model Tests ──────────────────────────────────────


class TestDLQEntry:
    def test_defaults(self):
        entry = DLQEntry(
            job_id="job123",
            job_name="Test Job",
            handler="test.handler",
            payload={"key": "value"},
            reason=DLQReason.MAX_RETRIES_EXHAUSTED,
        )
        assert entry.job_id == "job123"
        assert entry.job_name == "Test Job"
        assert entry.handler == "test.handler"
        assert entry.payload == {"key": "value"}
        assert entry.reason == DLQReason.MAX_RETRIES_EXHAUSTED
        assert entry.error_message == ""
        assert entry.retry_attempts == 0
        assert entry.original_job == {}
        assert entry.resolved is False
        assert entry.resolved_at is None
        assert entry.resolution is None
        assert entry.id  # Auto-generated
        assert entry.created_at  # Auto-generated

    def test_with_all_fields(self):
        entry = DLQEntry(
            job_id="job456",
            job_name="Failed Task",
            handler="data.processor",
            payload={"batch": [1, 2, 3]},
            reason=DLQReason.TIMEOUT,
            error_message="Handler timed out after 300s",
            retry_attempts=3,
            original_job={"name": "Failed Task", "handler": "data.processor"},
        )
        assert entry.reason == DLQReason.TIMEOUT
        assert entry.retry_attempts == 3
        assert entry.error_message == "Handler timed out after 300s"
        assert entry.original_job["name"] == "Failed Task"

    def test_age_seconds(self):
        entry = DLQEntry(
            job_id="j1",
            job_name="Old Job",
            handler="h",
            payload={},
            reason=DLQReason.MANUAL,
        )
        assert entry.age_seconds >= 0

    def test_all_reasons(self):
        """Verify all enum values are accessible."""
        assert DLQReason.MAX_RETRIES_EXHAUSTED == "max_retries_exhausted"
        assert DLQReason.TIMEOUT == "timeout"
        assert DLQReason.HANDLER_NOT_FOUND == "handler_not_found"
        assert DLQReason.MANUAL == "manual"


# ── DLQ Add / List / Get Tests ────────────────────────────────


class TestDLQAddListGet:
    def test_add_and_get(self, standalone_dlq):
        entry = standalone_dlq.add(
            job_id="job1",
            job_name="My Job",
            handler="test.handler",
            payload={"data": 123},
            reason=DLQReason.MAX_RETRIES_EXHAUSTED,
            error_message="Connection refused",
            retry_attempts=3,
        )
        assert entry.id

        fetched = standalone_dlq.get(entry.id)
        assert fetched is not None
        assert fetched.job_id == "job1"
        assert fetched.reason == DLQReason.MAX_RETRIES_EXHAUSTED

    def test_get_nonexistent(self, standalone_dlq):
        assert standalone_dlq.get("nonexistent") is None

    def test_list_entries_empty(self, standalone_dlq):
        entries = standalone_dlq.list_entries()
        assert entries == []

    def test_list_entries_all(self, standalone_dlq):
        for i in range(5):
            standalone_dlq.add(
                job_id=f"job{i}",
                job_name=f"Job {i}",
                handler="handler",
                payload={},
                reason=DLQReason.MAX_RETRIES_EXHAUSTED,
            )
        entries = standalone_dlq.list_entries()
        assert len(entries) == 5
        # Newest first
        assert entries[0].job_id == "job4"
        assert entries[4].job_id == "job0"

    def test_list_unresolved_only(self, standalone_dlq):
        e1 = standalone_dlq.add("j1", "J1", "h", {}, DLQReason.TIMEOUT)
        e2 = standalone_dlq.add("j2", "J2", "h", {}, DLQReason.TIMEOUT)
        standalone_dlq.discard(e1.id)

        unresolved = standalone_dlq.list_entries(unresolved_only=True)
        assert len(unresolved) == 1
        assert unresolved[0].id == e2.id

    def test_list_by_reason(self, standalone_dlq):
        standalone_dlq.add("j1", "J1", "h", {}, DLQReason.TIMEOUT)
        standalone_dlq.add("j2", "J2", "h", {}, DLQReason.MAX_RETRIES_EXHAUSTED)
        standalone_dlq.add("j3", "J3", "h", {}, DLQReason.TIMEOUT)

        timeouts = standalone_dlq.list_entries(reason=DLQReason.TIMEOUT)
        assert len(timeouts) == 2

        exhausted = standalone_dlq.list_entries(reason=DLQReason.MAX_RETRIES_EXHAUSTED)
        assert len(exhausted) == 1

    def test_list_with_pagination(self, standalone_dlq):
        for i in range(10):
            standalone_dlq.add(f"job{i}", f"Job {i}", "h", {}, DLQReason.MANUAL)

        page1 = standalone_dlq.list_entries(limit=3, offset=0)
        page2 = standalone_dlq.list_entries(limit=3, offset=3)
        assert len(page1) == 3
        assert len(page2) == 3
        assert page1[0].id != page2[0].id

    def test_count(self, standalone_dlq):
        standalone_dlq.add("j1", "J1", "h", {}, DLQReason.TIMEOUT)
        standalone_dlq.add("j2", "J2", "h", {}, DLQReason.TIMEOUT)
        assert standalone_dlq.count() == 2

        e = standalone_dlq.list_entries()[0]
        standalone_dlq.discard(e.id)
        assert standalone_dlq.count() == 2  # Still counted
        assert standalone_dlq.count(unresolved_only=True) == 1


# ── DLQ Resolution Tests ──────────────────────────────────────


class TestDLQResolve:
    def test_discard(self, standalone_dlq):
        entry = standalone_dlq.add("j1", "J1", "h", {}, DLQReason.MANUAL)
        result = standalone_dlq.discard(entry.id)
        assert result is True

        updated = standalone_dlq.get(entry.id)
        assert updated.resolved is True
        assert updated.resolution == "discarded"
        assert updated.resolved_at is not None

    def test_discard_nonexistent(self, standalone_dlq):
        assert standalone_dlq.discard("nonexistent") is False

    def test_replay_with_scheduler(self, dlq, scheduler):
        """Test replay with a real scheduler — job still exists."""
        # Add a job to the scheduler
        job = Job(name="Replay Test", handler="test.handler", payload={"val": 1})
        job.fail_count = 5
        job.last_error = "Something broke"
        job.enabled = False
        job.status = JobStatus.FAILED
        scheduler.store.save_job(job)

        # Add to DLQ
        entry = dlq.add(
            job_id=job.id,
            job_name=job.name,
            handler=job.handler,
            payload=job.payload,
            reason=DLQReason.MAX_RETRIES_EXHAUSTED,
            error_message="Something broke",
            retry_attempts=3,
            original_job=job.model_dump(mode="json"),
        )

        # Replay
        replayed = dlq.replay(entry.id)
        assert replayed is not None
        assert replayed.id == job.id
        assert replayed.enabled is True
        assert replayed.fail_count == 0
        assert replayed.last_error is None
        assert replayed.status == JobStatus.SCHEDULED

        # DLQ entry marked as resolved
        updated = dlq.get(entry.id)
        assert updated.resolved is True
        assert updated.resolution == "replayed"

    def test_replay_with_payload_override(self, dlq, scheduler):
        """Test replay with custom payload."""
        job = Job(name="Override Test", handler="test.handler", payload={"original": True})
        scheduler.store.save_job(job)

        entry = dlq.add(
            job_id=job.id,
            job_name=job.name,
            handler=job.handler,
            payload={"original": True},
            reason=DLQReason.MANUAL,
        )

        replayed = dlq.replay(entry.id, payload_override={"new_key": "new_value"})
        assert replayed is not None
        assert replayed.payload["original"] is True
        assert replayed.payload["new_key"] == "new_value"

    def test_replay_deleted_job(self, dlq, scheduler):
        """Test replay when original job was deleted — recreate from snapshot."""
        original = Job(name="Deleted Job", handler="test.handler", payload={"x": 1})
        original_snapshot = original.model_dump(mode="json")

        entry = dlq.add(
            job_id=original.id,
            job_name=original.name,
            handler=original.handler,
            payload=original.payload,
            reason=DLQReason.MANUAL,
            original_job=original_snapshot,
        )

        # Don't save the job — simulate deletion
        replayed = dlq.replay(entry.id)
        assert replayed is not None
        assert replayed.name == "Deleted Job"
        assert replayed.handler == "test.handler"

    def test_replay_nonexistent_entry(self, dlq):
        result = dlq.replay("nonexistent")
        assert result is None

    def test_replay_without_scheduler_marks_resolved(self, standalone_dlq):
        entry = standalone_dlq.add("j1", "J1", "h", {}, DLQReason.MANUAL)
        result = standalone_dlq.replay(entry.id)
        # Without scheduler, returns None but marks resolved
        assert result is None
        updated = standalone_dlq.get(entry.id)
        assert updated.resolved is True


# ── DLQ Purge Tests ───────────────────────────────────────────


class TestDLQPurge:
    def test_purge_resolved_only(self, standalone_dlq):
        e1 = standalone_dlq.add("j1", "J1", "h", {}, DLQReason.MANUAL)
        e2 = standalone_dlq.add("j2", "J2", "h", {}, DLQReason.MANUAL)
        standalone_dlq.discard(e1.id)

        purged = standalone_dlq.purge(resolved_only=True)
        assert purged == 1
        assert standalone_dlq.count() == 1

    def test_purge_all(self, standalone_dlq):
        standalone_dlq.add("j1", "J1", "h", {}, DLQReason.MANUAL)
        standalone_dlq.add("j2", "J2", "h", {}, DLQReason.MANUAL)

        purged = standalone_dlq.purge(resolved_only=False)
        assert purged == 2
        assert standalone_dlq.count() == 0

    def test_purge_empty(self, standalone_dlq):
        purged = standalone_dlq.purge()
        assert purged == 0


# ── DLQ Bulk Operations Tests ─────────────────────────────────


class TestDLQBulkOps:
    def test_replay_all(self, dlq, scheduler):
        for i in range(3):
            job = Job(name=f"Bulk {i}", handler="test.handler")
            scheduler.store.save_job(job)
            dlq.add(
                job_id=job.id,
                job_name=job.name,
                handler=job.handler,
                payload={},
                reason=DLQReason.MANUAL,
            )

        count = dlq.replay_all()
        assert count == 3

    def test_replay_all_by_reason(self, dlq, scheduler):
        for i in range(3):
            job = Job(name=f"Bulk {i}", handler="test.handler")
            scheduler.store.save_job(job)
            dlq.add(
                job_id=job.id,
                job_name=job.name,
                handler=job.handler,
                payload={},
                reason=DLQReason.TIMEOUT if i < 2 else DLQReason.MANUAL,
            )

        count = dlq.replay_all(reason=DLQReason.TIMEOUT)
        assert count == 2

    def test_discard_all(self, standalone_dlq):
        for i in range(3):
            standalone_dlq.add(f"j{i}", f"J{i}", "h", {}, DLQReason.MANUAL)

        count = standalone_dlq.discard_all()
        assert count == 3
        assert standalone_dlq.count(unresolved_only=True) == 0

    def test_discard_all_by_reason(self, standalone_dlq):
        standalone_dlq.add("j1", "J1", "h", {}, DLQReason.TIMEOUT)
        standalone_dlq.add("j2", "J2", "h", {}, DLQReason.MANUAL)

        count = standalone_dlq.discard_all(reason=DLQReason.TIMEOUT)
        assert count == 1
        # Only TIMEOUT should be discarded
        all_entries = standalone_dlq.list_entries()
        for e in all_entries:
            if e.reason == DLQReason.TIMEOUT:
                assert e.resolved is True
            elif e.reason == DLQReason.MANUAL:
                assert e.resolved is False


# ── DLQ Stats Tests ───────────────────────────────────────────


class TestDLQStats:
    def test_empty_stats(self, standalone_dlq):
        stats = standalone_dlq.get_stats()
        assert stats.total_entries == 0
        assert stats.unresolved == 0
        assert stats.resolved == 0
        assert stats.by_reason == {}
        assert stats.oldest_unresolved_age_seconds is None

    def test_stats_with_entries(self, standalone_dlq):
        standalone_dlq.add("j1", "J1", "h", {}, DLQReason.TIMEOUT)
        standalone_dlq.add("j2", "J2", "h", {}, DLQReason.MAX_RETRIES_EXHAUSTED)
        e3 = standalone_dlq.add("j3", "J3", "h", {}, DLQReason.TIMEOUT)
        standalone_dlq.discard(e3.id)

        stats = standalone_dlq.get_stats()
        assert stats.total_entries == 3
        assert stats.unresolved == 2
        assert stats.resolved == 1
        assert stats.by_reason.get("timeout") == 2
        assert stats.by_reason.get("max_retries_exhausted") == 1
        assert stats.oldest_unresolved_age_seconds is not None
        assert stats.oldest_unresolved_age_seconds >= 0


# ── DLQ Persistence Tests ─────────────────────────────────────


class TestDLQPersistence:
    def test_persistence_across_instances(self, store, scheduler):
        """Test that DLQ entries persist across instances."""
        dlq1 = DeadLetterQueue(scheduler=scheduler)
        entry = dlq1.add(
            "persist-job",
            "Persist Test",
            "handler",
            {"data": 42},
            DLQReason.MAX_RETRIES_EXHAUSTED,
        )

        # Create new DLQ with same scheduler/store
        dlq2 = DeadLetterQueue(scheduler=scheduler)
        entries = dlq2.list_entries()
        assert len(entries) == 1
        assert entries[0].job_id == "persist-job"
        assert entries[0].payload == {"data": 42}


# ── DLQ Scheduler Integration Tests ───────────────────────────


class TestDLQSchedulerIntegration:
    def test_failed_job_goes_to_dlq(self, store):
        """Verify that a permanently failed job ends up in the DLQ."""
        scheduler = Scheduler(store=store)
        assert scheduler.dlq is not None

        # Register a handler that always fails
        def failing_handler(payload):
            raise RuntimeError("Always fails")

        scheduler.handlers.register("always_fail", failing_handler)

        # Create a one-time job with no retries
        job = Job(
            name="Failing Job",
            handler="always_fail",
            delay=0,  # Run immediately
        )
        scheduler.add_job(job)

        # Run it
        asyncio.run(scheduler.run_job(job.id))

        # Should be in DLQ
        entries = scheduler.dlq.list_entries()
        assert len(entries) == 1
        assert entries[0].job_id == job.id
        assert entries[0].reason == DLQReason.MAX_RETRIES_EXHAUSTED
        assert "Always fails" in entries[0].error_message

    def test_successful_job_not_in_dlq(self, store):
        """Verify successful jobs don't go to DLQ."""
        scheduler = Scheduler(store=store)

        def success_handler(payload):
            return {"result": "ok"}

        scheduler.handlers.register("success", success_handler)

        job = Job(name="Success Job", handler="success", delay=0)
        scheduler.add_job(job)
        asyncio.run(scheduler.run_job(job.id))

        assert scheduler.dlq.count() == 0

    def test_recurring_job_failure_not_dlq(self, store):
        """Recurring jobs that fail shouldn't go to DLQ (they'll retry next cycle)."""
        scheduler = Scheduler(store=store)

        def fail_handler(payload):
            raise ValueError("Temporary failure")

        scheduler.handlers.register("temp_fail", fail_handler)

        job = Job(
            name="Recurring Fail",
            handler="temp_fail",
            cron="* * * * *",  # Every minute
        )
        scheduler.add_job(job)
        asyncio.run(scheduler.run_job(job.id))

        # Recurring job with future runs shouldn't be dead-lettered
        assert scheduler.dlq.count() == 0

    def test_dlq_disabled(self, store):
        """Test that DLQ can be disabled."""
        scheduler = Scheduler(store=store, enable_dlq=False)
        assert scheduler.dlq is None
