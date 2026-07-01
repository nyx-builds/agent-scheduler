"""Dead Letter Queue for permanently failed jobs.

When a job exhausts all retry attempts, it is moved to the DLQ
for later inspection, replay, or manual discard. This prevents
silent failures in autonomous agent workflows and provides an
audit trail of what went wrong.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class DLQReason(str, Enum):
    """Reason a job was moved to the dead letter queue."""

    MAX_RETRIES_EXHAUSTED = "max_retries_exhausted"
    TIMEOUT = "timeout"
    HANDLER_NOT_FOUND = "handler_not_found"
    MANUAL = "manual"


class DLQEntry(BaseModel):
    """A dead-lettered job entry with full context for debugging."""

    id: str = Field(default_factory=lambda: uuid.uuid4().hex[:12])
    job_id: str = Field(..., description="Original job ID")
    job_name: str = Field(..., description="Original job name")
    handler: str = Field(..., description="Handler that was invoked")
    payload: dict[str, Any] = Field(default_factory=dict, description="Last payload sent to handler")

    reason: DLQReason = Field(..., description="Why the job was dead-lettered")
    error_message: str = Field(default="", description="Final error that caused dead-lettering")
    retry_attempts: int = Field(default=0, ge=0, description="Number of retries before dead-lettering")

    # Preserve original job config for replay
    original_job: dict[str, Any] = Field(
        default_factory=dict,
        description="Snapshot of the original Job config for replay",
    )

    # Lifecycle
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    resolved: bool = Field(default=False, description="Whether this entry has been resolved (replayed/discarded)")
    resolved_at: Optional[datetime] = Field(default=None)
    resolution: Optional[str] = Field(default=None, description="How resolved: 'replayed', 'discarded', etc.")

    @property
    def age_seconds(self) -> float:
        """Age of this DLQ entry in seconds."""
        return (datetime.now(timezone.utc) - self.created_at).total_seconds()


class DLQStats(BaseModel):
    """Summary statistics for the dead letter queue."""

    total_entries: int = 0
    unresolved: int = 0
    resolved: int = 0
    by_reason: dict[str, int] = Field(default_factory=dict)
    oldest_unresolved_age_seconds: Optional[float] = None


class DeadLetterQueue:
    """Manages dead-lettered jobs — add, list, replay, discard.

    Integrates with the Scheduler for replay functionality.
    Jobs that exhaust retries are automatically sent here by
    the scheduler's execution engine.
    """

    def __init__(self, scheduler: Any = None) -> None:
        self._scheduler = scheduler
        self._entries: list[DLQEntry] = []
        self._load_from_store()

    # ── Store Integration ─────────────────────────────────────

    def _get_store(self) -> Any:
        """Get the scheduler's store if available."""
        if self._scheduler is not None:
            return getattr(self._scheduler, "store", None)
        return None

    def _load_from_store(self) -> None:
        """Load existing DLQ entries from storage."""
        store = self._get_store()
        if store is None:
            return
        try:
            import json
            from pathlib import Path

            if hasattr(store, "data_dir"):
                dlq_file = Path(store.data_dir) / "dlq.json"
                if dlq_file.exists():
                    data = json.loads(dlq_file.read_text())
                    self._entries = [DLQEntry.model_validate(e) for e in data]
        except Exception:
            pass

    def _save_to_store(self) -> None:
        """Persist DLQ entries to storage."""
        store = self._get_store()
        if store is None:
            return
        try:
            import json
            from pathlib import Path

            if hasattr(store, "data_dir"):
                dlq_file = Path(store.data_dir) / "dlq.json"
                dlq_file.write_text(
                    json.dumps(
                        [e.model_dump(mode="json") for e in self._entries],
                        indent=2,
                        default=str,
                    )
                )
        except Exception:
            pass

    # ── Core Operations ───────────────────────────────────────

    def add(
        self,
        job_id: str,
        job_name: str,
        handler: str,
        payload: dict[str, Any],
        reason: DLQReason,
        error_message: str = "",
        retry_attempts: int = 0,
        original_job: Optional[dict[str, Any]] = None,
    ) -> DLQEntry:
        """Add a job to the dead letter queue.

        Args:
            job_id: Original job ID
            job_name: Original job name
            handler: Handler that was invoked
            payload: Last payload sent to handler
            reason: Why the job is being dead-lettered
            error_message: Final error message
            retry_attempts: Number of retries attempted
            original_job: Snapshot of the Job config for replay

        Returns:
            The created DLQEntry
        """
        entry = DLQEntry(
            job_id=job_id,
            job_name=job_name,
            handler=handler,
            payload=payload,
            reason=reason,
            error_message=error_message,
            retry_attempts=retry_attempts,
            original_job=original_job or {},
        )
        self._entries.append(entry)
        self._save_to_store()
        return entry

    def get(self, entry_id: str) -> Optional[DLQEntry]:
        """Get a DLQ entry by ID."""
        for entry in self._entries:
            if entry.id == entry_id:
                return entry
        return None

    def list_entries(
        self,
        unresolved_only: bool = False,
        reason: Optional[DLQReason] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[DLQEntry]:
        """List DLQ entries with optional filtering.

        Args:
            unresolved_only: Show only entries that haven't been resolved
            reason: Filter by dead-letter reason
            limit: Max entries to return
            offset: Number of entries to skip

        Returns:
            List of DLQEntry objects, newest first
        """
        entries = list(reversed(self._entries))  # newest first

        if unresolved_only:
            entries = [e for e in entries if not e.resolved]
        if reason is not None:
            entries = [e for e in entries if e.reason == reason]

        return entries[offset : offset + limit]

    def count(self, unresolved_only: bool = False) -> int:
        """Count DLQ entries."""
        if unresolved_only:
            return sum(1 for e in self._entries if not e.resolved)
        return len(self._entries)

    # ── Resolution Operations ─────────────────────────────────

    def replay(
        self,
        entry_id: str,
        payload_override: Optional[dict[str, Any]] = None,
        reset_retries: bool = True,
    ) -> Optional[Any]:
        """Replay a dead-lettered job by creating a new execution.

        This re-submits the job to the scheduler with the original
        (or overridden) payload. The DLQ entry is marked as resolved.

        Args:
            entry_id: DLQ entry ID to replay
            payload_override: Optional new payload (merges with original)
            reset_retries: If True, clear the job's failure state

        Returns:
            The replayed Job, or None if entry not found or scheduler unavailable
        """
        entry = self.get(entry_id)
        if entry is None:
            return None

        if self._scheduler is None:
            # Mark resolved even without scheduler (for testing)
            entry.resolved = True
            entry.resolved_at = datetime.now(timezone.utc)
            entry.resolution = "replayed"
            self._save_to_store()
            return None

        # Try to find the original job
        job = self._scheduler.get_job(entry.job_id)

        from agent_scheduler.models import Job, JobStatus

        if job is not None:
            # Job still exists — reset it for re-execution
            merged_payload = {**entry.payload}
            if payload_override:
                merged_payload.update(payload_override)
            job.payload = merged_payload

            if reset_retries:
                job.fail_count = 0
                job.last_error = None

            # Re-enable and reschedule
            job.enabled = True
            job.status = JobStatus.SCHEDULED
            job.next_run_at = job.compute_next_run()
            if job.next_run_at is None:
                job.next_run_at = datetime.now(timezone.utc)
            job.mark_updated()
            self._scheduler.store.save_job(job)
        else:
            # Original job was deleted — recreate from snapshot
            snapshot = entry.original_job.copy()
            snapshot["payload"] = {**entry.payload}
            if payload_override:
                snapshot["payload"].update(payload_override)
            if reset_retries:
                snapshot["fail_count"] = 0
                snapshot["last_error"] = None
            snapshot["enabled"] = True
            snapshot["status"] = JobStatus.SCHEDULED.value
            snapshot["id"] = entry.job_id  # Preserve original ID

            try:
                job = Job.model_validate(snapshot)
                job.next_run_at = datetime.now(timezone.utc)
                self._scheduler.add_job(job)
            except Exception:
                # If snapshot is invalid, create minimal job
                job = Job(
                    id=entry.job_id,
                    name=entry.job_name,
                    handler=entry.handler,
                    payload={**entry.payload, **(payload_override or {})},
                )
                if reset_retries:
                    job.fail_count = 0
                self._scheduler.add_job(job)

        # Mark entry as resolved
        entry.resolved = True
        entry.resolved_at = datetime.now(timezone.utc)
        entry.resolution = "replayed"
        self._save_to_store()

        return job

    def discard(self, entry_id: str) -> bool:
        """Discard a DLQ entry (mark as resolved without replaying).

        Args:
            entry_id: DLQ entry ID

        Returns:
            True if entry was found and discarded
        """
        entry = self.get(entry_id)
        if entry is None:
            return False

        entry.resolved = True
        entry.resolved_at = datetime.now(timezone.utc)
        entry.resolution = "discarded"
        self._save_to_store()
        return True

    def purge(self, resolved_only: bool = True) -> int:
        """Remove DLQ entries from storage.

        Args:
            resolved_only: If True (default), only remove resolved entries.
                          If False, remove ALL entries.

        Returns:
            Number of entries purged
        """
        if resolved_only:
            before = len(self._entries)
            self._entries = [e for e in self._entries if not e.resolved]
            purged = before - len(self._entries)
        else:
            purged = len(self._entries)
            self._entries = []

        if purged > 0:
            self._save_to_store()
        return purged

    # ── Statistics ────────────────────────────────────────────

    def get_stats(self) -> DLQStats:
        """Get summary statistics for the dead letter queue."""
        total = len(self._entries)
        unresolved_list = [e for e in self._entries if not e.resolved]
        resolved_list = [e for e in self._entries if e.resolved]

        by_reason: dict[str, int] = {}
        for entry in self._entries:
            reason = entry.reason.value
            by_reason[reason] = by_reason.get(reason, 0) + 1

        oldest_age = None
        if unresolved_list:
            oldest = min(self._entries, key=lambda e: e.created_at)
            oldest_age = oldest.age_seconds

        return DLQStats(
            total_entries=total,
            unresolved=len(unresolved_list),
            resolved=len(resolved_list),
            by_reason=by_reason,
            oldest_unresolved_age_seconds=oldest_age,
        )

    # ── Bulk Operations ───────────────────────────────────────

    def replay_all(self, reason: Optional[DLQReason] = None) -> int:
        """Replay all unresolved DLQ entries.

        Args:
            reason: Only replay entries with this reason (None = all)

        Returns:
            Number of entries replayed
        """
        count = 0
        for entry in list(self._entries):
            if entry.resolved:
                continue
            if reason is not None and entry.reason != reason:
                continue
            result = self.replay(entry.id)
            if result is not None:
                count += 1
            else:
                # Still mark as replayed attempt
                count += 1
        return count

    def discard_all(self, reason: Optional[DLQReason] = None) -> int:
        """Discard all unresolved DLQ entries.

        Args:
            reason: Only discard entries with this reason (None = all)

        Returns:
            Number of entries discarded
        """
        count = 0
        for entry in self._entries:
            if entry.resolved:
                continue
            if reason is not None and entry.reason != reason:
                continue
            entry.resolved = True
            entry.resolved_at = datetime.now(timezone.utc)
            entry.resolution = "discarded"
            count += 1

        if count > 0:
            self._save_to_store()
        return count
