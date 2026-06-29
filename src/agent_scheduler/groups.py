"""Job groups for multi-tenant / multi-agent scheduling.

Allows organizing jobs into groups (e.g., per agent, per project)
with group-level stats, bulk operations, and isolation.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class GroupQuota(BaseModel):
    """Resource quotas for a job group."""

    max_jobs: Optional[int] = Field(default=None, ge=0, description="Maximum jobs in this group (None = unlimited)")
    max_concurrent: Optional[int] = Field(default=None, ge=1, description="Max concurrent executions (None = use global)")
    max_executions_per_hour: Optional[int] = Field(default=None, ge=0, description="Max executions per hour (None = unlimited)")

    @property
    def is_unlimited(self) -> bool:
        """Check if all quotas are unlimited."""
        return self.max_jobs is None and self.max_concurrent is None and self.max_executions_per_hour is None


class JobGroup(BaseModel):
    """A named group of related jobs (e.g., per agent, per project)."""

    id: str = Field(default_factory=lambda: uuid.uuid4().hex[:12])
    name: str = Field(..., min_length=1, description="Group name")
    description: str = Field(default="", description="Group description")
    tags: list[str] = Field(default_factory=list, description="Tags applied to all jobs in group")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Extra key-value data")
    quota: GroupQuota = Field(default_factory=GroupQuota, description="Resource quotas")
    enabled: bool = Field(default=True, description="Whether the group is active")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def mark_updated(self) -> None:
        self.updated_at = datetime.now(timezone.utc)


class GroupStats(BaseModel):
    """Statistics for a job group."""

    group_id: str
    group_name: str
    total_jobs: int = 0
    active_jobs: int = 0
    paused_jobs: int = 0
    completed_jobs: int = 0
    failed_jobs: int = 0
    total_executions: int = 0
    successful_executions: int = 0
    failed_executions: int = 0
    quota: GroupQuota = Field(default_factory=GroupQuota)
    quota_usage_pct: float = Field(default=0.0, description="Job quota usage percentage")


class GroupManager:
    """Manages job groups — CRUD, quotas, and bulk operations."""

    def __init__(self, store: Any = None, scheduler: Any = None) -> None:
        self._store = store
        self._scheduler = scheduler
        self._groups: dict[str, JobGroup] = {}  # In-memory cache
        self._load_groups()

    def _load_groups(self) -> None:
        """Load groups from store if available."""
        if self._store is not None:
            # Groups stored in a dedicated table/collection
            try:
                import json
                from pathlib import Path
                if hasattr(self._store, 'data_dir'):
                    groups_file = Path(self._store.data_dir) / "groups.json"
                    if groups_file.exists():
                        data = json.loads(groups_file.read_text())
                        for g in data:
                            group = JobGroup.model_validate(g)
                            self._groups[group.id] = group
                            self._groups[group.name] = group  # Also index by name
            except Exception:
                pass

    def _save_groups(self) -> None:
        """Persist groups to store."""
        if self._store is not None:
            try:
                import json
                from pathlib import Path
                if hasattr(self._store, 'data_dir'):
                    groups_file = Path(self._store.data_dir) / "groups.json"
                    # Deduplicate — only save by ID
                    unique = {g.id: g for g in self._groups.values() if isinstance(g, JobGroup)}
                    groups_file.write_text(
                        json.dumps([g.model_dump(mode="json") for g in unique.values()], indent=2, default=str)
                    )
            except Exception:
                pass

    def create_group(
        self,
        name: str,
        description: str = "",
        tags: Optional[list[str]] = None,
        metadata: Optional[dict[str, Any]] = None,
        quota: Optional[GroupQuota] = None,
    ) -> JobGroup:
        """Create a new job group.

        Args:
            name: Group name (must be unique)
            description: Group description
            tags: Tags applied to all jobs in the group
            metadata: Extra key-value data
            quota: Resource quotas

        Returns:
            The created JobGroup

        Raises:
            ValueError: If a group with the same name already exists
        """
        if name in self._groups:
            raise ValueError(f"Group '{name}' already exists")

        group = JobGroup(
            name=name,
            description=description,
            tags=tags or [],
            metadata=metadata or {},
            quota=quota or GroupQuota(),
        )
        self._groups[group.id] = group
        self._groups[group.name] = group
        self._save_groups()
        return group

    def get_group(self, identifier: str) -> Optional[JobGroup]:
        """Get a group by ID or name."""
        return self._groups.get(identifier)

    def update_group(self, identifier: str, **updates: Any) -> Optional[JobGroup]:
        """Update a group's configuration."""
        group = self.get_group(identifier)
        if group is None:
            return None

        for key, value in updates.items():
            if key == "quota" and isinstance(value, dict):
                value = GroupQuota(**value)
            if hasattr(group, key):
                setattr(group, key, value)

        group.mark_updated()
        self._groups[group.id] = group
        self._groups[group.name] = group
        self._save_groups()
        return group

    def delete_group(self, identifier: str) -> bool:
        """Delete a group (does not delete its jobs)."""
        group = self.get_group(identifier)
        if group is None:
            return False

        # Remove from both indexes
        self._groups.pop(group.id, None)
        self._groups.pop(group.name, None)
        self._save_groups()
        return True

    def list_groups(self, enabled_only: bool = False) -> list[JobGroup]:
        """List all groups."""
        seen_ids: set[str] = set()
        groups = []
        for g in self._groups.values():
            if isinstance(g, JobGroup) and g.id not in seen_ids:
                seen_ids.add(g.id)
                if enabled_only and not g.enabled:
                    continue
                groups.append(g)
        return groups

    def check_quota(self, group_id: str) -> bool:
        """Check if a group can accept more jobs based on its quota.

        Returns True if the group is within quota (can add more jobs).
        """
        group = self.get_group(group_id)
        if group is None:
            return False

        if group.quota.max_jobs is None:
            return True  # Unlimited

        # Count current jobs in this group
        current_count = self._count_group_jobs(group_id)
        return current_count < group.quota.max_jobs

    def get_stats(self, identifier: str) -> Optional[GroupStats]:
        """Get statistics for a group."""
        group = self.get_group(identifier)
        if group is None:
            return None

        if self._scheduler is None:
            return GroupStats(
                group_id=group.id,
                group_name=group.name,
                quota=group.quota,
            )

        # Get jobs tagged with group ID
        group_jobs = self._get_group_jobs(group.id)
        active = [j for j in group_jobs if j.enabled and j.status.value not in ("completed", "cancelled")]
        paused = [j for j in group_jobs if j.status.value == "paused"]
        completed = [j for j in group_jobs if j.status.value == "completed"]
        failed = [j for j in group_jobs if j.status.value == "failed"]

        # Count executions
        total_exec = 0
        success_exec = 0
        fail_exec = 0
        for job in group_jobs:
            history = self._scheduler.get_history(job_id=job.id, limit=1000)
            total_exec += len(history)
            success_exec += len([e for e in history if e.is_success])
            fail_exec += len([e for e in history if e.is_failure])

        # Quota usage
        quota_usage = 0.0
        if group.quota.max_jobs and group.quota.max_jobs > 0:
            quota_usage = (len(group_jobs) / group.quota.max_jobs) * 100

        return GroupStats(
            group_id=group.id,
            group_name=group.name,
            total_jobs=len(group_jobs),
            active_jobs=len(active),
            paused_jobs=len(paused),
            completed_jobs=len(completed),
            failed_jobs=len(failed),
            total_executions=total_exec,
            successful_executions=success_exec,
            failed_executions=fail_exec,
            quota=group.quota,
            quota_usage_pct=round(quota_usage, 1),
        )

    def add_job_to_group(self, group_id: str, job: Any) -> bool:
        """Add a job to a group by tagging it with the group ID."""
        group = self.get_group(group_id)
        if group is None:
            return False

        if not self.check_quota(group_id):
            return False

        # Add group tag to the job
        group_tag = f"group:{group.id}"
        if group_tag not in job.tags:
            job.tags.append(group_tag)
        # Also add the group's default tags
        for tag in group.tags:
            if tag not in job.tags:
                job.tags.append(tag)

        return True

    def _get_group_jobs(self, group_id: str) -> list[Any]:
        """Get all jobs belonging to a group."""
        if self._scheduler is None:
            return []
        group_tag = f"group:{group_id}"
        return self._scheduler.get_jobs_by_tag(group_tag)

    def _count_group_jobs(self, group_id: str) -> int:
        """Count jobs in a group."""
        return len(self._get_group_jobs(group_id))

    def pause_group(self, identifier: str) -> int:
        """Pause all jobs in a group. Returns number of jobs paused."""
        group = self.get_group(identifier)
        if group is None:
            return 0

        jobs = self._get_group_jobs(group.id)
        count = 0
        for job in jobs:
            if job.enabled:
                self._scheduler.pause_job(job.id)
                count += 1
        return count

    def resume_group(self, identifier: str) -> int:
        """Resume all paused jobs in a group. Returns number of jobs resumed."""
        group = self.get_group(identifier)
        if group is None:
            return 0

        jobs = self._get_group_jobs(group.id)
        count = 0
        for job in jobs:
            if not job.enabled and job.status.value == "paused":
                self._scheduler.resume_job(job.id)
                count += 1
        return count

    def delete_group_jobs(self, identifier: str) -> int:
        """Delete all jobs in a group. Returns number of jobs deleted."""
        group = self.get_group(identifier)
        if group is None:
            return 0

        jobs = self._get_group_jobs(group.id)
        count = 0
        for job in jobs:
            self._scheduler.delete_job(job.id)
            count += 1
        return count
