"""Data models for agent-scheduler."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator


class Priority(str, Enum):
    """Job priority levels."""

    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"

    @property
    def weight(self) -> int:
        return {Priority.LOW: 0, Priority.NORMAL: 1, Priority.HIGH: 2}[self]


class JobStatus(str, Enum):
    """Job lifecycle status."""

    PENDING = "pending"
    SCHEDULED = "scheduled"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ExecutionStatus(str, Enum):
    """Status of a single job execution."""

    SUCCESS = "success"
    FAILED = "failed"
    TIMEOUT = "timeout"
    RETRY = "retry"


class RetryPolicy(BaseModel):
    """Configuration for automatic retries on failure."""

    max_retries: int = Field(default=3, ge=0, description="Maximum number of retry attempts")
    backoff_seconds: float = Field(default=30, ge=0, description="Base backoff duration in seconds")
    backoff_multiplier: float = Field(default=2.0, ge=1.0, description="Exponential multiplier for backoff")
    max_backoff: float = Field(default=3600, ge=0, description="Maximum backoff cap in seconds")
    retry_on_errors: Optional[list[str]] = Field(
        default=None,
        description="List of error pattern substrings to retry on (None = retry all)",
    )

    def get_backoff(self, attempt: int) -> float:
        """Calculate backoff duration for a given retry attempt."""
        backoff = self.backoff_seconds * (self.backoff_multiplier ** (attempt - 1))
        return min(backoff, self.max_backoff)

    def should_retry(self, error_message: str) -> bool:
        """Check if an error should trigger a retry."""
        if self.retry_on_errors is None:
            return True
        return any(pattern in error_message for pattern in self.retry_on_errors)


class Job(BaseModel):
    """A scheduled job definition."""

    id: str = Field(default_factory=lambda: uuid.uuid4().hex[:12])
    name: str = Field(..., min_length=1, description="Human-readable job name")
    handler: str = Field(..., min_length=1, description="Handler function identifier")
    payload: dict[str, Any] = Field(default_factory=dict, description="Data passed to handler")

    # Scheduling
    cron: Optional[str] = Field(default=None, description="Cron expression for recurring jobs")
    delay: Optional[float] = Field(default=None, ge=0, description="Seconds until first run")
    run_at: Optional[datetime] = Field(default=None, description="Specific future run time")
    timezone: str = Field(default="UTC", description="Timezone for cron evaluation")

    # Execution control
    priority: Priority = Field(default=Priority.NORMAL, description="Job priority level")
    retry_policy: Optional[RetryPolicy] = Field(default=None, description="Retry configuration")
    timeout: float = Field(default=300, ge=0, description="Run timeout in seconds")
    max_runs: Optional[int] = Field(default=None, ge=1, description="Maximum number of executions")

    # State
    enabled: bool = Field(default=True, description="Whether job is active")
    status: JobStatus = Field(default=JobStatus.SCHEDULED, description="Current job status")
    tags: list[str] = Field(default_factory=list, description="Tags for filtering")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Extra key-value data")

    # Tracking
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    last_run_at: Optional[datetime] = Field(default=None, description="Last execution time")
    next_run_at: Optional[datetime] = Field(default=None, description="Next scheduled run time")
    run_count: int = Field(default=0, ge=0, description="Number of successful runs")
    fail_count: int = Field(default=0, ge=0, description="Number of failed runs")
    last_error: Optional[str] = Field(default=None, description="Last error message")

    @field_validator("cron")
    @classmethod
    def validate_cron(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            try:
                from croniter import croniter

                croniter(v)
            except (ValueError, KeyError) as e:
                raise ValueError(f"Invalid cron expression: {v!r} — {e}")
        return v

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        # Name must be usable as an identifier
        cleaned = v.strip()
        if not cleaned:
            raise ValueError("Job name cannot be blank")
        if len(cleaned) > 128:
            raise ValueError("Job name must be ≤128 characters")
        return cleaned

    @property
    def is_recurring(self) -> bool:
        return self.cron is not None

    @property
    def is_one_time(self) -> bool:
        return self.cron is None and (self.delay is not None or self.run_at is not None)

    @property
    def is_immediate(self) -> bool:
        return self.cron is None and self.delay is None and self.run_at is None

    def compute_next_run(self, now: Optional[datetime] = None) -> Optional[datetime]:
        """Compute the next run time for this job."""
        if not self.enabled or self.status == JobStatus.PAUSED:
            return None

        if now is None:
            now = datetime.now(timezone.utc)

        if self.cron:
            from croniter import croniter

            cron = croniter(self.cron, now)
            return cron.get_next(datetime)

        if self.run_at:
            if self.run_at > now:
                return self.run_at
            return None  # Past due one-time

        if self.delay is not None:
            from datetime import timedelta

            return now + timedelta(seconds=self.delay)

        return now  # Immediate job

    def mark_updated(self) -> None:
        self.updated_at = datetime.now(timezone.utc)


class JobExecution(BaseModel):
    """Record of a single job execution."""

    id: str = Field(default_factory=lambda: uuid.uuid4().hex[:12])
    job_id: str = Field(..., description="ID of the executed job")
    job_name: str = Field(..., description="Name of the executed job")
    status: ExecutionStatus = Field(..., description="Execution result status")
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: Optional[datetime] = Field(default=None)
    duration_seconds: Optional[float] = Field(default=None, ge=0)
    error_message: Optional[str] = Field(default=None)
    retry_attempt: int = Field(default=0, ge=0, description="0 = first attempt, N = retry #N")
    result: Optional[dict[str, Any]] = Field(default=None, description="Handler result data")

    @property
    def is_success(self) -> bool:
        return self.status == ExecutionStatus.SUCCESS

    @property
    def is_failure(self) -> bool:
        return self.status in (ExecutionStatus.FAILED, ExecutionStatus.TIMEOUT)


class JobDependency(BaseModel):
    """A dependency between two jobs — the dependent runs after the dependency completes."""

    id: str = Field(default_factory=lambda: uuid.uuid4().hex[:12])
    job_id: str = Field(..., description="ID of the dependent job (runs after)")
    depends_on_id: str = Field(..., description="ID of the dependency job (must complete first)")
    on_status: ExecutionStatus = Field(
        default=ExecutionStatus.SUCCESS,
        description="Required status of dependency to trigger dependent",
    )
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class SchedulerStats(BaseModel):
    """Scheduler statistics summary."""

    total_jobs: int = 0
    active_jobs: int = 0
    paused_jobs: int = 0
    completed_jobs: int = 0
    failed_jobs: int = 0
    total_executions: int = 0
    successful_executions: int = 0
    failed_executions: int = 0
    upcoming_jobs: int = 0
    tags: list[str] = Field(default_factory=list)
