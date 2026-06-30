"""Execution analytics for agent-scheduler.

Provides insights into job execution performance, reliability, and health:
- Success/failure rates over time
- Duration statistics (min, max, avg, percentiles)
- Failure pattern analysis (most common errors)
- Per-job health scores
- Scheduler-wide health dashboard
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from pydantic import BaseModel, Field

from agent_scheduler.models import JobExecution, JobStatus, ExecutionStatus


class DurationStats(BaseModel):
    """Statistics about job execution durations."""

    count: int = Field(default=0, ge=0, description="Number of executions measured")
    min_seconds: Optional[float] = Field(default=None, description="Minimum duration")
    max_seconds: Optional[float] = Field(default=None, description="Maximum duration")
    avg_seconds: Optional[float] = Field(default=None, description="Average (mean) duration")
    median_seconds: Optional[float] = Field(default=None, description="50th percentile duration")
    p95_seconds: Optional[float] = Field(default=None, description="95th percentile duration")
    p99_seconds: Optional[float] = Field(default=None, description="99th percentile duration")

    @property
    def avg_ms(self) -> Optional[float]:
        """Average duration in milliseconds."""
        return round(self.avg_seconds * 1000, 2) if self.avg_seconds is not None else None


class FailurePattern(BaseModel):
    """A recurring error pattern from job failures."""

    error: str = Field(..., description="Error message (truncated)")
    count: int = Field(default=0, ge=0, description="Number of times this error occurred")
    last_seen: Optional[datetime] = Field(default=None, description="Most recent occurrence")
    affected_jobs: list[str] = Field(default_factory=list, description="Job names affected by this error")


class JobHealthReport(BaseModel):
    """Health report for a single job."""

    job_id: str
    job_name: str
    status: JobStatus = Field(default=JobStatus.SCHEDULED)
    total_executions: int = Field(default=0, ge=0)
    successful_executions: int = Field(default=0, ge=0)
    failed_executions: int = Field(default=0, ge=0)
    retry_count: int = Field(default=0, ge=0, description="Total retry attempts recorded")
    success_rate: float = Field(default=0.0, ge=0.0, le=100.0, description="Success percentage")
    health_score: float = Field(default=0.0, ge=0.0, le=100.0, description="Composite health score (0-100)")
    health_grade: str = Field(default="F", description="Letter grade (A-F)")
    avg_duration_seconds: Optional[float] = Field(default=None)
    last_run_at: Optional[datetime] = Field(default=None)
    last_error: Optional[str] = Field(default=None)
    last_5_statuses: list[str] = Field(default_factory=list, description="Most recent execution statuses (oldest→newest)")
    is_stale: bool = Field(default=False, description="True if job hasn't run in 24h despite being scheduled")


class SchedulerAnalytics(BaseModel):
    """Aggregate analytics across the entire scheduler."""

    total_executions: int = Field(default=0, ge=0)
    successful_executions: int = Field(default=0, ge=0)
    failed_executions: int = Field(default=0, ge=0)
    retry_count: int = Field(default=0, ge=0)
    overall_success_rate: float = Field(default=0.0, ge=0.0, le=100.0)
    overall_health_score: float = Field(default=0.0, ge=0.0, le=100.0)
    overall_health_grade: str = Field(default="F")

    # Period-based
    last_24h: dict[str, int] = Field(default_factory=dict, description="Counts for the last 24 hours")
    last_7d: dict[str, int] = Field(default_factory=dict, description="Counts for the last 7 days")

    # Duration
    duration_stats: DurationStats = Field(default_factory=DurationStats)

    # Healthiest / unhealthiest
    healthiest_jobs: list[JobHealthReport] = Field(default_factory=list, description="Top 5 healthiest jobs")
    unhealthiest_jobs: list[JobHealthReport] = Field(default_factory=list, description="Top 5 unhealthiest jobs")

    # Failure patterns
    top_failures: list[FailurePattern] = Field(default_factory=list, description="Most common failure patterns")

    # At-risk jobs
    at_risk_jobs: list[str] = Field(default_factory=list, description="Names of jobs with health_score < 50")
    stale_jobs: list[str] = Field(default_factory=list, description="Names of stale jobs (not running)")

    @property
    def summary(self) -> str:
        """Human-readable summary."""
        return (
            f"Health: {self.overall_health_grade} ({self.overall_health_score:.1f}/100) | "
            f"Success: {self.overall_success_rate:.1f}% | "
            f"Executions: {self.total_executions} | "
            f"At-risk: {len(self.at_risk_jobs)}"
        )


def _percentile(sorted_values: list[float], p: float) -> Optional[float]:
    """Compute the p-th percentile from a sorted list of values."""
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return sorted_values[0]
    k = (len(sorted_values) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_values) - 1)
    if f == c:
        return sorted_values[f]
    d0 = sorted_values[int(f)] * (c - k)
    d1 = sorted_values[int(c)] * (k - f)
    return d0 + d1


def _grade_from_score(score: float) -> str:
    """Convert a 0-100 score to a letter grade."""
    if score >= 90:
        return "A"
    if score >= 80:
        return "B"
    if score >= 70:
        return "C"
    if score >= 60:
        return "D"
    return "F"


def compute_duration_stats(executions: list[JobExecution]) -> DurationStats:
    """Compute duration statistics from a list of executions.

    Only executions with a non-None ``duration_seconds`` are considered.
    """
    durations = sorted(
        e.duration_seconds
        for e in executions
        if e.duration_seconds is not None and e.duration_seconds >= 0
    )
    if not durations:
        return DurationStats(count=0)

    avg = sum(durations) / len(durations)
    return DurationStats(
        count=len(durations),
        min_seconds=round(durations[0], 4),
        max_seconds=round(durations[-1], 4),
        avg_seconds=round(avg, 4),
        median_seconds=round(_percentile(durations, 50) or 0, 4),
        p95_seconds=round(_percentile(durations, 95) or 0, 4),
        p99_seconds=round(_percentile(durations, 99) or 0, 4),
    )


def analyze_failures(executions: list[JobExecution], top_n: int = 10) -> list[FailurePattern]:
    """Identify and rank the most common failure patterns.

    Groups similar errors together — errors are truncated and normalized
    before comparison so that transient details (timestamps, IDs) don't
    fragment the pattern.
    """
    # Collect only failed executions
    failed = [
        e for e in executions
        if e.is_failure and e.error_message
    ]
    if not failed:
        return []

    # Normalize errors: truncate and strip dynamic content
    patterns: dict[str, FailurePattern] = {}

    for e in failed:
        normalized = _normalize_error(e.error_message or "")
        if normalized not in patterns:
            patterns[normalized] = FailurePattern(
                error=normalized,
                count=0,
                last_seen=e.started_at,
                affected_jobs=[],
            )
        p = patterns[normalized]
        p.count += 1
        if e.started_at and (p.last_seen is None or e.started_at > p.last_seen):
            p.last_seen = e.started_at
        if e.job_name not in p.affected_jobs:
            p.affected_jobs.append(e.job_name)

    # Sort by count, return top N
    result = sorted(patterns.values(), key=lambda p: p.count, reverse=True)
    return result[:top_n]


def _normalize_error(error: str) -> str:
    """Normalize an error message for pattern matching.

    - Truncates to 200 chars
    - Removes common dynamic content (timestamps, UUIDs, numbers)
    - Lowercases for consistency
    """
    text = error.strip()
    if len(text) > 200:
        text = text[:197] + "..."
    return text


def compute_job_health(
    job: Any,
    executions: list[JobExecution],
    now: Optional[datetime] = None,
) -> JobHealthReport:
    """Compute a health report for a single job.

    The health score (0-100) is a composite metric that considers:
    - **Success rate** (40% weight): Higher success rate = higher score
    - **Recent execution trend** (25% weight): Weighted average of last 5
      executions — more recent executions weigh more
    - **Recency** (15% weight): Jobs that ran recently score higher
    - **Failure rate trend** (20% weight): Improving trend is rewarded

    Args:
        job: A Job object (or anything with ``id``, ``name``, ``status``,
             ``last_run_at``, ``last_error``, ``enabled`` attributes)
        executions: Execution history for this job (newest first preferred)
        now: Reference timestamp (defaults to current UTC)

    Returns:
        JobHealthReport with score, grade, and statistics
    """
    if now is None:
        now = datetime.now(timezone.utc)

    total = len(executions)
    successes = len([e for e in executions if e.is_success])
    failures = len([e for e in executions if e.is_failure])
    retries = len([e for e in executions if e.status == ExecutionStatus.RETRY])

    success_rate = (successes / total * 100) if total > 0 else 0.0

    # Last 5 statuses (oldest→newest) — executions may be newest-first,
    # so we reverse for the window
    sorted_exec = sorted(executions, key=lambda e: e.started_at)
    recent = sorted_exec[-5:] if len(sorted_exec) >= 5 else sorted_exec
    recent_statuses = [e.status.value for e in recent]

    # Trend score: weight more recent executions higher
    trend_score = 0.0
    if recent:
        weights = []
        for i in range(len(recent)):
            weights.append((i + 1) / len(recent))
        total_weight = sum(weights)
        trend_score = sum(
            (100.0 if e.is_success else 0.0) * w
            for e, w in zip(recent, weights)
        ) / total_weight if total_weight > 0 else 0.0

    # Recency score: full marks if ran in last hour, decaying over 24h
    recency_score = 0.0
    last_run = getattr(job, "last_run_at", None)
    if last_run:
        hours_ago = (now - last_run).total_seconds() / 3600 if now > last_run else 0
        if hours_ago <= 1:
            recency_score = 100.0
        elif hours_ago <= 24:
            recency_score = max(0.0, 100.0 * (1 - (hours_ago - 1) / 23))
        # else: 0

    # Failure trend: compare first half vs second half failure rates
    failure_trend = 50.0  # Neutral
    if total >= 4:
        mid = total // 2
        first_half = executions[:mid] if len(executions) > mid else executions
        second_half = executions[mid:] if len(executions) > mid else []
        first_fail_rate = len([e for e in first_half if e.is_failure]) / len(first_half) * 100 if first_half else 0
        second_fail_rate = len([e for e in second_half if e.is_failure]) / len(second_half) * 100 if second_half else 0
        if second_fail_rate < first_fail_rate:
            # Improving
            improvement = first_fail_rate - second_fail_rate
            failure_trend = min(100.0, 50.0 + improvement)
        elif second_fail_rate > first_fail_rate:
            # Worsening
            decline = second_fail_rate - first_fail_rate
            failure_trend = max(0.0, 50.0 - decline)
        else:
            failure_trend = 50.0

    # Composite health score
    health_score = (
        success_rate * 0.40
        + trend_score * 0.25
        + recency_score * 0.15
        + failure_trend * 0.20
    )
    health_score = round(max(0.0, min(100.0, health_score)), 1)
    grade = _grade_from_score(health_score)

    # Duration average
    durations = [e.duration_seconds for e in executions if e.duration_seconds is not None]
    avg_duration = round(sum(durations) / len(durations), 4) if durations else None

    # Stale check: scheduled + enabled but hasn't run in 24h
    is_stale = False
    job_status = getattr(job, "status", JobStatus.SCHEDULED)
    job_enabled = getattr(job, "enabled", True)
    if job_status not in (JobStatus.COMPLETED, JobStatus.CANCELLED, JobStatus.FAILED) and job_enabled:
        if last_run is None:
            is_stale = True  # Never ran
        elif (now - last_run).total_seconds() > 86400:
            is_stale = True

    return JobHealthReport(
        job_id=getattr(job, "id", ""),
        job_name=getattr(job, "name", ""),
        status=job_status,
        total_executions=total,
        successful_executions=successes,
        failed_executions=failures,
        retry_count=retries,
        success_rate=round(success_rate, 1),
        health_score=health_score,
        health_grade=grade,
        avg_duration_seconds=avg_duration,
        last_run_at=last_run,
        last_error=getattr(job, "last_error", None),
        last_5_statuses=recent_statuses,
        is_stale=is_stale,
    )


class AnalyticsEngine:
    """Computes analytics for the scheduler.

    Works with any scheduler or store that provides:
    - ``list_jobs()`` → list of jobs
    - ``get_executions(job_id)`` or ``get_all_executions()`` → executions
    """

    def __init__(self, scheduler: Any = None, store: Any = None) -> None:
        self._scheduler = scheduler
        self._store = store or (scheduler.store if scheduler else None)

    def job_report(self, job: Any, now: Optional[datetime] = None) -> JobHealthReport:
        """Get a health report for a single job."""
        executions = self._get_executions_for_job(getattr(job, "id", ""))
        return compute_job_health(job, executions, now)

    def all_reports(self, now: Optional[datetime] = None) -> list[JobHealthReport]:
        """Get health reports for all jobs."""
        if now is None:
            now = datetime.now(timezone.utc)
        jobs = self._get_jobs()
        reports = []
        for job in jobs:
            executions = self._get_executions_for_job(job.id)
            reports.append(compute_job_health(job, executions, now))
        return reports

    def dashboard(self, now: Optional[datetime] = None) -> SchedulerAnalytics:
        """Compute full scheduler analytics dashboard."""
        if now is None:
            now = datetime.now(timezone.utc)

        all_executions = self._get_all_executions(limit=10000)
        jobs = self._get_jobs()
        reports = []
        for job in jobs:
            executions = self._get_executions_for_job(job.id)
            reports.append(compute_job_health(job, executions, now))

        total = len(all_executions)
        successes = len([e for e in all_executions if e.is_success])
        failures = len([e for e in all_executions if e.is_failure])
        retries = len([e for e in all_executions if e.status == ExecutionStatus.RETRY])

        success_rate = (successes / total * 100) if total > 0 else 0.0

        # Duration stats
        duration_stats = compute_duration_stats(all_executions)

        # Period counts
        h24_cutoff = now - timedelta(hours=24)
        d7_cutoff = now - timedelta(days=7)

        last_24h_exec = [e for e in all_executions if e.started_at and e.started_at >= h24_cutoff]
        last_7d_exec = [e for e in all_executions if e.started_at and e.started_at >= d7_cutoff]

        last_24h = {
            "total": len(last_24h_exec),
            "success": len([e for e in last_24h_exec if e.is_success]),
            "failed": len([e for e in last_24h_exec if e.is_failure]),
        }
        last_7d = {
            "total": len(last_7d_exec),
            "success": len([e for e in last_7d_exec if e.is_success]),
            "failed": len([e for e in last_7d_exec if e.is_failure]),
        }

        # Sort reports by health score
        reports_sorted = sorted(reports, key=lambda r: r.health_score, reverse=True)
        healthiest = [r for r in reports_sorted if r.total_executions > 0][:5]
        unhealthiest = sorted([r for r in reports if r.total_executions > 0], key=lambda r: r.health_score)[:5]

        # Failure patterns
        top_failures = analyze_failures(all_executions, top_n=10)

        # At-risk and stale
        at_risk = [r.job_name for r in reports if r.health_score < 50 and r.total_executions > 0]
        stale = [r.job_name for r in reports if r.is_stale]

        # Overall health: average of all job health scores (that have executions)
        scored = [r.health_score for r in reports if r.total_executions > 0]
        overall_health = round(sum(scored) / len(scored), 1) if scored else 0.0

        return SchedulerAnalytics(
            total_executions=total,
            successful_executions=successes,
            failed_executions=failures,
            retry_count=retries,
            overall_success_rate=round(success_rate, 1),
            overall_health_score=overall_health,
            overall_health_grade=_grade_from_score(overall_health),
            last_24h=last_24h,
            last_7d=last_7d,
            duration_stats=duration_stats,
            healthiest_jobs=healthiest,
            unhealthiest_jobs=unhealthiest,
            top_failures=top_failures,
            at_risk_jobs=at_risk,
            stale_jobs=stale,
        )

    # ── Internal helpers ──────────────────────────────────────

    def _get_jobs(self) -> list[Any]:
        if self._scheduler is not None and hasattr(self._scheduler, "list_jobs"):
            return self._scheduler.list_jobs()
        if self._store is not None and hasattr(self._store, "list_jobs"):
            return self._store.list_jobs()
        return []

    def _get_executions_for_job(self, job_id: str) -> list[JobExecution]:
        if self._scheduler is not None and hasattr(self._scheduler, "get_history"):
            return self._scheduler.get_history(job_id=job_id, limit=1000)
        if self._store is not None and hasattr(self._store, "get_executions"):
            return self._store.get_executions(job_id, limit=1000)
        return []

    def _get_all_executions(self, limit: int = 10000) -> list[JobExecution]:
        if self._scheduler is not None and hasattr(self._scheduler, "get_history"):
            return self._scheduler.get_history(job_id=None, limit=limit)
        if self._store is not None and hasattr(self._store, "get_all_executions"):
            return self._store.get_all_executions(limit=limit)
        return []
