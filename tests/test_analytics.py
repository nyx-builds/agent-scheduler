"""Tests for the analytics module."""

import pytest
from datetime import datetime, timedelta, timezone

from agent_scheduler.models import Job, JobExecution, JobStatus, ExecutionStatus, Priority
from agent_scheduler.analytics import (
    AnalyticsEngine,
    DurationStats,
    FailurePattern,
    JobHealthReport,
    SchedulerAnalytics,
    _percentile,
    _grade_from_score,
    compute_duration_stats,
    compute_job_health,
    analyze_failures,
)


# ── Fixtures ────────────────────────────────────────────────


def make_execution(
    status: ExecutionStatus = ExecutionStatus.SUCCESS,
    duration: float | None = 1.0,
    error: str | None = None,
    started_at: datetime | None = None,
    job_name: str = "test-job",
    job_id: str = "job-1",
) -> JobExecution:
    """Create a JobExecution for testing."""
    return JobExecution(
        job_id=job_id,
        job_name=job_name,
        status=status,
        duration_seconds=duration,
        error_message=error,
        started_at=started_at or datetime.now(timezone.utc),
    )


def make_job(
    name: str = "test-job",
    status: JobStatus = JobStatus.SCHEDULED,
    last_run_at: datetime | None = None,
    last_error: str | None = None,
    enabled: bool = True,
) -> Job:
    """Create a minimal Job-like object for testing."""
    return Job(
        name=name,
        handler="test.handler",
        cron="0 9 * * *",
        priority=Priority.NORMAL,
        status=status,
        last_run_at=last_run_at,
        last_error=last_error,
        enabled=enabled,
    )


# ── DurationStats ──────────────────────────────────────────


class TestDurationStats:
    def test_avg_ms_property(self):
        ds = DurationStats(count=3, avg_seconds=2.5)
        assert ds.avg_ms == 2500.0

    def test_avg_ms_none(self):
        ds = DurationStats(count=0)
        assert ds.avg_ms is None


# ── _percentile ─────────────────────────────────────────────


class TestPercentile:
    def test_empty(self):
        assert _percentile([], 50) is None

    def test_single(self):
        assert _percentile([5.0], 50) == 5.0

    def test_median_odd(self):
        vals = [1.0, 2.0, 3.0, 4.0, 5.0]
        assert _percentile(vals, 50) == 3.0

    def test_p95(self):
        vals = [float(i) for i in range(1, 101)]  # 1..100
        p95 = _percentile(vals, 95)
        assert p95 is not None
        assert 94 <= p95 <= 96

    def test_p0(self):
        vals = [10.0, 20.0, 30.0]
        assert _percentile(vals, 0) == 10.0

    def test_p100(self):
        vals = [10.0, 20.0, 30.0]
        assert _percentile(vals, 100) == 30.0


# ── _grade_from_score ──────────────────────────────────────


class TestGradeFromScore:
    @pytest.mark.parametrize("score,expected", [
        (100, "A"),
        (90, "A"),
        (89.9, "B"),
        (80, "B"),
        (79.9, "C"),
        (70, "C"),
        (69.9, "D"),
        (60, "D"),
        (59.9, "F"),
        (0, "F"),
    ])
    def test_grades(self, score, expected):
        assert _grade_from_score(score) == expected


# ── compute_duration_stats ──────────────────────────────────


class TestComputeDurationStats:
    def test_empty(self):
        result = compute_duration_stats([])
        assert result.count == 0
        assert result.min_seconds is None

    def test_single(self):
        execs = [make_execution(duration=5.0)]
        result = compute_duration_stats(execs)
        assert result.count == 1
        assert result.min_seconds == 5.0
        assert result.max_seconds == 5.0
        assert result.avg_seconds == 5.0
        assert result.median_seconds == 5.0

    def test_multiple(self):
        execs = [make_execution(duration=d) for d in [1.0, 2.0, 3.0, 4.0, 5.0]]
        result = compute_duration_stats(execs)
        assert result.count == 5
        assert result.min_seconds == 1.0
        assert result.max_seconds == 5.0
        assert result.avg_seconds == 3.0
        assert result.median_seconds == 3.0

    def test_skips_none_durations(self):
        execs = [make_execution(duration=None), make_execution(duration=5.0)]
        result = compute_duration_stats(execs)
        assert result.count == 1


# ── analyze_failures ───────────────────────────────────────


class TestAnalyzeFailures:
    def test_no_failures(self):
        execs = [make_execution(status=ExecutionStatus.SUCCESS)]
        result = analyze_failures(execs)
        assert result == []

    def test_single_failure(self):
        execs = [make_execution(status=ExecutionStatus.FAILED, error="Connection refused")]
        result = analyze_failures(execs)
        assert len(result) == 1
        assert result[0].count == 1
        assert "Connection refused" in result[0].error

    def test_groups_similar_errors(self):
        execs = [
            make_execution(status=ExecutionStatus.FAILED, error="Timeout after 30s"),
            make_execution(status=ExecutionStatus.FAILED, error="Timeout after 30s"),
            make_execution(status=ExecutionStatus.FAILED, error="DNS error"),
        ]
        result = analyze_failures(execs)
        assert len(result) == 2
        # Most common first
        assert result[0].count == 2

    def test_tracks_affected_jobs(self):
        execs = [
            make_execution(status=ExecutionStatus.FAILED, error="OOM", job_name="job-a"),
            make_execution(status=ExecutionStatus.FAILED, error="OOM", job_name="job-b"),
        ]
        result = analyze_failures(execs)
        assert "job-a" in result[0].affected_jobs
        assert "job-b" in result[0].affected_jobs

    def test_top_n_limit(self):
        execs = [
            make_execution(status=ExecutionStatus.FAILED, error=f"Error {i}")
            for i in range(20)
        ]
        result = analyze_failures(execs, top_n=5)
        assert len(result) == 5

    def test_skips_no_error_message(self):
        execs = [make_execution(status=ExecutionStatus.FAILED, error=None)]
        result = analyze_failures(execs)
        assert result == []

    def test_timeout_failures(self):
        execs = [make_execution(status=ExecutionStatus.TIMEOUT, error="Job exceeded timeout")]
        result = analyze_failures(execs)
        assert len(result) == 1


# ── compute_job_health ─────────────────────────────────────


class TestComputeJobHealth:
    def test_no_executions(self):
        job = make_job()
        report = compute_job_health(job, [])
        assert report.total_executions == 0
        assert report.success_rate == 0.0
        # Stale because never ran
        assert report.is_stale is True

    def test_all_success(self):
        now = datetime.now(timezone.utc)
        job = make_job(last_run_at=now - timedelta(minutes=30))
        execs = [make_execution(status=ExecutionStatus.SUCCESS, started_at=now - timedelta(minutes=i + 1)) for i in range(10)]
        report = compute_job_health(job, execs, now=now)
        assert report.success_rate == 100.0
        assert report.failed_executions == 0
        assert report.health_score >= 80
        assert report.is_stale is False

    def test_all_failures(self):
        job = make_job()
        execs = [make_execution(status=ExecutionStatus.FAILED, error="error") for _ in range(10)]
        report = compute_job_health(job, execs)
        assert report.success_rate == 0.0
        assert report.failed_executions == 10
        assert report.health_score < 30

    def test_mixed(self):
        now = datetime.now(timezone.utc)
        job = make_job(last_run_at=now - timedelta(hours=2))
        execs = [
            make_execution(status=ExecutionStatus.SUCCESS, started_at=now - timedelta(hours=5)),
            make_execution(status=ExecutionStatus.SUCCESS, started_at=now - timedelta(hours=4)),
            make_execution(status=ExecutionStatus.FAILED, error="x", started_at=now - timedelta(hours=3)),
            make_execution(status=ExecutionStatus.SUCCESS, started_at=now - timedelta(hours=2)),
            make_execution(status=ExecutionStatus.SUCCESS, started_at=now - timedelta(hours=1)),
        ]
        report = compute_job_health(job, execs, now=now)
        assert report.total_executions == 5
        assert report.successful_executions == 4
        assert report.failed_executions == 1
        assert 50 < report.success_rate < 100

    def test_stale_job(self):
        now = datetime.now(timezone.utc)
        job = make_job(last_run_at=now - timedelta(hours=48))
        report = compute_job_health(job, [], now=now)
        assert report.is_stale is True

    def test_recent_stale(self):
        now = datetime.now(timezone.utc)
        job = make_job(last_run_at=now - timedelta(hours=1))
        report = compute_job_health(job, [], now=now)
        assert report.is_stale is False

    def test_completed_job_not_stale(self):
        now = datetime.now(timezone.utc)
        job = make_job(status=JobStatus.COMPLETED, last_run_at=now - timedelta(hours=48))
        report = compute_job_health(job, [], now=now)
        assert report.is_stale is False

    def test_disabled_job_not_stale(self):
        now = datetime.now(timezone.utc)
        job = make_job(enabled=False, last_run_at=None)
        report = compute_job_health(job, [], now=now)
        assert report.is_stale is False

    def test_last_5_statuses(self):
        now = datetime.now(timezone.utc)
        execs = [
            make_execution(status=ExecutionStatus.SUCCESS, started_at=now - timedelta(minutes=10)),
            make_execution(status=ExecutionStatus.FAILED, error="x", started_at=now - timedelta(minutes=9)),
            make_execution(status=ExecutionStatus.SUCCESS, started_at=now - timedelta(minutes=8)),
            make_execution(status=ExecutionStatus.SUCCESS, started_at=now - timedelta(minutes=7)),
            make_execution(status=ExecutionStatus.SUCCESS, started_at=now - timedelta(minutes=6)),
        ]
        job = make_job(last_run_at=now - timedelta(minutes=6))
        report = compute_job_health(job, execs, now=now)
        assert len(report.last_5_statuses) == 5
        assert report.last_5_statuses[-1] == "success"

    def test_avg_duration(self):
        execs = [
            make_execution(duration=2.0),
            make_execution(duration=4.0),
        ]
        job = make_job()
        report = compute_job_health(job, execs)
        assert report.avg_duration_seconds == 3.0


# ── AnalyticsEngine ────────────────────────────────────────


class MockStore:
    """Mock store for AnalyticsEngine tests."""

    def __init__(self, jobs=None, executions=None):
        self._jobs = jobs or []
        self._executions = executions or []

    def list_jobs(self):
        return self._jobs

    def get_executions(self, job_id, limit=1000):
        return [e for e in self._executions if e.job_id == job_id][:limit]

    def get_all_executions(self, limit=10000):
        return self._executions[:limit]


class TestAnalyticsEngine:
    def test_empty_dashboard(self):
        engine = AnalyticsEngine(store=MockStore())
        dashboard = engine.dashboard()
        assert dashboard.total_executions == 0
        assert dashboard.overall_success_rate == 0.0
        assert dashboard.healthiest_jobs == []

    def test_dashboard_with_data(self):
        now = datetime.now(timezone.utc)
        job1 = make_job(name="healthy-job", last_run_at=now - timedelta(minutes=30))
        job1.id = "job-1"
        job2 = make_job(name="unhealthy-job", last_run_at=now - timedelta(minutes=30))
        job2.id = "job-2"

        execs = [
            make_execution(status=ExecutionStatus.SUCCESS, job_name="healthy-job", job_id="job-1", started_at=now - timedelta(minutes=30)),
            make_execution(status=ExecutionStatus.SUCCESS, job_name="healthy-job", job_id="job-1", started_at=now - timedelta(minutes=60)),
            make_execution(status=ExecutionStatus.FAILED, error="boom", job_name="unhealthy-job", job_id="job-2", started_at=now - timedelta(minutes=30)),
            make_execution(status=ExecutionStatus.FAILED, error="boom", job_name="unhealthy-job", job_id="job-2", started_at=now - timedelta(minutes=60)),
        ]

        store = MockStore(jobs=[job1, job2], executions=execs)
        engine = AnalyticsEngine(store=store)
        dashboard = engine.dashboard()

        assert dashboard.total_executions == 4
        assert dashboard.successful_executions == 2
        assert dashboard.failed_executions == 2
        assert dashboard.overall_success_rate == 50.0
        assert len(dashboard.top_failures) >= 1

    def test_job_report(self):
        job = make_job(name="test", last_run_at=datetime.now(timezone.utc) - timedelta(minutes=10))
        job.id = "job-1"
        execs = [make_execution(status=ExecutionStatus.SUCCESS, job_id="job-1")]
        store = MockStore(jobs=[job], executions=execs)
        engine = AnalyticsEngine(store=store)
        report = engine.job_report(job)
        assert report.job_name == "test"
        assert report.total_executions == 1

    def test_all_reports(self):
        job1 = make_job(name="job-1")
        job1.id = "j1"
        job2 = make_job(name="job-2")
        job2.id = "j2"
        store = MockStore(jobs=[job1, job2], executions=[])
        engine = AnalyticsEngine(store=store)
        reports = engine.all_reports()
        assert len(reports) == 2

    def test_at_risk_jobs(self):
        job = make_job(name="bad-job")
        job.id = "job-x"
        execs = [
            make_execution(status=ExecutionStatus.FAILED, error="e", job_name="bad-job", job_id="job-x")
            for _ in range(5)
        ]
        store = MockStore(jobs=[job], executions=execs)
        engine = AnalyticsEngine(store=store)
        dashboard = engine.dashboard()
        assert "bad-job" in dashboard.at_risk_jobs

    def test_period_counts(self):
        now = datetime.now(timezone.utc)
        job = make_job(name="p-job")
        job.id = "jp"
        execs = [
            make_execution(status=ExecutionStatus.SUCCESS, started_at=now - timedelta(hours=2), job_id="jp", job_name="p-job"),
            make_execution(status=ExecutionStatus.SUCCESS, started_at=now - timedelta(days=3), job_id="jp", job_name="p-job"),
        ]
        store = MockStore(jobs=[job], executions=execs)
        engine = AnalyticsEngine(store=store)
        dashboard = engine.dashboard()
        assert dashboard.last_24h["total"] == 1
        assert dashboard.last_7d["total"] == 2

    def test_summary_property(self):
        engine = AnalyticsEngine(store=MockStore())
        dashboard = engine.dashboard()
        summary = dashboard.summary
        assert "Health:" in summary
        assert "Success:" in summary
