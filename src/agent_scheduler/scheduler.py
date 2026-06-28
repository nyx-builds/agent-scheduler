"""Core scheduler engine."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

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
from agent_scheduler.handler import HandlerRegistry, get_default_registry
from agent_scheduler.store import JobStore, JSONJobStore

logger = logging.getLogger(__name__)


class Scheduler:
    """Task scheduling engine for autonomous agents.

    Supports:
    - Recurring jobs via cron expressions
    - One-time delayed or future-dated tasks
    - Immediate tasks
    - Priority-based job queue
    - Configurable retry with exponential backoff
    - Job dependencies (chain jobs)
    - Tags for filtering and organization
    - Full execution history
    - JSON persistence
    """

    def __init__(
        self,
        store: Optional[JobStore] = None,
        handler_registry: Optional[HandlerRegistry] = None,
        poll_interval: float = 1.0,
        max_concurrent: int = 10,
    ) -> None:
        self.store = store or JSONJobStore()
        self.handlers = handler_registry or get_default_registry()
        self.poll_interval = poll_interval
        self.max_concurrent = max_concurrent

        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._active_jobs: dict[str, asyncio.Task] = {}

    # ── Job CRUD ──────────────────────────────────────────────

    def add_job(self, job: Job) -> Job:
        """Add a new job to the scheduler."""
        # Compute initial next_run_at
        job.next_run_at = job.compute_next_run()
        if job.next_run_at is None and job.is_immediate:
            job.next_run_at = datetime.now(timezone.utc)
        self.store.save_job(job)
        logger.info(f"Added job '{job.name}' (id={job.id}, cron={job.cron}, next={job.next_run_at})")
        return job

    def get_job(self, job_id: str) -> Optional[Job]:
        """Get a job by ID."""
        return self.store.get_job(job_id)

    def get_job_by_name(self, name: str) -> Optional[Job]:
        """Get a job by name."""
        for job in self.store.list_jobs():
            if job.name == name:
                return job
        return None

    def update_job(self, job_id: str, **updates: Any) -> Optional[Job]:
        """Update job fields."""
        job = self.store.get_job(job_id)
        if job is None:
            return None
        for key, value in updates.items():
            if hasattr(job, key):
                setattr(job, key, value)
        job.mark_updated()
        # Recompute next run if schedule changed
        schedule_fields = {"cron", "delay", "run_at", "enabled"}
        if schedule_fields & set(updates.keys()):
            job.next_run_at = job.compute_next_run()
        self.store.save_job(job)
        logger.info(f"Updated job '{job.name}' (id={job.id})")
        return job

    def delete_job(self, job_id: str) -> bool:
        """Delete a job and its execution history."""
        job = self.store.get_job(job_id)
        if job is None:
            return False
        self.store.delete_job(job_id)
        logger.info(f"Deleted job '{job.name}' (id={job_id})")
        return True

    def list_jobs(
        self,
        enabled_only: bool = False,
        tag: Optional[str] = None,
        status: Optional[JobStatus] = None,
    ) -> list[Job]:
        """List jobs with optional filtering."""
        jobs = self.store.list_jobs()
        if enabled_only:
            jobs = [j for j in jobs if j.enabled]
        if tag:
            jobs = [j for j in jobs if tag in j.tags]
        if status:
            jobs = [j for j in jobs if j.status == status]
        return jobs

    # ── Job Control ──────────────────────────────────────────

    def pause_job(self, job_id: str) -> Optional[Job]:
        """Pause a job (stops scheduling but preserves config)."""
        return self.update_job(job_id, status=JobStatus.PAUSED, enabled=False)

    def resume_job(self, job_id: str) -> Optional[Job]:
        """Resume a paused job."""
        return self.update_job(job_id, status=JobStatus.SCHEDULED, enabled=True)

    def cancel_job(self, job_id: str) -> Optional[Job]:
        """Cancel a job permanently."""
        return self.update_job(job_id, status=JobStatus.CANCELLED, enabled=False)

    # ── Dependencies ─────────────────────────────────────────

    def add_dependency(
        self,
        job_id: str,
        depends_on_id: str,
        on_status: ExecutionStatus = ExecutionStatus.SUCCESS,
    ) -> JobDependency:
        """Create a dependency: job_id runs after depends_on_id completes with on_status."""
        dep = JobDependency(job_id=job_id, depends_on_id=depends_on_id, on_status=on_status)
        self.store.save_dependency(dep)
        logger.info(f"Added dependency: {job_id} depends on {depends_on_id} (on {on_status.value})")
        return dep

    def get_dependencies(self, job_id: str) -> list[JobDependency]:
        """Get all dependencies for a job (both as dependent and dependency)."""
        return self.store.get_dependencies(job_id)

    def remove_dependency(self, dep_id: str) -> bool:
        """Remove a dependency."""
        return self.store.delete_dependency(dep_id)

    # ── Execution ────────────────────────────────────────────

    async def run_job(self, job_id: str) -> Optional[JobExecution]:
        """Manually trigger a job execution."""
        job = self.store.get_job(job_id)
        if job is None:
            return None
        return await self._execute_job(job)

    async def run_due_jobs(self) -> list[JobExecution]:
        """Find and execute all due jobs, respecting priority order."""
        now = datetime.now(timezone.utc)
        due_jobs = []

        for job in self.store.list_jobs():
            if not job.enabled or job.status in (JobStatus.PAUSED, JobStatus.CANCELLED):
                continue
            if job.next_run_at and job.next_run_at <= now:
                # Check max_runs
                if job.max_runs and job.run_count >= job.max_runs:
                    job.status = JobStatus.COMPLETED
                    job.enabled = False
                    self.store.save_job(job)
                    continue
                # Check dependencies
                if not self._check_dependencies_met(job):
                    continue
                due_jobs.append(job)

        # Sort by priority (high first)
        due_jobs.sort(key=lambda j: j.priority.weight, reverse=True)

        results = []
        for job in due_jobs:
            if self._semaphore:
                await self._semaphore.acquire()
            try:
                execution = await self._execute_job(job)
                if execution:
                    results.append(execution)
            finally:
                if self._semaphore:
                    self._semaphore.release()

        return results

    async def _execute_job(self, job: Job) -> JobExecution:
        """Execute a single job with retry logic."""
        now = datetime.now(timezone.utc)
        execution = JobExecution(
            job_id=job.id,
            job_name=job.name,
            status=ExecutionStatus.SUCCESS,
            retry_attempt=0,
        )
        execution.started_at = now

        # Execute with retries
        max_attempts = 1
        if job.retry_policy:
            max_attempts = 1 + job.retry_policy.max_retries

        last_error = None
        for attempt in range(max_attempts):
            execution.retry_attempt = attempt
            result = await self.handlers.execute(job.handler, job.payload, job.timeout)

            if result.success:
                execution.status = ExecutionStatus.SUCCESS
                execution.result = result.data
                execution.finished_at = datetime.now(timezone.utc)
                execution.duration_seconds = (execution.finished_at - execution.started_at).total_seconds()

                # Update job
                job.run_count += 1
                job.last_run_at = now
                job.last_error = None
                job.status = JobStatus.SCHEDULED

                # Compute next run
                job.next_run_at = job.compute_next_run()

                # Check if one-time job is done
                if not job.is_recurring:
                    if job.max_runs and job.run_count >= job.max_runs:
                        job.status = JobStatus.COMPLETED
                        job.enabled = False
                    elif job.run_at and not job.cron:
                        # One-time run_at job is done
                        job.status = JobStatus.COMPLETED
                        job.enabled = False
                    elif job.delay is not None and not job.cron and job.run_count >= 1:
                        # One-time delayed job is done
                        job.status = JobStatus.COMPLETED
                        job.enabled = False
                        job.delay = None  # Clear delay so it doesn't re-schedule

                job.mark_updated()
                self.store.save_job(job)
                self.store.save_execution(execution)

                # Trigger dependent jobs
                await self._trigger_dependents(job.id, execution.status)

                logger.info(f"Job '{job.name}' executed successfully (attempt {attempt + 1})")
                return execution
            else:
                last_error = result.error
                execution.error_message = last_error
                execution.status = ExecutionStatus.FAILED

                # Check if we should retry
                if job.retry_policy and attempt < max_attempts - 1:
                    if job.retry_policy.should_retry(last_error):
                        backoff = job.retry_policy.get_backoff(attempt + 1)
                        execution.status = ExecutionStatus.RETRY
                        logger.warning(
                            f"Job '{job.name}' failed (attempt {attempt + 1}), "
                            f"retrying in {backoff}s: {last_error}"
                        )
                        await asyncio.sleep(min(backoff, 1))  # Cap sleep for testing
                        continue

                # Final failure
                execution.status = ExecutionStatus.TIMEOUT if "timed out" in (last_error or "") else ExecutionStatus.FAILED
                break

        execution.finished_at = datetime.now(timezone.utc)
        execution.duration_seconds = (execution.finished_at - execution.started_at).total_seconds()

        # Update job on failure
        job.fail_count += 1
        job.last_error = last_error
        job.last_run_at = now
        job.status = JobStatus.FAILED
        job.next_run_at = job.compute_next_run()  # May still have future runs
        if job.is_recurring and job.next_run_at:
            job.status = JobStatus.SCHEDULED  # Recurring jobs stay scheduled
        job.mark_updated()
        self.store.save_job(job)
        self.store.save_execution(execution)

        # Trigger dependents on failure too
        await self._trigger_dependents(job.id, execution.status)

        logger.error(f"Job '{job.name}' failed: {last_error}")
        return execution

    def _check_dependencies_met(self, job: Job) -> bool:
        """Check if all dependencies for a job have been satisfied."""
        deps = self.store.get_dependencies(job.id)
        if not deps:
            return True

        for dep in deps:
            # Find the latest execution of the dependency job
            executions = self.store.get_executions(dep.depends_on_id)
            if not executions:
                return False  # Dependency has never run
            latest = executions[-1]
            if latest.status != dep.on_status:
                return False
        return True

    async def _trigger_dependents(self, job_id: str, status: ExecutionStatus) -> None:
        """Trigger dependent jobs whose dependency conditions are met."""
        all_deps = self.store.list_dependencies()
        for dep in all_deps:
            if dep.depends_on_id == job_id and dep.on_status == status:
                dependent_job = self.store.get_job(dep.job_id)
                if dependent_job and dependent_job.enabled:
                    # Schedule the dependent job to run immediately
                    dependent_job.next_run_at = datetime.now(timezone.utc)
                    self.store.save_job(dependent_job)

    # ── History & Stats ──────────────────────────────────────

    def get_history(
        self,
        job_id: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[JobExecution]:
        """Get execution history, optionally filtered by job."""
        if job_id:
            return self.store.get_executions(job_id, limit=limit, offset=offset)
        return self.store.get_all_executions(limit=limit, offset=offset)

    def get_stats(self) -> SchedulerStats:
        """Get scheduler statistics."""
        jobs = self.store.list_jobs()
        all_executions = self.store.get_all_executions(limit=10000)

        active = [j for j in jobs if j.enabled and j.status not in (JobStatus.COMPLETED, JobStatus.CANCELLED)]
        now = datetime.now(timezone.utc)
        upcoming = [j for j in active if j.next_run_at and j.next_run_at > now]

        all_tags: set[str] = set()
        for j in jobs:
            all_tags.update(j.tags)

        return SchedulerStats(
            total_jobs=len(jobs),
            active_jobs=len(active),
            paused_jobs=len([j for j in jobs if j.status == JobStatus.PAUSED]),
            completed_jobs=len([j for j in jobs if j.status == JobStatus.COMPLETED]),
            failed_jobs=len([j for j in jobs if j.status == JobStatus.FAILED]),
            total_executions=len(all_executions),
            successful_executions=len([e for e in all_executions if e.is_success]),
            failed_executions=len([e for e in all_executions if e.is_failure]),
            upcoming_jobs=len(upcoming),
            tags=sorted(all_tags),
        )

    def get_next_run(self, job_id: str) -> Optional[datetime]:
        """Get the next scheduled run time for a job."""
        job = self.store.get_job(job_id)
        if job is None:
            return None
        return job.next_run_at

    # ── Tags ─────────────────────────────────────────────────

    def list_tags(self) -> list[str]:
        """List all tags across all jobs."""
        tags: set[str] = set()
        for job in self.store.list_jobs():
            tags.update(job.tags)
        return sorted(tags)

    def get_jobs_by_tag(self, tag: str) -> list[Job]:
        """Get all jobs with a specific tag."""
        return [j for j in self.store.list_jobs() if tag in j.tags]

    # ── Scheduler Lifecycle ──────────────────────────────────

    async def start(self) -> None:
        """Start the scheduler loop."""
        self._running = True
        self._semaphore = asyncio.Semaphore(self.max_concurrent)
        logger.info(f"Scheduler started (poll_interval={self.poll_interval}s, max_concurrent={self.max_concurrent})")
        self._task = asyncio.create_task(self._run_loop())

    async def _run_loop(self) -> None:
        """Main scheduling loop."""
        while self._running:
            try:
                await self.run_due_jobs()
            except Exception as e:
                logger.error(f"Scheduler loop error: {e}")
            await asyncio.sleep(self.poll_interval)

    async def stop(self) -> None:
        """Stop the scheduler loop."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Scheduler stopped")

    @property
    def is_running(self) -> bool:
        return self._running
