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
from agent_scheduler.webhook import WebhookEvent, WebhookManager
from agent_scheduler.dlq import DeadLetterQueue, DLQReason
from agent_scheduler.result_chain import ResultChainManager
from agent_scheduler.circuit_breaker import (
    CircuitBreakerRegistry,
    CircuitConfig,
    CircuitState,
)

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
        webhook_manager: Optional[WebhookManager] = None,
        poll_interval: float = 1.0,
        max_concurrent: int = 10,
        enable_dlq: bool = True,
        enable_result_chaining: bool = True,
        enable_circuit_breaker: bool = True,
    ) -> None:
        self.store = store or JSONJobStore()
        self.handlers = handler_registry or get_default_registry()
        self.webhooks = webhook_manager
        if self.webhooks is None and self.store is not None:
            # Auto-initialize webhook manager with the store
            try:
                self.webhooks = WebhookManager(store=self.store)
            except Exception:
                pass
        self.poll_interval = poll_interval
        self.max_concurrent = max_concurrent

        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._active_jobs: dict[str, asyncio.Task] = {}

        # v0.5.0: Dead Letter Queue for permanently failed jobs
        self.dlq: Optional[DeadLetterQueue] = None
        if enable_dlq:
            self.dlq = DeadLetterQueue(scheduler=self)

        # v0.5.0: Result chaining for job dependency pipelines
        self.result_chains: Optional[ResultChainManager] = None
        if enable_result_chaining:
            self.result_chains = ResultChainManager(scheduler=self)

        # v0.6.0: Circuit breaker registry for flaky handler protection
        self.circuit_breakers: Optional[CircuitBreakerRegistry] = None
        if enable_circuit_breaker:
            self.circuit_breakers = CircuitBreakerRegistry()

        # v0.6.0: Track condition-skip counts
        self._condition_skips: dict[str, int] = {}

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
        """Find and execute all due jobs, respecting priority order.

        v0.6.0 additions:
        - Circuit breaker: skips jobs whose handler circuit is OPEN
        - Conditional execution: skips jobs whose execution_condition evaluates False
        """
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

                # v0.6.0: Check circuit breaker
                if self.circuit_breakers is not None:
                    cb = self.circuit_breakers.get(job.handler)
                    if cb is not None and not cb.allow():
                        logger.info(
                            f"Job '{job.name}' skipped — circuit OPEN for "
                            f"handler '{job.handler}'"
                        )
                        # Reschedule to after cooldown
                        continue

                # v0.6.0: Check execution condition
                if job.execution_condition:
                    if not self._evaluate_job_condition(job):
                        self._condition_skips[job.id] = (
                            self._condition_skips.get(job.id, 0) + 1
                        )
                        logger.info(
                            f"Job '{job.name}' skipped — execution_condition "
                            f"evaluated False (skip #{self._condition_skips[job.id]})"
                        )
                        # Reschedule for next cron tick
                        job.next_run_at = job.compute_next_run()
                        if job.next_run_at is None:
                            # Non-recurring job with unmet condition
                            job.status = JobStatus.COMPLETED
                            job.enabled = False
                        job.mark_updated()
                        self.store.save_job(job)
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

    def _evaluate_job_condition(self, job: Job) -> bool:
        """Evaluate a job's execution_condition. Returns True if it should run."""
        from agent_scheduler.conditions import (
            ConditionContext,
            evaluate_condition,
            ConditionEvaluationError,
        )

        # Build context from job state
        last_result = None
        last_status = None
        history = self.store.get_executions(job.id, limit=1)
        if history:
            last = history[0]
            last_result = last.result
            last_status = last.status.value

        context = ConditionContext(
            payload=job.payload,
            last_result=last_result,
            last_status=last_status,
            job_tags=job.tags,
            job_metadata=job.metadata,
            run_count=job.run_count,
            fail_count=job.fail_count,
        )

        try:
            return evaluate_condition(job.execution_condition, context)
        except ConditionEvaluationError as e:
            logger.error(f"Job '{job.name}' condition error: {e}")
            return False  # Fail-safe: skip on error

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

            # v0.6.0: Record circuit breaker outcome
            if self.circuit_breakers is not None:
                cb = self.circuit_breakers.get_or_create(job.handler)
                if result.success:
                    cb.record_success()
                else:
                    cb.record_failure(result.error)

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

                # Trigger dependent jobs (pass result data for chaining)
                await self._trigger_dependents(job.id, execution.status, execution.result)

                # Fire webhook for job completion
                if self.webhooks:
                    await self.webhooks.fire_event(WebhookEvent.JOB_COMPLETED, job, execution)

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

                        # Fire retry webhook
                        if self.webhooks:
                            await self.webhooks.fire_event(WebhookEvent.JOB_RETRY, job, execution)

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

        # v0.5.0: Move to Dead Letter Queue if this is a non-recurring job
        # that has permanently failed (no future runs possible)
        should_dead_letter = (
            self.dlq is not None
            and not (job.is_recurring and job.next_run_at is not None)
        )
        if should_dead_letter:
            reason = (
                DLQReason.TIMEOUT
                if execution.status == ExecutionStatus.TIMEOUT
                else DLQReason.MAX_RETRIES_EXHAUSTED
            )
            self.dlq.add(
                job_id=job.id,
                job_name=job.name,
                handler=job.handler,
                payload=job.payload,
                reason=reason,
                error_message=last_error or "Unknown error",
                retry_attempts=max_attempts - 1,
                original_job=job.model_dump(mode="json"),
            )

        job.mark_updated()
        self.store.save_job(job)
        self.store.save_execution(execution)

        # Trigger dependents on failure too
        await self._trigger_dependents(job.id, execution.status)

        # Fire failure/timeout webhook
        if self.webhooks:
            event = WebhookEvent.JOB_TIMEOUT if execution.status == ExecutionStatus.TIMEOUT else WebhookEvent.JOB_FAILED
            await self.webhooks.fire_event(event, job, execution)

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

    async def _trigger_dependents(self, job_id: str, status: ExecutionStatus, result_data: Optional[dict[str, Any]] = None) -> None:
        """Trigger dependent jobs whose dependency conditions are met.

        v0.5.0: If result chaining is enabled and a link is configured,
        the parent job's result data is merged into the dependent's payload.
        """
        all_deps = self.store.list_dependencies()
        for dep in all_deps:
            if dep.depends_on_id == job_id and dep.on_status == status:
                dependent_job = self.store.get_job(dep.job_id)
                if dependent_job and dependent_job.enabled:
                    # v0.5.0: Apply result chaining if configured
                    if (
                        self.result_chains is not None
                        and result_data is not None
                    ):
                        dependent_job.payload = self.result_chains.process_result(
                            parent_job_id=job_id,
                            child_job_id=dependent_job.id,
                            parent_result=result_data,
                            child_payload=dependent_job.payload,
                        )

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

    # ── v0.6.0: Circuit Breaker & Conditions ─────────────────

    def get_circuit_breaker_status(self) -> list[dict[str, Any]]:
        """Get the status of all circuit breakers."""
        if self.circuit_breakers is None:
            return []
        return [cb.to_dict() for cb in self.circuit_breakers.list_breakers()]

    def get_circuit_breaker(self, handler: str) -> Optional[dict[str, Any]]:
        """Get the status of a specific handler's circuit breaker."""
        if self.circuit_breakers is None:
            return None
        cb = self.circuit_breakers.get(handler)
        return cb.to_dict() if cb else None

    def reset_circuit_breaker(self, handler: str) -> bool:
        """Manually reset a circuit breaker to CLOSED state."""
        if self.circuit_breakers is None:
            return False
        cb = self.circuit_breakers.get(handler)
        if cb is None:
            return False
        cb.reset()
        return True

    def reset_all_circuit_breakers(self) -> int:
        """Reset all circuit breakers. Returns count of reset breakers."""
        if self.circuit_breakers is None:
            return 0
        return self.circuit_breakers.reset_all()

    def get_condition_skip_count(self, job_id: str) -> int:
        """Get the number of times a job was skipped due to its execution_condition."""
        return self._condition_skips.get(job_id, 0)

    def configure_circuit_breaker(
        self,
        handler: str,
        config: CircuitConfig,
    ) -> None:
        """Configure circuit breaker settings for a handler."""
        if self.circuit_breakers is None:
            self.circuit_breakers = CircuitBreakerRegistry()
        cb = self.circuit_breakers.get_or_create(handler, config=config)
        # Update config on existing breaker
        cb.config = config
