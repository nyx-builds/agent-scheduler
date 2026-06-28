"""Persistence layer for agent-scheduler."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from agent_scheduler.models import Job, JobDependency, JobExecution
from agent_scheduler.webhook import Webhook, WebhookDelivery
from agent_scheduler.templates import JobTemplate


class JobStore:
    """Abstract base class for job storage."""

    def save_job(self, job: Job) -> None:
        raise NotImplementedError

    def get_job(self, job_id: str) -> Optional[Job]:
        raise NotImplementedError

    def delete_job(self, job_id: str) -> bool:
        raise NotImplementedError

    def list_jobs(self) -> list[Job]:
        raise NotImplementedError

    def save_execution(self, execution: JobExecution) -> None:
        raise NotImplementedError

    def get_executions(self, job_id: str, limit: int = 50, offset: int = 0) -> list[JobExecution]:
        raise NotImplementedError

    def get_all_executions(self, limit: int = 100, offset: int = 0) -> list[JobExecution]:
        raise NotImplementedError

    def save_dependency(self, dep: JobDependency) -> None:
        raise NotImplementedError

    def get_dependencies(self, job_id: str) -> list[JobDependency]:
        raise NotImplementedError

    def list_dependencies(self) -> list[JobDependency]:
        raise NotImplementedError

    def delete_dependency(self, dep_id: str) -> bool:
        raise NotImplementedError

    def save_webhook(self, webhook: Webhook) -> None:
        raise NotImplementedError

    def get_webhook(self, webhook_id: str) -> Optional[Webhook]:
        raise NotImplementedError

    def delete_webhook(self, webhook_id: str) -> bool:
        raise NotImplementedError

    def list_webhooks(self) -> list[Webhook]:
        raise NotImplementedError

    def save_webhook_delivery(self, delivery: WebhookDelivery) -> None:
        raise NotImplementedError

    def get_webhook_delivery(self, delivery_id: str) -> Optional[WebhookDelivery]:
        raise NotImplementedError

    def get_webhook_deliveries(self, webhook_id: Optional[str] = None, limit: int = 50, offset: int = 0) -> list[WebhookDelivery]:
        raise NotImplementedError

    def save_template(self, template: JobTemplate) -> None:
        raise NotImplementedError

    def get_template(self, template_id: str) -> Optional[JobTemplate]:
        raise NotImplementedError

    def delete_template(self, template_id: str) -> bool:
        raise NotImplementedError

    def list_templates(self) -> list[JobTemplate]:
        raise NotImplementedError


class JSONJobStore(JobStore):
    """JSON file-based job storage."""

    def __init__(self, data_dir: Optional[str] = None) -> None:
        if data_dir is None:
            data_dir = os.environ.get("SCHEDULER_DATA_DIR", str(Path.home() / ".agent-scheduler"))
        self.data_dir = Path(data_dir)
        self.jobs_file = self.data_dir / "jobs.json"
        self.executions_file = self.data_dir / "executions.json"
        self.dependencies_file = self.data_dir / "dependencies.json"
        self.webhooks_file = self.data_dir / "webhooks.json"
        self.webhook_deliveries_file = self.data_dir / "webhook_deliveries.json"
        self.templates_file = self.data_dir / "templates.json"
        self._ensure_data_dir()

    def _ensure_data_dir(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        for path in [
            self.jobs_file, self.executions_file, self.dependencies_file,
            self.webhooks_file, self.webhook_deliveries_file, self.templates_file,
        ]:
            if not path.exists():
                path.write_text("[]")

    def _read_json(self, path: Path) -> list[dict]:
        try:
            return json.loads(path.read_text())
        except (json.JSONDecodeError, FileNotFoundError):
            return []

    def _write_json(self, path: Path, data: list[dict]) -> None:
        path.write_text(json.dumps(data, indent=2, default=str))

    # ── Jobs ─────────────────────────────────────────────────

    def save_job(self, job: Job) -> None:
        jobs = self._read_json(self.jobs_file)
        # Update or insert
        found = False
        for i, j in enumerate(jobs):
            if j.get("id") == job.id:
                jobs[i] = job.model_dump(mode="json")
                found = True
                break
        if not found:
            jobs.append(job.model_dump(mode="json"))
        self._write_json(self.jobs_file, jobs)

    def get_job(self, job_id: str) -> Optional[Job]:
        jobs = self._read_json(self.jobs_file)
        for j in jobs:
            if j.get("id") == job_id:
                return Job.model_validate(j)
        return None

    def delete_job(self, job_id: str) -> bool:
        jobs = self._read_json(self.jobs_file)
        new_jobs = [j for j in jobs if j.get("id") != job_id]
        if len(new_jobs) == len(jobs):
            return False
        self._write_json(self.jobs_file, new_jobs)
        # Also delete related executions
        executions = self._read_json(self.executions_file)
        executions = [e for e in executions if e.get("job_id") != job_id]
        self._write_json(self.executions_file, executions)
        # And dependencies
        deps = self._read_json(self.dependencies_file)
        deps = [d for d in deps if d.get("job_id") != job_id and d.get("depends_on_id") != job_id]
        self._write_json(self.dependencies_file, deps)
        return True

    def list_jobs(self) -> list[Job]:
        jobs = self._read_json(self.jobs_file)
        return [Job.model_validate(j) for j in jobs]

    # ── Executions ───────────────────────────────────────────

    def save_execution(self, execution: JobExecution) -> None:
        executions = self._read_json(self.executions_file)
        executions.append(execution.model_dump(mode="json"))
        self._write_json(self.executions_file, executions)

    def get_executions(self, job_id: str, limit: int = 50, offset: int = 0) -> list[JobExecution]:
        executions = self._read_json(self.executions_file)
        filtered = [e for e in executions if e.get("job_id") == job_id]
        # Sort by started_at descending
        filtered.sort(key=lambda e: e.get("started_at", ""), reverse=True)
        return [JobExecution.model_validate(e) for e in filtered[offset : offset + limit]]

    def get_all_executions(self, limit: int = 100, offset: int = 0) -> list[JobExecution]:
        executions = self._read_json(self.executions_file)
        executions.sort(key=lambda e: e.get("started_at", ""), reverse=True)
        return [JobExecution.model_validate(e) for e in executions[offset : offset + limit]]

    # ── Dependencies ─────────────────────────────────────────

    def save_dependency(self, dep: JobDependency) -> None:
        deps = self._read_json(self.dependencies_file)
        deps.append(dep.model_dump(mode="json"))
        self._write_json(self.dependencies_file, deps)

    def get_dependencies(self, job_id: str) -> list[JobDependency]:
        deps = self._read_json(self.dependencies_file)
        return [
            JobDependency.model_validate(d)
            for d in deps
            if d.get("job_id") == job_id or d.get("depends_on_id") == job_id
        ]

    def list_dependencies(self) -> list[JobDependency]:
        deps = self._read_json(self.dependencies_file)
        return [JobDependency.model_validate(d) for d in deps]

    def delete_dependency(self, dep_id: str) -> bool:
        deps = self._read_json(self.dependencies_file)
        new_deps = [d for d in deps if d.get("id") != dep_id]
        if len(new_deps) == len(deps):
            return False
        self._write_json(self.dependencies_file, new_deps)
        return True

    # ── Webhooks ─────────────────────────────────────────────

    def save_webhook(self, webhook: Webhook) -> None:
        webhooks = self._read_json(self.webhooks_file)
        found = False
        for i, w in enumerate(webhooks):
            if w.get("id") == webhook.id:
                webhooks[i] = webhook.model_dump(mode="json")
                found = True
                break
        if not found:
            webhooks.append(webhook.model_dump(mode="json"))
        self._write_json(self.webhooks_file, webhooks)

    def get_webhook(self, webhook_id: str) -> Optional[Webhook]:
        webhooks = self._read_json(self.webhooks_file)
        for w in webhooks:
            if w.get("id") == webhook_id:
                return Webhook.model_validate(w)
        return None

    def delete_webhook(self, webhook_id: str) -> bool:
        webhooks = self._read_json(self.webhooks_file)
        new_webhooks = [w for w in webhooks if w.get("id") != webhook_id]
        if len(new_webhooks) == len(webhooks):
            return False
        self._write_json(self.webhooks_file, new_webhooks)
        return True

    def list_webhooks(self) -> list[Webhook]:
        webhooks = self._read_json(self.webhooks_file)
        return [Webhook.model_validate(w) for w in webhooks]

    # ── Webhook Deliveries ───────────────────────────────────

    def save_webhook_delivery(self, delivery: WebhookDelivery) -> None:
        deliveries = self._read_json(self.webhook_deliveries_file)
        deliveries.append(delivery.model_dump(mode="json"))
        self._write_json(self.webhook_deliveries_file, deliveries)

    def get_webhook_delivery(self, delivery_id: str) -> Optional[WebhookDelivery]:
        deliveries = self._read_json(self.webhook_deliveries_file)
        for d in deliveries:
            if d.get("id") == delivery_id:
                return WebhookDelivery.model_validate(d)
        return None

    def get_webhook_deliveries(self, webhook_id: Optional[str] = None, limit: int = 50, offset: int = 0) -> list[WebhookDelivery]:
        deliveries = self._read_json(self.webhook_deliveries_file)
        if webhook_id:
            deliveries = [d for d in deliveries if d.get("webhook_id") == webhook_id]
        deliveries.sort(key=lambda d: d.get("created_at", ""), reverse=True)
        return [WebhookDelivery.model_validate(d) for d in deliveries[offset : offset + limit]]

    # ── Templates ────────────────────────────────────────────

    def save_template(self, template: JobTemplate) -> None:
        templates = self._read_json(self.templates_file)
        found = False
        for i, t in enumerate(templates):
            if t.get("id") == template.id:
                templates[i] = template.model_dump(mode="json")
                found = True
                break
        if not found:
            templates.append(template.model_dump(mode="json"))
        self._write_json(self.templates_file, templates)

    def get_template(self, template_id: str) -> Optional[JobTemplate]:
        templates = self._read_json(self.templates_file)
        for t in templates:
            if t.get("id") == template_id or t.get("name") == template_id:
                return JobTemplate.model_validate(t)
        return None

    def delete_template(self, template_id: str) -> bool:
        templates = self._read_json(self.templates_file)
        new_templates = [t for t in templates if t.get("id") != template_id and t.get("name") != template_id]
        if len(new_templates) == len(templates):
            return False
        self._write_json(self.templates_file, new_templates)
        return True

    def list_templates(self) -> list[JobTemplate]:
        templates = self._read_json(self.templates_file)
        return [JobTemplate.model_validate(t) for t in templates]
