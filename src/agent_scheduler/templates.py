"""Job templates for agent-scheduler.

Reusable job blueprints that make it easy to create
commonly-used job types without repeating configuration.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field

from agent_scheduler.models import Job, Priority, RetryPolicy


class TemplateCategory(str, Enum):
    """Categories for organizing job templates."""

    MONITORING = "monitoring"
    BACKUP = "backup"
    REPORTING = "reporting"
    MAINTENANCE = "maintenance"
    NOTIFICATION = "notification"
    DATA_PIPELINE = "data-pipeline"
    CUSTOM = "custom"


class JobTemplate(BaseModel):
    """A reusable job definition template.

    Templates capture the common configuration for a type of job
    (handler, retry policy, priority, etc.) so that agents can
    quickly instantiate jobs with just the variable parts.
    """

    id: str = Field(default_factory=lambda: uuid.uuid4().hex[:12])
    name: str = Field(..., min_length=1, description="Template name (e.g. 'daily-backup')")
    description: str = Field(default="", description="What this template creates")
    category: TemplateCategory = Field(default=TemplateCategory.CUSTOM, description="Template category")

    # Default job configuration
    handler: str = Field(..., min_length=1, description="Default handler function identifier")
    default_cron: Optional[str] = Field(default=None, description="Default cron expression")
    default_priority: Priority = Field(default=Priority.NORMAL, description="Default job priority")
    default_timeout: float = Field(default=300, ge=0, description="Default timeout in seconds")
    default_max_runs: Optional[int] = Field(default=None, ge=1, description="Default max runs")
    default_tags: list[str] = Field(default_factory=list, description="Default tags")
    default_metadata: dict[str, Any] = Field(default_factory=dict, description="Default metadata")
    default_payload: dict[str, Any] = Field(default_factory=dict, description="Default payload values")

    # Retry policy defaults
    default_max_retries: int = Field(default=0, ge=0, description="Default max retries")
    default_backoff_seconds: float = Field(default=30, ge=0, description="Default backoff base")
    default_backoff_multiplier: float = Field(default=2.0, ge=1.0, description="Default backoff multiplier")
    default_max_backoff: float = Field(default=3600, ge=0, description="Default max backoff cap")
    default_retry_on_errors: Optional[list[str]] = Field(
        default=None, description="Default error patterns to retry on"
    )

    # Variable fields — placeholders that must be provided when instantiating
    required_fields: list[str] = Field(
        default_factory=list,
        description="Field names that must be provided when creating a job from this template",
    )
    optional_fields: list[str] = Field(
        default_factory=list,
        description="Field names that can optionally be overridden",
    )

    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @staticmethod
    def _has_field(field_path: str, data: dict[str, Any]) -> bool:
        """Check if a dotted field path exists in the data dict.

        Supports paths like 'payload.endpoint' which checks
        data.get('payload', {}).get('endpoint').
        """
        parts = field_path.split(".")
        current: Any = data
        for part in parts:
            if isinstance(current, dict):
                if part not in current:
                    return False
                current = current[part]
            else:
                return False
        return True

    def create_job(self, **overrides: Any) -> Job:
        """Create a Job instance from this template with optional overrides.

        Required fields must be provided in overrides.
        Any job field can be overridden.
        """
        # Validate required fields — supports dotted paths like "payload.endpoint"
        missing = []
        for field_path in self.required_fields:
            if not self._has_field(field_path, overrides):
                missing.append(field_path)
        if missing:
            raise ValueError(
                f"Missing required fields for template '{self.name}': {missing}"
            )

        # Build retry policy from template defaults
        retry_policy = None
        max_retries = overrides.pop("max_retries", self.default_max_retries)
        if max_retries > 0:
            retry_policy = RetryPolicy(
                max_retries=max_retries,
                backoff_seconds=overrides.pop("backoff_seconds", self.default_backoff_seconds),
                backoff_multiplier=overrides.pop("backoff_multiplier", self.default_backoff_multiplier),
                max_backoff=overrides.pop("max_backoff", self.default_max_backoff),
                retry_on_errors=overrides.pop("retry_on_errors", self.default_retry_on_errors),
            )

        # Merge default payload with overrides
        payload = dict(self.default_payload)
        if "payload" in overrides:
            payload.update(overrides.pop("payload"))

        # Build job from template defaults + overrides
        job_kwargs: dict[str, Any] = {
            "handler": overrides.pop("handler", self.handler),
            "cron": overrides.pop("cron", self.default_cron),
            "priority": overrides.pop("priority", self.default_priority),
            "timeout": overrides.pop("timeout", self.default_timeout),
            "max_runs": overrides.pop("max_runs", self.default_max_runs),
            "tags": overrides.pop("tags", list(self.default_tags)),
            "metadata": overrides.pop("metadata", dict(self.default_metadata)),
            "payload": payload,
            "retry_policy": retry_policy,
        }

        # Name: use override or generate from template
        job_kwargs["name"] = overrides.pop("name", f"{self.name}-{uuid.uuid4().hex[:6]}")

        # Delay / run_at
        if "delay" in overrides:
            job_kwargs["delay"] = overrides.pop("delay")
        if "run_at" in overrides:
            job_kwargs["run_at"] = overrides.pop("run_at")

        # Any remaining overrides are set directly on the job
        for key, value in overrides.items():
            if hasattr(Job, key) if isinstance(Job, type) else False:
                job_kwargs[key] = value

        return Job(**job_kwargs)


# ── Built-in templates ─────────────────────────────────────────

BUILTIN_TEMPLATES: dict[str, JobTemplate] = {}


def _register_builtin(template: JobTemplate) -> None:
    """Register a built-in template."""
    BUILTIN_TEMPLATES[template.name] = template


# Health check template
_register_builtin(JobTemplate(
    name="health-check",
    description="Periodic health check with automatic retry on failure",
    category=TemplateCategory.MONITORING,
    handler="health.check",
    default_cron="*/5 * * * *",  # Every 5 minutes
    default_priority=Priority.HIGH,
    default_timeout=30,
    default_tags=["monitoring", "health"],
    default_max_retries=2,
    default_backoff_seconds=10,
    default_retry_on_errors=["timeout", "connection"],
    default_payload={"endpoint": "", "expected_status": 200},
    required_fields=["payload.endpoint"],
    optional_fields=["payload.expected_status", "cron"],
))

# Daily backup template
_register_builtin(JobTemplate(
    name="daily-backup",
    description="Daily backup job with exponential backoff retry",
    category=TemplateCategory.BACKUP,
    handler="backup.run",
    default_cron="0 2 * * *",  # 2 AM daily
    default_priority=Priority.NORMAL,
    default_timeout=3600,  # 1 hour
    default_tags=["backup", "daily"],
    default_max_retries=3,
    default_backoff_seconds=60,
    default_backoff_multiplier=2.0,
    default_retry_on_errors=["disk", "permission", "timeout"],
    default_payload={"source": "", "destination": "", "compress": True},
    required_fields=["payload.source", "payload.destination"],
    optional_fields=["payload.compress", "cron", "timeout"],
))

# Weekly report template
_register_builtin(JobTemplate(
    name="weekly-report",
    description="Generate and send a weekly summary report",
    category=TemplateCategory.REPORTING,
    handler="report.generate",
    default_cron="0 9 * * MON",  # Monday 9 AM
    default_priority=Priority.LOW,
    default_timeout=600,
    default_tags=["reporting", "weekly"],
    default_max_retries=1,
    default_payload={"report_type": "", "recipients": [], "format": "pdf"},
    required_fields=["payload.report_type", "payload.recipients"],
    optional_fields=["payload.format", "cron"],
))

# Data pipeline template
_register_builtin(JobTemplate(
    name="data-pipeline",
    description="ETL/data pipeline job with configurable stages",
    category=TemplateCategory.DATA_PIPELINE,
    handler="pipeline.run",
    default_cron="0 */6 * * *",  # Every 6 hours
    default_priority=Priority.NORMAL,
    default_timeout=1800,
    default_tags=["pipeline", "etl"],
    default_max_retries=2,
    default_backoff_seconds=120,
    default_retry_on_errors=["connection", "timeout", "rate_limit"],
    default_payload={"pipeline_id": "", "stages": [], "on_failure": "stop"},
    required_fields=["payload.pipeline_id"],
    optional_fields=["payload.stages", "payload.on_failure", "cron"],
))

# Cleanup template
_register_builtin(JobTemplate(
    name="cleanup",
    description="Periodic cleanup of temporary resources",
    category=TemplateCategory.MAINTENANCE,
    handler="cleanup.run",
    default_cron="0 3 * * *",  # 3 AM daily
    default_priority=Priority.LOW,
    default_timeout=1800,
    default_tags=["maintenance", "cleanup"],
    default_max_retries=1,
    default_payload={"target": "", "older_than_days": 30, "dry_run": False},
    required_fields=["payload.target"],
    optional_fields=["payload.older_than_days", "payload.dry_run", "cron"],
))

# Notification template
_register_builtin(JobTemplate(
    name="notification",
    description="Send a notification (email, slack, webhook) on a schedule",
    category=TemplateCategory.NOTIFICATION,
    handler="notify.send",
    default_priority=Priority.HIGH,
    default_timeout=30,
    default_tags=["notification"],
    default_payload={"channel": "", "message": "", "priority": "normal"},
    required_fields=["payload.channel", "payload.message"],
    optional_fields=["payload.priority", "cron", "delay", "run_at"],
))


class TemplateManager:
    """Manages job templates — CRUD and instantiation."""

    def __init__(self, store: Any = None) -> None:
        self._store = store

    def set_store(self, store: Any) -> None:
        """Set the template store."""
        self._store = store

    # ── Template CRUD ──────────────────────────────────────

    def create_template(self, template: JobTemplate) -> JobTemplate:
        """Create a new template."""
        if self._store is not None:
            self._store.save_template(template)
        return template

    def get_template(self, template_id: str) -> Optional[JobTemplate]:
        """Get a template by ID."""
        # Check built-ins first
        for t in BUILTIN_TEMPLATES.values():
            if t.id == template_id or t.name == template_id:
                return t
        if self._store is not None:
            return self._store.get_template(template_id)
        return None

    def get_template_by_name(self, name: str) -> Optional[JobTemplate]:
        """Get a template by name."""
        if name in BUILTIN_TEMPLATES:
            return BUILTIN_TEMPLATES[name]
        if self._store is not None:
            for template in self._store.list_templates():
                if template.name == name:
                    return template
        return None

    def update_template(self, template_id: str, **updates: Any) -> Optional[JobTemplate]:
        """Update a template."""
        template = self.get_template(template_id)
        if template is None:
            return None
        # Don't modify built-ins — create a copy
        if template.name in BUILTIN_TEMPLATES:
            import copy
            template = copy.deepcopy(template)
            template.id = uuid.uuid4().hex[:12]
        for key, value in updates.items():
            if hasattr(template, key):
                setattr(template, key, value)
        template.updated_at = datetime.now(timezone.utc)
        if self._store is not None:
            self._store.save_template(template)
        return template

    def delete_template(self, template_id: str) -> bool:
        """Delete a custom template (built-ins cannot be deleted)."""
        if template_id in BUILTIN_TEMPLATES:
            return False  # Cannot delete built-ins
        if self._store is not None:
            return self._store.delete_template(template_id)
        return False

    def list_templates(
        self,
        category: Optional[TemplateCategory] = None,
    ) -> list[JobTemplate]:
        """List all templates (built-in + custom), optionally filtered by category."""
        templates = list(BUILTIN_TEMPLATES.values())
        if self._store is not None:
            templates.extend(self._store.list_templates())
        if category:
            templates = [t for t in templates if t.category == category]
        return templates

    # ── Template Instantiation ─────────────────────────────

    def instantiate(
        self,
        template_identifier: str,
        **overrides: Any,
    ) -> Job:
        """Create a Job from a template with the given overrides.

        Args:
            template_identifier: Template ID or name
            **overrides: Field overrides for the job

        Returns:
            A new Job instance

        Raises:
            ValueError: If template not found or required fields missing
        """
        template = self.get_template(template_identifier) or self.get_template_by_name(template_identifier)
        if template is None:
            raise ValueError(f"Template not found: {template_identifier}")
        return template.create_job(**overrides)
