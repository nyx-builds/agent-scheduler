"""MCP (Model Context Protocol) server for agent-scheduler."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Optional

from agent_scheduler.models import (
    ExecutionStatus,
    Job,
    JobDependency,
    JobStatus,
    Priority,
    RetryPolicy,
)
from agent_scheduler.scheduler import Scheduler
from agent_scheduler.store import JSONJobStore
from agent_scheduler.webhook import Webhook, WebhookEvent, WebhookManager
from agent_scheduler.templates import JobTemplate, TemplateCategory, TemplateManager
from agent_scheduler.groups import GroupManager


def _job_to_dict(job: Job) -> dict[str, Any]:
    """Convert a Job to a serializable dict for MCP responses."""
    d = job.model_dump(mode="json")
    d["is_recurring"] = job.is_recurring
    d["is_one_time"] = job.is_one_time
    d["is_immediate"] = job.is_immediate
    return d


def _execution_to_dict(exec_record: Any) -> dict[str, Any]:
    return exec_record.model_dump(mode="json")


# ── MCP Tool Definitions ────────────────────────────────────

TOOLS = [
    {
        "name": "scheduler_create_job",
        "description": "Create a new scheduled job. Supports cron (recurring), delay (one-time after N seconds), run_at (specific time), or immediate execution.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Human-readable job name"},
                "handler": {"type": "string", "description": "Handler function identifier"},
                "payload": {"type": "object", "description": "Data passed to handler", "default": {}},
                "cron": {"type": "string", "description": "Cron expression for recurring jobs (e.g. '0 9 * * MON-FRI')"},
                "delay": {"type": "number", "description": "Seconds until first run (one-time)"},
                "run_at": {"type": "string", "description": "ISO datetime for future run"},
                "priority": {"type": "string", "enum": ["low", "normal", "high"], "default": "normal"},
                "tags": {"type": "array", "items": {"type": "string"}, "description": "Tags for organizing jobs"},
                "max_retries": {"type": "integer", "description": "Max retry attempts on failure", "default": 0},
                "timeout": {"type": "number", "description": "Run timeout in seconds", "default": 300},
                "max_runs": {"type": "integer", "description": "Maximum number of executions"},
            },
            "required": ["name", "handler"],
        },
    },
    {
        "name": "scheduler_list_jobs",
        "description": "List all scheduled jobs with optional filtering by tag or status.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tag": {"type": "string", "description": "Filter by tag"},
                "status": {"type": "string", "enum": ["scheduled", "paused", "completed", "failed", "cancelled"]},
                "enabled_only": {"type": "boolean", "description": "Show only enabled jobs", "default": False},
            },
        },
    },
    {
        "name": "scheduler_get_job",
        "description": "Get detailed information about a specific job by ID or name.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "job_identifier": {"type": "string", "description": "Job ID or name"},
            },
            "required": ["job_identifier"],
        },
    },
    {
        "name": "scheduler_update_job",
        "description": "Update a job's configuration (cron, priority, tags, payload, etc.).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "job_identifier": {"type": "string", "description": "Job ID or name"},
                "cron": {"type": "string", "description": "New cron expression"},
                "priority": {"type": "string", "enum": ["low", "normal", "high"]},
                "tags": {"type": "array", "items": {"type": "string"}},
                "payload": {"type": "object"},
                "timeout": {"type": "number"},
                "max_runs": {"type": "integer"},
                "enabled": {"type": "boolean"},
            },
            "required": ["job_identifier"],
        },
    },
    {
        "name": "scheduler_delete_job",
        "description": "Delete a scheduled job and its execution history.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "job_identifier": {"type": "string", "description": "Job ID or name"},
            },
            "required": ["job_identifier"],
        },
    },
    {
        "name": "scheduler_pause_job",
        "description": "Pause a job (stops scheduling but preserves configuration).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "job_identifier": {"type": "string", "description": "Job ID or name"},
            },
            "required": ["job_identifier"],
        },
    },
    {
        "name": "scheduler_resume_job",
        "description": "Resume a paused job.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "job_identifier": {"type": "string", "description": "Job ID or name"},
            },
            "required": ["job_identifier"],
        },
    },
    {
        "name": "scheduler_run_job",
        "description": "Manually trigger a job execution immediately.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "job_identifier": {"type": "string", "description": "Job ID or name"},
            },
            "required": ["job_identifier"],
        },
    },
    {
        "name": "scheduler_get_history",
        "description": "Get execution history for a specific job or all jobs.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "job_identifier": {"type": "string", "description": "Job ID or name (omit for all jobs)"},
                "limit": {"type": "integer", "description": "Number of records", "default": 50},
            },
        },
    },
    {
        "name": "scheduler_get_next_run",
        "description": "Get the next scheduled run time for a job.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "job_identifier": {"type": "string", "description": "Job ID or name"},
            },
            "required": ["job_identifier"],
        },
    },
    {
        "name": "scheduler_get_stats",
        "description": "Get scheduler statistics (job counts, execution counts, tags).",
        "inputSchema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "scheduler_list_tags",
        "description": "List all tags across all jobs.",
        "inputSchema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "scheduler_get_jobs_by_tag",
        "description": "Get all jobs with a specific tag.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tag": {"type": "string", "description": "Tag to filter by"},
            },
            "required": ["tag"],
        },
    },
    {
        "name": "scheduler_create_dependency",
        "description": "Create a job dependency — the dependent job runs after the dependency job completes with the specified status.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "job_identifier": {"type": "string", "description": "ID or name of the dependent job (runs after)"},
                "depends_on_identifier": {"type": "string", "description": "ID or name of the dependency job (must complete first)"},
                "on_status": {"type": "string", "enum": ["success", "failed", "timeout"], "default": "success"},
            },
            "required": ["job_identifier", "depends_on_identifier"],
        },
    },
    {
        "name": "scheduler_get_dependencies",
        "description": "Get all dependencies for a job.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "job_identifier": {"type": "string", "description": "Job ID or name"},
            },
            "required": ["job_identifier"],
        },
    },
    # ── Webhook Tools ───────────────────────────────────────
    {
        "name": "scheduler_create_webhook",
        "description": "Create a webhook subscription to receive HTTP callbacks on job events (completion, failure, retry, etc.).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Webhook name"},
                "url": {"type": "string", "description": "HTTP(S) URL to POST event payloads to"},
                "secret": {"type": "string", "description": "HMAC-SHA256 signing secret for payload verification"},
                "events": {"type": "array", "items": {"type": "string"}, "description": "Events to subscribe to (e.g. ['job.completed', 'job.failed'])"},
                "tags": {"type": "array", "items": {"type": "string"}, "description": "Only fire for jobs with these tags (empty = all)"},
                "max_retries": {"type": "integer", "description": "Max delivery retry attempts", "default": 3},
                "timeout": {"type": "number", "description": "HTTP request timeout in seconds", "default": 10},
            },
            "required": ["name", "url"],
        },
    },
    {
        "name": "scheduler_list_webhooks",
        "description": "List all webhook subscriptions.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "enabled_only": {"type": "boolean", "description": "Show only enabled webhooks", "default": False},
            },
        },
    },
    {
        "name": "scheduler_delete_webhook",
        "description": "Delete a webhook subscription.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "webhook_id": {"type": "string", "description": "Webhook ID"},
            },
            "required": ["webhook_id"],
        },
    },
    {
        "name": "scheduler_get_webhook_deliveries",
        "description": "Get webhook delivery history (success/failure records).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "webhook_id": {"type": "string", "description": "Filter by webhook ID"},
                "limit": {"type": "integer", "description": "Number of records", "default": 50},
            },
        },
    },
    # ── Template Tools ──────────────────────────────────────
    {
        "name": "scheduler_list_templates",
        "description": "List available job templates (built-in and custom).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "category": {"type": "string", "description": "Filter by category", "enum": ["monitoring", "backup", "reporting", "maintenance", "notification", "data-pipeline", "custom"]},
            },
        },
    },
    {
        "name": "scheduler_get_template",
        "description": "Get detailed information about a job template.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "template_name": {"type": "string", "description": "Template name"},
            },
            "required": ["template_name"],
        },
    },
    {
        "name": "scheduler_instantiate_template",
        "description": "Create a job from a template with optional overrides.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "template_name": {"type": "string", "description": "Template name to use"},
                "name": {"type": "string", "description": "Job name (default: auto-generated)"},
                "cron": {"type": "string", "description": "Override cron expression"},
                "priority": {"type": "string", "enum": ["low", "normal", "high"]},
                "payload": {"type": "object", "description": "Override/additional payload values"},
                "tags": {"type": "array", "items": {"type": "string"}, "description": "Additional tags"},
            },
            "required": ["template_name"],
        },
    },
    {
        "name": "scheduler_create_template",
        "description": "Create a custom job template for reusing job configurations.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Template name"},
                "handler": {"type": "string", "description": "Default handler function"},
                "description": {"type": "string", "description": "Template description"},
                "category": {"type": "string", "description": "Template category", "enum": ["monitoring", "backup", "reporting", "maintenance", "notification", "data-pipeline", "custom"]},
                "cron": {"type": "string", "description": "Default cron expression"},
                "priority": {"type": "string", "enum": ["low", "normal", "high"]},
                "timeout": {"type": "number", "description": "Default timeout in seconds"},
                "max_retries": {"type": "integer", "description": "Default max retries"},
                "tags": {"type": "array", "items": {"type": "string"}, "description": "Default tags"},
                "required_fields": {"type": "array", "items": {"type": "string"}, "description": "Required field names"},
                "payload": {"type": "object", "description": "Default payload values"},
            },
            "required": ["name", "handler"],
        },
    },
    # ── Group Tools ────────────────────────────────────────
    {
        "name": "scheduler_create_group",
        "description": "Create a job group for organizing related jobs (e.g., per agent, per project) with optional quotas.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Group name (must be unique)"},
                "description": {"type": "string", "description": "Group description"},
                "tags": {"type": "array", "items": {"type": "string"}, "description": "Tags applied to all jobs in group"},
                "max_jobs": {"type": "integer", "description": "Maximum jobs allowed in group (null = unlimited)"},
                "max_concurrent": {"type": "integer", "description": "Max concurrent executions"},
            },
            "required": ["name"],
        },
    },
    {
        "name": "scheduler_list_groups",
        "description": "List all job groups with optional filtering.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "enabled_only": {"type": "boolean", "description": "Show only enabled groups", "default": False},
            },
        },
    },
    {
        "name": "scheduler_get_group",
        "description": "Get group details and statistics.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "group_identifier": {"type": "string", "description": "Group ID or name"},
            },
            "required": ["group_identifier"],
        },
    },
    {
        "name": "scheduler_pause_group",
        "description": "Pause all jobs in a group.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "group_identifier": {"type": "string", "description": "Group ID or name"},
            },
            "required": ["group_identifier"],
        },
    },
    {
        "name": "scheduler_resume_group",
        "description": "Resume all paused jobs in a group.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "group_identifier": {"type": "string", "description": "Group ID or name"},
            },
            "required": ["group_identifier"],
        },
    },
]


class MCPServer:
    """MCP server implementation for agent-scheduler."""

    def __init__(self, scheduler: Scheduler, template_manager: Optional[TemplateManager] = None, group_manager: Optional[GroupManager] = None) -> None:
        self.scheduler = scheduler
        self.template_manager = template_manager or TemplateManager(store=scheduler.store)
        self.group_manager = group_manager or GroupManager(store=scheduler.store, scheduler=scheduler)

    def _resolve_job(self, job_identifier: str) -> Optional[Job]:
        """Resolve a job by ID or name."""
        job = self.scheduler.get_job(job_identifier)
        if job is None:
            job = self.scheduler.get_job_by_name(job_identifier)
        return job

    async def handle_tool_call(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        """Handle an MCP tool call."""
        try:
            handler = getattr(self, f"_tool_{name}", None)
            if handler is None:
                return {"error": f"Unknown tool: {name}"}
            result = await handler(arguments)
            return result
        except Exception as e:
            return {"error": str(e)}

    async def _tool_scheduler_create_job(self, args: dict[str, Any]) -> dict[str, Any]:
        retry_policy = None
        if args.get("max_retries", 0) > 0:
            retry_policy = RetryPolicy(max_retries=args["max_retries"])

        run_at_dt = None
        if args.get("run_at"):
            run_at_dt = datetime.fromisoformat(args["run_at"])
            if run_at_dt.tzinfo is None:
                run_at_dt = run_at_dt.replace(tzinfo=timezone.utc)

        job = Job(
            name=args["name"],
            handler=args["handler"],
            payload=args.get("payload", {}),
            cron=args.get("cron"),
            delay=args.get("delay"),
            run_at=run_at_dt,
            priority=Priority(args.get("priority", "normal")),
            retry_policy=retry_policy,
            timeout=args.get("timeout", 300),
            max_runs=args.get("max_runs"),
            tags=args.get("tags", []),
        )
        self.scheduler.add_job(job)

        # Fire webhook
        if self.scheduler.webhooks:
            await self.scheduler.webhooks.fire_event(WebhookEvent.JOB_CREATED, job)

        return {"job": _job_to_dict(job), "message": f"Job '{job.name}' created"}

    async def _tool_scheduler_list_jobs(self, args: dict[str, Any]) -> dict[str, Any]:
        status = JobStatus(args["status"]) if args.get("status") else None
        jobs = self.scheduler.list_jobs(
            enabled_only=args.get("enabled_only", False),
            tag=args.get("tag"),
            status=status,
        )
        return {"jobs": [_job_to_dict(j) for j in jobs], "count": len(jobs)}

    async def _tool_scheduler_get_job(self, args: dict[str, Any]) -> dict[str, Any]:
        job = self._resolve_job(args["job_identifier"])
        if job is None:
            return {"error": f"Job not found: {args['job_identifier']}"}
        return {"job": _job_to_dict(job)}

    async def _tool_scheduler_update_job(self, args: dict[str, Any]) -> dict[str, Any]:
        job = self._resolve_job(args["job_identifier"])
        if job is None:
            return {"error": f"Job not found: {args['job_identifier']}"}
        updates = {k: v for k, v in args.items() if k not in ("job_identifier",) and v is not None}
        if "priority" in updates:
            updates["priority"] = Priority(updates["priority"])
        updated = self.scheduler.update_job(job.id, **updates)
        if updated is None:
            return {"error": "Failed to update job"}
        return {"job": _job_to_dict(updated), "message": f"Job '{updated.name}' updated"}

    async def _tool_scheduler_delete_job(self, args: dict[str, Any]) -> dict[str, Any]:
        job = self._resolve_job(args["job_identifier"])
        if job is None:
            return {"error": f"Job not found: {args['job_identifier']}"}
        self.scheduler.delete_job(job.id)
        return {"message": f"Job '{job.name}' deleted"}

    async def _tool_scheduler_pause_job(self, args: dict[str, Any]) -> dict[str, Any]:
        job = self._resolve_job(args["job_identifier"])
        if job is None:
            return {"error": f"Job not found: {args['job_identifier']}"}
        self.scheduler.pause_job(job.id)
        return {"message": f"Job '{job.name}' paused"}

    async def _tool_scheduler_resume_job(self, args: dict[str, Any]) -> dict[str, Any]:
        job = self._resolve_job(args["job_identifier"])
        if job is None:
            return {"error": f"Job not found: {args['job_identifier']}"}
        self.scheduler.resume_job(job.id)
        return {"message": f"Job '{job.name}' resumed"}

    async def _tool_scheduler_run_job(self, args: dict[str, Any]) -> dict[str, Any]:
        job = self._resolve_job(args["job_identifier"])
        if job is None:
            return {"error": f"Job not found: {args['job_identifier']}"}
        execution = await self.scheduler.run_job(job.id)
        if execution is None:
            return {"error": "Failed to execute job"}
        return {"execution": _execution_to_dict(execution)}

    async def _tool_scheduler_get_history(self, args: dict[str, Any]) -> dict[str, Any]:
        job_id = None
        if args.get("job_identifier"):
            job = self._resolve_job(args["job_identifier"])
            if job:
                job_id = job.id
            else:
                return {"error": f"Job not found: {args['job_identifier']}"}
        limit = args.get("limit", 50)
        executions = self.scheduler.get_history(job_id=job_id, limit=limit)
        return {"executions": [_execution_to_dict(e) for e in executions], "count": len(executions)}

    async def _tool_scheduler_get_next_run(self, args: dict[str, Any]) -> dict[str, Any]:
        job = self._resolve_job(args["job_identifier"])
        if job is None:
            return {"error": f"Job not found: {args['job_identifier']}"}
        next_run = self.scheduler.get_next_run(job.id)
        return {"next_run_at": next_run.isoformat() if next_run else None}

    async def _tool_scheduler_get_stats(self, args: dict[str, Any]) -> dict[str, Any]:
        stats = self.scheduler.get_stats()
        return {"stats": stats.model_dump(mode="json")}

    async def _tool_scheduler_list_tags(self, args: dict[str, Any]) -> dict[str, Any]:
        tags = self.scheduler.list_tags()
        return {"tags": tags}

    async def _tool_scheduler_get_jobs_by_tag(self, args: dict[str, Any]) -> dict[str, Any]:
        jobs = self.scheduler.get_jobs_by_tag(args["tag"])
        return {"jobs": [_job_to_dict(j) for j in jobs], "count": len(jobs)}

    async def _tool_scheduler_create_dependency(self, args: dict[str, Any]) -> dict[str, Any]:
        job = self._resolve_job(args["job_identifier"])
        depends_on = self._resolve_job(args["depends_on_identifier"])
        if job is None:
            return {"error": f"Job not found: {args['job_identifier']}"}
        if depends_on is None:
            return {"error": f"Dependency job not found: {args['depends_on_identifier']}"}
        on_status = ExecutionStatus(args.get("on_status", "success"))
        dep = self.scheduler.add_dependency(job.id, depends_on.id, on_status)
        return {"dependency": dep.model_dump(mode="json"), "message": f"Dependency created: {job.name} depends on {depends_on.name}"}

    async def _tool_scheduler_get_dependencies(self, args: dict[str, Any]) -> dict[str, Any]:
        job = self._resolve_job(args["job_identifier"])
        if job is None:
            return {"error": f"Job not found: {args['job_identifier']}"}
        deps = self.scheduler.get_dependencies(job.id)
        return {"dependencies": [d.model_dump(mode="json") for d in deps]}

    # ── Webhook Tools ───────────────────────────────────────

    async def _tool_scheduler_create_webhook(self, args: dict[str, Any]) -> dict[str, Any]:
        if self.scheduler.webhooks is None:
            return {"error": "Webhook support not enabled"}
        events = [WebhookEvent(e) for e in args.get("events", [e.value for e in WebhookEvent])]
        webhook = Webhook(
            name=args["name"],
            url=args["url"],
            secret=args.get("secret"),
            events=events,
            tags=args.get("tags", []),
            timeout=args.get("timeout", 10.0),
            max_retries=args.get("max_retries", 3),
        )
        self.scheduler.webhooks.create_webhook(webhook)
        return {"webhook": webhook.model_dump(mode="json"), "message": f"Webhook '{webhook.name}' created"}

    async def _tool_scheduler_list_webhooks(self, args: dict[str, Any]) -> dict[str, Any]:
        if self.scheduler.webhooks is None:
            return {"error": "Webhook support not enabled"}
        enabled_only = args.get("enabled_only", False)
        webhooks = self.scheduler.webhooks.list_webhooks(enabled_only=enabled_only)
        return {"webhooks": [w.model_dump(mode="json") for w in webhooks], "count": len(webhooks)}

    async def _tool_scheduler_delete_webhook(self, args: dict[str, Any]) -> dict[str, Any]:
        if self.scheduler.webhooks is None:
            return {"error": "Webhook support not enabled"}
        deleted = self.scheduler.webhooks.delete_webhook(args["webhook_id"])
        if not deleted:
            return {"error": f"Webhook not found: {args['webhook_id']}"}
        return {"message": f"Webhook {args['webhook_id']} deleted"}

    async def _tool_scheduler_get_webhook_deliveries(self, args: dict[str, Any]) -> dict[str, Any]:
        if self.scheduler.webhooks is None:
            return {"error": "Webhook support not enabled"}
        webhook_id = args.get("webhook_id")
        limit = args.get("limit", 50)
        deliveries = self.scheduler.webhooks.get_deliveries(webhook_id=webhook_id, limit=limit)
        return {
            "deliveries": [d.model_dump(mode="json") for d in deliveries],
            "count": len(deliveries),
        }

    # ── Template Tools ──────────────────────────────────────

    async def _tool_scheduler_list_templates(self, args: dict[str, Any]) -> dict[str, Any]:
        category = TemplateCategory(args["category"]) if args.get("category") else None
        templates = self.template_manager.list_templates(category=category)
        return {
            "templates": [
                {
                    "name": t.name,
                    "description": t.description,
                    "category": t.category.value,
                    "handler": t.handler,
                    "default_cron": t.default_cron,
                    "default_priority": t.default_priority.value,
                    "required_fields": t.required_fields,
                }
                for t in templates
            ],
            "count": len(templates),
        }

    async def _tool_scheduler_get_template(self, args: dict[str, Any]) -> dict[str, Any]:
        template = self.template_manager.get_template_by_name(args["template_name"])
        if template is None:
            return {"error": f"Template not found: {args['template_name']}"}
        return {"template": template.model_dump(mode="json")}

    async def _tool_scheduler_instantiate_template(self, args: dict[str, Any]) -> dict[str, Any]:
        overrides = {}
        if args.get("name"):
            overrides["name"] = args["name"]
        if args.get("cron"):
            overrides["cron"] = args["cron"]
        if args.get("priority"):
            overrides["priority"] = Priority(args["priority"])
        if args.get("payload"):
            overrides["payload"] = args["payload"]
        if args.get("tags"):
            overrides["tags"] = args["tags"]

        try:
            job = self.template_manager.instantiate(args["template_name"], **overrides)
        except ValueError as e:
            return {"error": str(e)}

        self.scheduler.add_job(job)
        return {"job": _job_to_dict(job), "message": f"Job '{job.name}' created from template '{args['template_name']}'"}

    async def _tool_scheduler_create_template(self, args: dict[str, Any]) -> dict[str, Any]:
        template = JobTemplate(
            name=args["name"],
            handler=args["handler"],
            description=args.get("description", ""),
            category=TemplateCategory(args.get("category", "custom")),
            default_cron=args.get("cron"),
            default_priority=Priority(args.get("priority", "normal")),
            default_timeout=args.get("timeout", 300),
            default_max_retries=args.get("max_retries", 0),
            default_tags=args.get("tags", []),
            required_fields=args.get("required_fields", []),
            default_payload=args.get("payload", {}),
        )
        self.template_manager.create_template(template)
        return {"template": template.model_dump(mode="json"), "message": f"Template '{template.name}' created"}

    # ── Group Tools ────────────────────────────────────────

    async def _tool_scheduler_create_group(self, args: dict[str, Any]) -> dict[str, Any]:
        from agent_scheduler.groups import GroupQuota
        quota = None
        max_jobs = args.get("max_jobs")
        max_concurrent = args.get("max_concurrent")
        if max_jobs is not None or max_concurrent is not None:
            quota = GroupQuota(max_jobs=max_jobs, max_concurrent=max_concurrent)
        try:
            group = self.group_manager.create_group(
                name=args["name"],
                description=args.get("description", ""),
                tags=args.get("tags"),
                quota=quota,
            )
            return {"group": group.model_dump(mode="json"), "message": f"Group '{group.name}' created"}
        except ValueError as e:
            return {"error": str(e)}

    async def _tool_scheduler_list_groups(self, args: dict[str, Any]) -> dict[str, Any]:
        groups = self.group_manager.list_groups(enabled_only=args.get("enabled_only", False))
        return {"groups": [g.model_dump(mode="json") for g in groups], "count": len(groups)}

    async def _tool_scheduler_get_group(self, args: dict[str, Any]) -> dict[str, Any]:
        group = self.group_manager.get_group(args["group_identifier"])
        if group is None:
            return {"error": f"Group not found: {args['group_identifier']}"}
        stats = self.group_manager.get_stats(args["group_identifier"])
        result = {"group": group.model_dump(mode="json")}
        if stats:
            result["stats"] = stats.model_dump(mode="json")
        return result

    async def _tool_scheduler_pause_group(self, args: dict[str, Any]) -> dict[str, Any]:
        group = self.group_manager.get_group(args["group_identifier"])
        if group is None:
            return {"error": f"Group not found: {args['group_identifier']}"}
        count = self.group_manager.pause_group(args["group_identifier"])
        return {"message": f"Paused {count} jobs in group '{group.name}'"}

    async def _tool_scheduler_resume_group(self, args: dict[str, Any]) -> dict[str, Any]:
        group = self.group_manager.get_group(args["group_identifier"])
        if group is None:
            return {"error": f"Group not found: {args['group_identifier']}"}
        count = self.group_manager.resume_group(args["group_identifier"])
        return {"message": f"Resumed {count} jobs in group '{group.name}'"}


async def run_mcp_server(data_dir: Optional[str] = None, port: int = 8080) -> None:
    """Run the MCP server using stdio transport."""
    import sys

    store = JSONJobStore(data_dir=data_dir)
    scheduler = Scheduler(store=store)
    template_manager = TemplateManager(store=store)
    server = MCPServer(scheduler, template_manager)

    # Simple stdio-based MCP protocol
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    await asyncio.get_event_loop().connect_read_pipe(lambda: protocol, sys.stdin)

    writer_transport, writer_protocol = await asyncio.get_event_loop().connect_write_pipe(
        asyncio.streams.FlowControlMixin, sys.stdout
    )
    writer = asyncio.StreamWriter(writer_transport, writer_protocol, reader, asyncio.get_event_loop())

    while True:
        try:
            line = await reader.readline()
            if not line:
                break
            message = json.loads(line.decode())

            if message.get("method") == "tools/list":
                response = {"jsonrpc": "2.0", "id": message.get("id"), "result": {"tools": TOOLS}}
            elif message.get("method") == "tools/call":
                params = message.get("params", {})
                tool_name = params.get("name")
                arguments = params.get("arguments", {})
                result = await server.handle_tool_call(tool_name, arguments)
                response = {"jsonrpc": "2.0", "id": message.get("id"), "result": {"content": [{"type": "text", "text": json.dumps(result)}]}}
            elif message.get("method") == "initialize":
                response = {
                    "jsonrpc": "2.0",
                    "id": message.get("id"),
                    "result": {
                        "protocolVersion": "2024-11-05",
                        "capabilities": {"tools": {}},
                        "serverInfo": {"name": "agent-scheduler", "version": "0.3.0"},
                    },
                }
            else:
                response = {"jsonrpc": "2.0", "id": message.get("id"), "error": {"code": -32601, "message": f"Unknown method: {message.get('method')}"}}

            writer.write((json.dumps(response) + "\n").encode())
            await writer.drain()
        except asyncio.CancelledError:
            break
        except Exception as e:
            error_response = {"jsonrpc": "2.0", "id": None, "error": {"code": -32603, "message": str(e)}}
            try:
                writer.write((json.dumps(error_response) + "\n").encode())
                await writer.drain()
            except Exception:
                pass
