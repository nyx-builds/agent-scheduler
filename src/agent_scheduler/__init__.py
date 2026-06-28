"""Agent Scheduler — Task scheduling engine for autonomous agents."""

__version__ = "0.2.0"

from agent_scheduler.models import (
    Job,
    JobStatus,
    RetryPolicy,
    JobExecution,
    JobDependency,
    Priority,
)
from agent_scheduler.scheduler import Scheduler
from agent_scheduler.handler import HandlerRegistry
from agent_scheduler.webhook import Webhook, WebhookEvent, WebhookManager
from agent_scheduler.templates import JobTemplate, TemplateCategory, TemplateManager

__all__ = [
    "Job",
    "JobStatus",
    "RetryPolicy",
    "JobExecution",
    "JobDependency",
    "Priority",
    "Scheduler",
    "HandlerRegistry",
    "Webhook",
    "WebhookEvent",
    "WebhookManager",
    "JobTemplate",
    "TemplateCategory",
    "TemplateManager",
]
