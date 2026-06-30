"""Agent Scheduler — Task scheduling engine for autonomous agents."""

__version__ = "0.4.0"

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
from agent_scheduler.groups import JobGroup, GroupManager, GroupQuota
from agent_scheduler.auth import ApiKey, ApiKeyManager, ApiKeyScope, RateLimitConfig
from agent_scheduler.analytics import (
    AnalyticsEngine,
    JobHealthReport,
    SchedulerAnalytics,
    DurationStats,
    FailurePattern,
    compute_job_health,
    compute_duration_stats,
    analyze_failures,
)
from agent_scheduler.cron_helper import (
    CronBuilder,
    CronInfo,
    CronValidation,
    validate_cron,
    parse_cron,
    describe_cron,
    preview_runs,
    suggest_cron,
)
from agent_scheduler.notifications import (
    Notification,
    NotificationLevel,
    DeliveryResult,
    NotificationChannel,
    ChannelManager,
    SlackChannel,
    DiscordChannel,
    EmailChannel,
    HttpChannel,
    create_channel_from_config,
)

__all__ = [
    # Models
    "Job",
    "JobStatus",
    "RetryPolicy",
    "JobExecution",
    "JobDependency",
    "Priority",
    # Core
    "Scheduler",
    "HandlerRegistry",
    # Webhooks
    "Webhook",
    "WebhookEvent",
    "WebhookManager",
    # Templates
    "JobTemplate",
    "TemplateCategory",
    "TemplateManager",
    # Groups
    "JobGroup",
    "GroupManager",
    "GroupQuota",
    # Auth
    "ApiKey",
    "ApiKeyManager",
    "ApiKeyScope",
    "RateLimitConfig",
    # Analytics
    "AnalyticsEngine",
    "JobHealthReport",
    "SchedulerAnalytics",
    "DurationStats",
    "FailurePattern",
    "compute_job_health",
    "compute_duration_stats",
    "analyze_failures",
    # Cron Helper
    "CronBuilder",
    "CronInfo",
    "CronValidation",
    "validate_cron",
    "parse_cron",
    "describe_cron",
    "preview_runs",
    "suggest_cron",
    # Notifications
    "Notification",
    "NotificationLevel",
    "DeliveryResult",
    "NotificationChannel",
    "ChannelManager",
    "SlackChannel",
    "DiscordChannel",
    "EmailChannel",
    "HttpChannel",
    "create_channel_from_config",
]
