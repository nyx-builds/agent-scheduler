"""Agent Scheduler — Task scheduling engine for autonomous agents."""

__version__ = "0.6.0"

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
from agent_scheduler.dlq import (
    DLQEntry,
    DLQReason,
    DLQStats,
    DeadLetterQueue,
)
from agent_scheduler.result_chain import (
    ChainStep,
    Pipeline,
    PipelineStatus,
    ResultChainManager,
    ResultConfig,
    ResultMergeStrategy,
)
from agent_scheduler.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerRegistry,
    CircuitConfig,
    CircuitState,
)
from agent_scheduler.conditions import (
    AndCondition,
    ConditionContext,
    ConditionEvaluationError,
    ConditionOperator,
    ConditionRule,
    evaluate_condition,
    NotCondition,
    OrCondition,
)
from agent_scheduler.time_window import (
    TimeWindow,
    is_within_window,
    next_window_start,
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
    # DLQ
    "DLQEntry",
    "DLQReason",
    "DLQStats",
    "DeadLetterQueue",
    # Result Chaining
    "ChainStep",
    "Pipeline",
    "PipelineStatus",
    "ResultChainManager",
    "ResultConfig",
    "ResultMergeStrategy",
    # Circuit Breaker (v0.6.0)
    "CircuitBreaker",
    "CircuitBreakerRegistry",
    "CircuitConfig",
    "CircuitState",
    # Conditional Execution (v0.6.0)
    "AndCondition",
    "ConditionContext",
    "ConditionEvaluationError",
    "ConditionOperator",
    "ConditionRule",
    "evaluate_condition",
    "NotCondition",
    "OrCondition",
    # Time Window (v0.6.0)
    "TimeWindow",
    "is_within_window",
    "next_window_start",
]
