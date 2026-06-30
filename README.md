# Agent Scheduler

Task scheduling engine for autonomous agents — cron-like recurring jobs, one-time delayed tasks, priority queues, retry logic, webhooks, templates, job groups, API key auth, SQLite backend, MCP server, and REST API.

## Features

### Core Scheduling
- **Cron jobs** — Recurring jobs via standard cron expressions (`0 9 * * MON-FRI`)
- **One-time tasks** — Delayed (`--delay 3600`) or scheduled at specific time (`--run-at`)
- **Immediate execution** — Jobs that run right away
- **Priority queues** — Low / Normal / High with ordered execution
- **Retry with backoff** — Exponential backoff, error-specific retry rules, max retries
- **Job dependencies** — Chain jobs with `on_status` conditions (success/failed/timeout)
- **Tags** — Filter and organize jobs with tags
- **Max runs** — Limit total executions per job
- **Timeout** — Per-job execution timeout

### Webhook Notifications (v0.2.0)
- **HTTP callbacks** — Fire POST requests on job events (created, completed, failed, timeout, retry, etc.)
- **HMAC signatures** — SHA-256 signing for payload verification
- **Tag filtering** — Only fire webhooks for jobs with specific tags
- **Custom headers** — Add authentication or custom HTTP headers
- **Delivery retry** — Configurable retry on failed deliveries
- **Delivery history** — Track all webhook delivery attempts

### Job Templates (v0.2.0)
- **6 built-in templates** — Health check, daily backup, weekly report, data pipeline, cleanup, notification
- **Custom templates** — Create reusable job blueprints
- **Required fields** — Enforce mandatory configuration when instantiating
- **Default overrides** — Pre-configured priority, retry, timeout, payload defaults
- **Categories** — Organize templates by type (monitoring, backup, reporting, etc.)

### SQLite Persistence (v0.3.0)
- **Production backend** — SQLite with WAL mode for atomic transactions
- **Efficient queries** — Indexed lookups by status, priority, next run, name
- **Scalable** — Handles millions of records with proper pagination
- **Better concurrency** — WAL mode allows concurrent reads during writes
- **Drop-in replacement** — Same JobStore interface, just use `SQLiteJobStore`

### API Key Authentication (v0.3.0)
- **Bearer token auth** — `Authorization: Bearer ask_...` or `X-API-Key` header
- **Scoped permissions** — 9 scopes (jobs:read/write, executions:read/write, webhooks:read/write, templates:read/write, admin, *)
- **Rate limiting** — Configurable per-key request limits (default: 100/min)
- **Key management** — Create, list, revoke, enable/disable keys via API or CLI
- **Usage tracking** — Last used timestamp and request count per key

### Job Groups (v0.3.0)
- **Multi-tenant** — Organize jobs by agent, project, or team
- **Quotas** — Per-group job limits and concurrent execution caps
- **Bulk operations** — Pause/resume all jobs in a group
- **Group stats** — Track job counts, execution stats, and quota usage per group
- **Auto-tagging** — Jobs automatically tagged with group ID and group defaults

### Execution Analytics (v0.4.0)
- **Health scoring** — Composite 0-100 score per job (success rate, trend, recency, failure trend)
- **Letter grades** — A-F health grades for quick at-a-glance assessment
- **Duration statistics** — Min, max, avg, median, p95, p99 percentiles
- **Failure pattern analysis** — Groups and ranks common errors across jobs
- **Scheduler dashboard** — Aggregate health, execution counts (24h/7d/all-time), top failures
- **At-risk detection** — Automatically flags jobs with health score < 50
- **Stale job detection** — Identifies scheduled jobs that haven't run in 24h+

### Cron Expression Toolkit (v0.4.0)
- **Validation** — Validate cron expressions with detailed error messages
- **Human-readable descriptions** — Translate cron to English ("Every Monday at 9:00 AM")
- **Run preview** — Show the next N scheduled run times
- **Expression builder** — Construct cron from natural parameters (`daily`, `weekly`, `weekdays`, etc.)
- **Field parser** — Extract field meanings from any cron expression

### Notification Channels (v0.4.0)
- **Slack** — Rich Block Kit messages via Incoming Webhooks
- **Discord** — Formatted embeds with color-coded severity
- **Email** — HTML + plain text via SMTP with TLS/SSL support
- **Generic HTTP** — JSON POST to any endpoint with optional HMAC signing
- **Channel manager** — Register multiple channels, filter by severity level
- **Config factory** — Create channels from config dicts for easy setup

### Integration
- **MCP server** — 29+ tools for agent integration via Model Context Protocol
- **REST API** — 28+ HTTP endpoints for remote integration (Starlette + raw ASGI fallback)
- **CLI** — 30+ commands with Rich formatting
- **JSON or SQLite persistence** — Zero-config JSON or production-grade SQLite

## Quick Start

### Install

```bash
pip install agent-scheduler
# Optional: for REST API support
pip install agent-scheduler[api]
```

### CLI Usage

```bash
# Create a recurring job
agent-scheduler add --name "daily-report" --handler report.generate \
  --cron "0 9 * * MON-FRI" --priority high --tags reporting,daily

# Create a one-time delayed job
agent-scheduler add --name "cleanup-temp" --handler cleanup.run \
  --delay 3600 --tags maintenance

# Create a job with retry policy
agent-scheduler add --name "api-poll" --handler poll.endpoint \
  --cron "*/5 * * * *" --max-retries 3 --timeout 30

# List all jobs
agent-scheduler list

# Show job details
agent-scheduler show daily-report

# Manually run a job
agent-scheduler run daily-report

# Run all due jobs
agent-scheduler run-due

# View execution history
agent-scheduler history daily-report --limit 20

# View statistics
agent-scheduler stats

# Pause/resume a job
agent-scheduler pause daily-report
agent-scheduler resume daily-report

# Delete a job
agent-scheduler delete daily-report --force
```

### Webhook Management

```bash
# Create a webhook for job completion events
agent-scheduler webhook add \
  --name "slack-notify" \
  --url "https://hooks.slack.com/services/XXX" \
  --events job.completed,job.failed \
  --tags monitoring \
  --secret "my-signing-secret"

# List webhooks
agent-scheduler webhook list

# View delivery history
agent-scheduler webhook deliveries

# Delete a webhook
agent-scheduler webhook delete <webhook-id> --force
```

### Template Usage

```bash
# List available templates
agent-scheduler template list

# Show template details
agent-scheduler template show health-check

# Create a job from a template
agent-scheduler template use health-check \
  --name "api-health" \
  --payload '{"endpoint": "https://api.example.com/health"}'

# Create a custom template
agent-scheduler template add \
  --name "my-pipeline" \
  --handler pipeline.run \
  --description "My data pipeline" \
  --category data-pipeline \
  --cron "0 */4 * * *" \
  --tags pipeline \
  --max-retries 2 \
  --required-fields "payload.pipeline_id"
```

### Analytics & Health (v0.4.0)

```bash
# Show the full analytics dashboard
agent-scheduler analytics

# Health report for a specific job
agent-scheduler health daily-report
```

### Cron Toolkit (v0.4.0)

```bash
# Validate a cron expression
agent-scheduler cron validate "0 9 * * MON-FRI"

# Describe a cron expression in plain English
agent-scheduler cron describe "*/15 * * * *"
# => Every 15 minutes

# Preview the next 10 runs
agent-scheduler cron preview "0 9 * * *" --count 10

# Build a cron expression from parameters
agent-scheduler cron build --frequency daily --hour 9 --minute 30
# => 30 9 * * *
agent-scheduler cron build --frequency weekly --day monday --hour 9
# => 0 9 * * 0
agent-scheduler cron build --frequency every-n-minutes --n 15
# => */15 * * * *
```

### REST API

```bash
# Start the REST API server
agent-scheduler api --host 0.0.0.0 --port 8080

# Or start the scheduler daemon (poll loop + MCP)
agent-scheduler start

# Or start the MCP server
agent-scheduler serve --port 8080
```

#### API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| GET | `/api/v1/jobs` | List jobs |
| POST | `/api/v1/jobs` | Create job |
| GET | `/api/v1/jobs/{id}` | Get job |
| PATCH | `/api/v1/jobs/{id}` | Update job |
| DELETE | `/api/v1/jobs/{id}` | Delete job |
| POST | `/api/v1/jobs/{id}/pause` | Pause job |
| POST | `/api/v1/jobs/{id}/resume` | Resume job |
| POST | `/api/v1/jobs/{id}/run` | Run job |
| GET | `/api/v1/jobs/{id}/next-run` | Get next run time |
| GET | `/api/v1/jobs/{id}/dependencies` | Get dependencies |
| GET | `/api/v1/history` | Execution history |
| GET | `/api/v1/stats` | Scheduler statistics |
| GET | `/api/v1/tags` | List tags |
| GET | `/api/v1/tags/{tag}` | Jobs by tag |
| POST | `/api/v1/dependencies` | Create dependency |
| POST | `/api/v1/run-due` | Run all due jobs |
| GET | `/api/v1/webhooks` | List webhooks |
| POST | `/api/v1/webhooks` | Create webhook |
| DELETE | `/api/v1/webhooks/{id}` | Delete webhook |
| GET | `/api/v1/webhook-deliveries` | Delivery history |

### MCP Tools (29 tools)

| Tool | Description |
|------|-------------|
| `scheduler_create_job` | Create a scheduled job |
| `scheduler_list_jobs` | List jobs with filters |
| `scheduler_get_job` | Get job details |
| `scheduler_update_job` | Update job config |
| `scheduler_delete_job` | Delete a job |
| `scheduler_pause_job` | Pause a job |
| `scheduler_resume_job` | Resume a job |
| `scheduler_run_job` | Manually trigger execution |
| `scheduler_get_history` | Execution history |
| `scheduler_get_next_run` | Next scheduled run |
| `scheduler_get_stats` | Scheduler statistics |
| `scheduler_list_tags` | List all tags |
| `scheduler_get_jobs_by_tag` | Jobs by tag |
| `scheduler_create_dependency` | Chain jobs |
| `scheduler_get_dependencies` | Job dependencies |
| `scheduler_create_webhook` | Create webhook subscription |
| `scheduler_list_webhooks` | List webhooks |
| `scheduler_delete_webhook` | Delete webhook |
| `scheduler_get_webhook_deliveries` | Webhook delivery history |
| `scheduler_list_templates` | List job templates |
| `scheduler_get_template` | Get template details |
| `scheduler_instantiate_template` | Create job from template |
| `scheduler_create_template` | Create custom template |
| `scheduler_create_group` | Create a job group |
| `scheduler_list_groups` | List job groups |
| `scheduler_get_group` | Get group details |
| `scheduler_pause_group` | Pause all jobs in group |
| `scheduler_resume_group` | Resume all jobs in group |
| `scheduler_analytics_dashboard` | Full analytics dashboard (v0.4.0) |
| `scheduler_job_health` | Per-job health report (v0.4.0) |
| `scheduler_validate_cron` | Validate cron expression (v0.4.0) |
| `scheduler_describe_cron` | Describe cron in English (v0.4.0) |
| `scheduler_preview_cron` | Preview upcoming runs (v0.4.0) |
| `scheduler_build_cron` | Build cron from parameters (v0.4.0) |

## Built-in Templates

| Template | Handler | Default Schedule | Category |
|----------|---------|-------------------|----------|
| `health-check` | `health.check` | Every 5 min | Monitoring |
| `daily-backup` | `backup.run` | Daily 2 AM | Backup |
| `weekly-report` | `report.generate` | Monday 9 AM | Reporting |
| `data-pipeline` | `pipeline.run` | Every 6 hours | Data Pipeline |
| `cleanup` | `cleanup.run` | Daily 3 AM | Maintenance |
| `notification` | `notify.send` | On demand | Notification |

## Webhook Events

| Event | When |
|-------|------|
| `job.created` | Job is created |
| `job.completed` | Job executes successfully |
| `job.failed` | Job execution fails |
| `job.timeout` | Job execution times out |
| `job.retry` | Job is being retried |
| `job.paused` | Job is paused |
| `job.resumed` | Job is resumed |
| `job.cancelled` | Job is cancelled |
| `job.deleted` | Job is deleted |

## Python API

```python
from agent_scheduler import Scheduler, Job, Priority, RetryPolicy
from agent_scheduler.webhook import Webhook, WebhookEvent

# Create scheduler
scheduler = Scheduler()

# Add a job
job = Job(
    name="daily-report",
    handler="report.generate",
    cron="0 9 * * MON-FRI",
    priority=Priority.HIGH,
    retry_policy=RetryPolicy(max_retries=3, backoff_seconds=60),
    tags=["reporting"],
    payload={"format": "pdf", "recipients": ["team@example.com"]},
)
scheduler.add_job(job)

# Add a webhook
webhook = Webhook(
    name="slack-notify",
    url="https://hooks.slack.com/services/XXX",
    events=[WebhookEvent.JOB_COMPLETED, WebhookEvent.JOB_FAILED],
    tags=["reporting"],
    secret="my-signing-secret",
)
scheduler.webhooks.create_webhook(webhook)

# Use a template
from agent_scheduler.templates import TemplateManager
manager = TemplateManager(store=scheduler.store)
job = manager.instantiate("health-check", payload={"endpoint": "https://api.example.com/health"})
scheduler.add_job(job)

# Run due jobs (async)
import asyncio
executions = asyncio.run(scheduler.run_due_jobs())

# Get stats
stats = scheduler.get_stats()
print(f"Active: {stats.active_jobs}, Failed: {stats.failed_jobs}")
```

### Analytics API (v0.4.0)

```python
from agent_scheduler.analytics import AnalyticsEngine

engine = AnalyticsEngine(scheduler=scheduler)

# Full dashboard
dashboard = engine.dashboard()
print(f"Health: {dashboard.overall_health_grade} ({dashboard.overall_health_score}/100)")
print(f"At-risk jobs: {dashboard.at_risk_jobs}")

# Single job health
job = scheduler.get_job_by_name("daily-report")
report = engine.job_report(job)
print(f"{report.job_name}: {report.health_grade} ({report.health_score}/100)")
```

### Cron Toolkit API (v0.4.0)

```python
from agent_scheduler.cron_helper import (
    validate_cron, describe_cron, preview_runs, suggest_cron, CronBuilder
)

# Validate
result = validate_cron("0 9 * * MON-FRI")
assert result.is_valid

# Describe
print(describe_cron("*/15 * * * *"))  # => "Every 15 minutes"

# Preview next runs
runs = preview_runs("0 9 * * *", n=5)

# Build from parameters
expr = suggest_cron("daily", hour=9, minute=30)  # => "30 9 * * *"
expr = suggest_cron("weekdays", hour=9)           # => "0 9 * * 0-4"
```

### Notification Channels API (v0.4.0)

```python
from agent_scheduler.notifications import (
    Notification, NotificationLevel, ChannelManager,
    SlackChannel, DiscordChannel, EmailChannel, HttpChannel,
)

# Set up channels
mgr = ChannelManager()
mgr.add_channel(SlackChannel(
    webhook_url="https://hooks.slack.com/services/XXX",
    name="ops-slack",
))
mgr.add_channel(DiscordChannel(
    webhook_url="https://discord.com/api/webhooks/XXX",
), levels=[NotificationLevel.ERROR])  # Only errors to Discord

# Send a notification
notif = Notification(
    title="Job Failed",
    message="daily-report failed after 3 retries",
    level=NotificationLevel.ERROR,
    job_name="daily-report",
    event_type="job.failed",
    metadata={"retry_count": 3},
)
results = asyncio.run(mgr.send(notif))

# Or create from config dict
from agent_scheduler.notifications import create_channel_from_config
ch = create_channel_from_config({
    "type": "slack",
    "webhook_url": "https://hooks.slack.com/services/XXX",
    "channel": "#alerts",
})
```

## Handler Registration

Register custom handlers for real job execution:

```python
from agent_scheduler import Scheduler, Job, HandlerRegistry

registry = HandlerRegistry()

# Sync handler
def my_handler(payload):
    # Do work
    return {"status": "done", "processed": len(payload)}

registry.register("my.handler", my_handler)

# Async handler
async def async_handler(payload):
    await some_async_work()
    return {"async": True}

registry.register("my.async_handler", async_handler)

scheduler = Scheduler(handler_registry=registry)
```

## Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `SCHEDULER_DATA_DIR` | `~/.agent-scheduler` | Data storage directory |

## License

MIT
