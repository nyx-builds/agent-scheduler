# Agent Scheduler

Task scheduling engine for autonomous agents — cron-like recurring jobs, one-time delayed tasks, priority queues, retry logic, webhooks, templates, MCP server, and REST API.

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

### Integration
- **MCP server** — 20 tools for agent integration via Model Context Protocol
- **REST API** — HTTP endpoints for remote integration (Starlette + raw ASGI fallback)
- **CLI** — 20+ commands with Rich formatting
- **JSON persistence** — Zero-config, survives restarts

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

### MCP Tools (20 tools)

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
