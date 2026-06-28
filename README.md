# agent-scheduler

Task scheduling engine for autonomous agents — cron-like recurring jobs, one-time delayed tasks, priority queues, retry logic, and MCP server integration.

## Features

- **Recurring Jobs** — Cron-like scheduling using standard cron expressions (e.g. `0 9 * * MON-FRI`)
- **One-Time Tasks** — Schedule delayed or future-dated one-shot tasks
- **Priority Queues** — Three priority levels (low, normal, high) with ordered execution
- **Retry Logic** — Configurable retry policies with exponential backoff
- **Job Dependencies** — Chain jobs together so one triggers after another completes
- **Tags & Filters** — Organize jobs with tags, filter and query by tag
- **Job History** — Full execution log with status, duration, and error details
- **Persistence** — JSON-based storage, survives restarts
- **MCP Server** — Full Model Context Protocol server with 15+ tools
- **CLI** — Rich terminal interface for managing schedules

## Installation

```bash
pip install agent-scheduler
```

## Quick Start

### Python API

```python
from agent_scheduler import Scheduler, Job, RetryPolicy
from datetime import timedelta

scheduler = Scheduler()

# One-time delayed task
job = Job(
    name="send-report",
    handler="email.send",
    payload={"to": "agent@example.com", "subject": "Daily Report"},
    delay=timedelta(hours=2),
    priority="high",
)
scheduler.add_job(job)

# Recurring cron job
job = Job(
    name="health-check",
    handler="http.get",
    payload={"url": "https://api.example.com/health"},
    cron="*/5 * * * *",  # Every 5 minutes
    retry_policy=RetryPolicy(max_retries=3, backoff_seconds=30),
    tags=["monitoring", "health"],
)
scheduler.add_job(job)

# Start the scheduler
scheduler.start()
```

### CLI

```bash
# Add a one-time task
agent-scheduler add --name "backup-db" --handler "db.backup" --delay 3600

# Add a recurring job
agent-scheduler add --name "sync-data" --handler "api.sync" --cron "0 */6 * * *"

# List all jobs
agent-scheduler list

# Show job details
agent-scheduler show backup-db

# Show execution history
agent-scheduler history backup-db

# Run due jobs now
agent-scheduler run

# Start the scheduler daemon
agent-scheduler start
```

### MCP Server

```bash
agent-scheduler serve --port 8080
```

MCP tools available:
- `scheduler_create_job` — Create a new scheduled job
- `scheduler_list_jobs` — List all jobs with optional filtering
- `scheduler_get_job` — Get job details
- `scheduler_update_job` — Update job configuration
- `scheduler_delete_job` — Delete a job
- `scheduler_pause_job` — Pause a job
- `scheduler_resume_job` — Resume a paused job
- `scheduler_run_job` — Trigger a job manually
- `scheduler_get_history` — Get execution history
- `scheduler_get_next_run` — Get next scheduled run time
- `scheduler_get_stats` — Get scheduler statistics
- `scheduler_list_tags` — List all tags
- `scheduler_get_jobs_by_tag` — Get jobs by tag
- `scheduler_create_dependency` — Create a job dependency
- `scheduler_get_dependencies` — Get job dependencies

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│  CLI / MCP  │────▶│   Scheduler   │────▶│   Handler   │
│  Interface  │     │    Engine     │     │  Registry   │
└─────────────┘     └──────┬───────┘     └─────────────┘
                           │
                    ┌──────┴───────┐
                    │   Job Queue  │
                    │  (Priority)  │
                    └──────┬───────┘
                           │
                    ┌──────┴───────┐
                    │  Persistence │
                    │   (JSON)     │
                    └──────────────┘
```

## Job Model

| Field | Type | Description |
|-------|------|-------------|
| `id` | str | Unique identifier (auto-generated) |
| `name` | str | Human-readable name |
| `handler` | str | Handler function identifier |
| `payload` | dict | Data passed to handler |
| `cron` | str \| None | Cron expression for recurring jobs |
| `delay` | float \| None | Seconds until first run (one-time) |
| `run_at` | datetime \| None | Specific future run time |
| `priority` | str | `low`, `normal`, or `high` |
| `retry_policy` | RetryPolicy \| None | Retry configuration |
| `tags` | list[str] | Tags for filtering |
| `enabled` | bool | Whether job is active |
| `max_runs` | int \| None | Maximum number of runs |
| `timeout` | float | Run timeout in seconds |
| `metadata` | dict | Extra key-value data |

## Retry Policy

```python
RetryPolicy(
    max_retries=3,           # Maximum retry attempts
    backoff_seconds=30,      # Base backoff duration
    backoff_multiplier=2.0,  # Exponential multiplier
    max_backoff=3600,        # Maximum backoff cap
    retry_on_errors=None,    # List of error patterns to retry on
)
```

## License

MIT
