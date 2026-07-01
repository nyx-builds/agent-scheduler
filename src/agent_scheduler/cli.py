"""CLI interface for agent-scheduler."""

from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text

from agent_scheduler.models import (
    ExecutionStatus,
    Job,
    JobStatus,
    Priority,
    RetryPolicy,
)
from agent_scheduler.scheduler import Scheduler
from agent_scheduler.store import JSONJobStore
from agent_scheduler.webhook import Webhook, WebhookEvent, WebhookManager
from agent_scheduler.templates import JobTemplate, TemplateCategory, TemplateManager, BUILTIN_TEMPLATES

console = Console()


def get_scheduler(data_dir: Optional[str] = None) -> Scheduler:
    store = JSONJobStore(data_dir=data_dir)
    return Scheduler(store=store)


@click.group()
@click.option("--data-dir", envvar="SCHEDULER_DATA_DIR", default=None, help="Data directory")
@click.pass_context
def cli(ctx: click.Context, data_dir: Optional[str]) -> None:
    """Agent Scheduler — Task scheduling engine for autonomous agents."""
    ctx.ensure_object(dict)
    ctx.obj["data_dir"] = data_dir


@cli.command()
@click.option("--name", required=True, help="Job name")
@click.option("--handler", required=True, help="Handler function identifier")
@click.option("--cron", default=None, help="Cron expression for recurring jobs")
@click.option("--delay", default=None, type=float, help="Delay in seconds before first run")
@click.option("--run-at", default=None, help="ISO datetime for future run")
@click.option("--priority", type=click.Choice(["low", "normal", "high"]), default="normal")
@click.option("--tags", default=None, help="Comma-separated tags")
@click.option("--max-retries", default=0, type=int, help="Max retry attempts")
@click.option("--timeout", default=300, type=float, help="Run timeout in seconds")
@click.option("--max-runs", default=None, type=int, help="Max number of runs")
@click.option("--payload", default=None, help="JSON payload string")
@click.pass_context
def add(
    ctx: click.Context,
    name: str,
    handler: str,
    cron: Optional[str],
    delay: Optional[float],
    run_at: Optional[str],
    priority: str,
    tags: Optional[str],
    max_retries: int,
    timeout: float,
    max_runs: Optional[int],
    payload: Optional[str],
) -> None:
    """Add a new scheduled job."""
    scheduler = get_scheduler(ctx.obj["data_dir"])

    # Parse payload
    job_payload = {}
    if payload:
        try:
            job_payload = json.loads(payload)
        except json.JSONDecodeError as e:
            console.print(f"[red]Invalid JSON payload: {e}[/red]")
            sys.exit(1)

    # Parse run_at
    run_at_dt = None
    if run_at:
        try:
            run_at_dt = datetime.fromisoformat(run_at)
            if run_at_dt.tzinfo is None:
                run_at_dt = run_at_dt.replace(tzinfo=timezone.utc)
        except ValueError as e:
            console.print(f"[red]Invalid run-at datetime: {e}[/red]")
            sys.exit(1)

    # Parse tags
    tag_list = []
    if tags:
        tag_list = [t.strip() for t in tags.split(",") if t.strip()]

    # Build retry policy
    retry_policy = None
    if max_retries > 0:
        retry_policy = RetryPolicy(max_retries=max_retries)

    job = Job(
        name=name,
        handler=handler,
        payload=job_payload,
        cron=cron,
        delay=delay,
        run_at=run_at_dt,
        priority=Priority(priority),
        retry_policy=retry_policy,
        timeout=timeout,
        max_runs=max_runs,
        tags=tag_list,
    )

    scheduler.add_job(job)
    console.print(Panel(
        f"[green]Job created successfully![/green]\n\n"
        f"  ID: {job.id}\n"
        f"  Name: {job.name}\n"
        f"  Handler: {job.handler}\n"
        f"  Type: {'Recurring' if job.is_recurring else 'One-time' if job.is_one_time else 'Immediate'}\n"
        f"  Next run: {job.next_run_at or 'N/A'}\n"
        f"  Priority: {job.priority.value}",
        title="Job Added",
    ))


@cli.command("list")
@click.option("--tag", default=None, help="Filter by tag")
@click.option("--status", default=None, type=click.Choice(["scheduled", "paused", "completed", "failed", "cancelled"]))
@click.option("--enabled-only", is_flag=True, help="Show only enabled jobs")
@click.pass_context
def list_jobs(ctx: click.Context, tag: Optional[str], status: Optional[str], enabled_only: bool) -> None:
    """List all scheduled jobs."""
    scheduler = get_scheduler(ctx.obj["data_dir"])
    status_enum = JobStatus(status) if status else None
    jobs = scheduler.list_jobs(enabled_only=enabled_only, tag=tag, status=status_enum)

    if not jobs:
        console.print("[dim]No jobs found.[/dim]")
        return

    table = Table(title="Scheduled Jobs", show_lines=True)
    table.add_column("ID", style="cyan", no_wrap=True)
    table.add_column("Name", style="white")
    table.add_column("Handler", style="blue")
    table.add_column("Type", style="magenta")
    table.add_column("Priority", style="yellow")
    table.add_column("Status", style="green")
    table.add_column("Next Run", style="green")
    table.add_column("Runs", justify="right")
    table.add_column("Tags", style="dim")

    for job in jobs:
        job_type = "Recurring" if job.is_recurring else "One-time" if job.is_one_time else "Immediate"
        next_run = job.next_run_at.strftime("%Y-%m-%d %H:%M") if job.next_run_at else "—"
        tags_str = ", ".join(job.tags) if job.tags else "—"

        status_style = {
            JobStatus.SCHEDULED: "green",
            JobStatus.PAUSED: "yellow",
            JobStatus.COMPLETED: "blue",
            JobStatus.FAILED: "red",
            JobStatus.CANCELLED: "dim",
        }.get(job.status, "white")

        table.add_row(
            job.id,
            job.name,
            job.handler,
            job_type,
            job.priority.value,
            f"[{status_style}]{job.status.value}[/{status_style}]",
            next_run,
            f"{job.run_count}/{job.fail_count}",
            tags_str,
        )

    console.print(table)


@cli.command()
@click.argument("job_identifier")
@click.pass_context
def show(ctx: click.Context, job_identifier: str) -> None:
    """Show detailed information about a job."""
    scheduler = get_scheduler(ctx.obj["data_dir"])
    job = scheduler.get_job(job_identifier) or scheduler.get_job_by_name(job_identifier)
    if job is None:
        console.print(f"[red]Job not found: {job_identifier}[/red]")
        sys.exit(1)

    job_type = "Recurring" if job.is_recurring else "One-time" if job.is_one_time else "Immediate"
    next_run = job.next_run_at.isoformat() if job.next_run_at else "N/A"
    last_run = job.last_run_at.isoformat() if job.last_run_at else "N/A"
    tags_str = ", ".join(job.tags) if job.tags else "None"

    info = (
        f"  [cyan]ID:[/cyan] {job.id}\n"
        f"  [cyan]Name:[/cyan] {job.name}\n"
        f"  [cyan]Handler:[/cyan] {job.handler}\n"
        f"  [cyan]Type:[/cyan] {job_type}\n"
        f"  [cyan]Status:[/cyan] {job.status.value}\n"
        f"  [cyan]Priority:[/cyan] {job.priority.value}\n"
        f"  [cyan]Enabled:[/cyan] {job.enabled}\n"
        f"  [cyan]Cron:[/cyan] {job.cron or 'N/A'}\n"
        f"  [cyan]Delay:[/cyan] {job.delay or 'N/A'}\n"
        f"  [cyan]Timeout:[/cyan] {job.timeout}s\n"
        f"  [cyan]Max Runs:[/cyan] {job.max_runs or 'Unlimited'}\n"
        f"  [cyan]Next Run:[/cyan] {next_run}\n"
        f"  [cyan]Last Run:[/cyan] {last_run}\n"
        f"  [cyan]Run Count:[/cyan] {job.run_count}\n"
        f"  [cyan]Fail Count:[/cyan] {job.fail_count}\n"
        f"  [cyan]Last Error:[/cyan] {job.last_error or 'None'}\n"
        f"  [cyan]Tags:[/cyan] {tags_str}\n"
        f"  [cyan]Retry Policy:[/cyan] {job.retry_policy or 'None'}\n"
        f"  [cyan]Created:[/cyan] {job.created_at.isoformat()}\n"
        f"  [cyan]Updated:[/cyan] {job.updated_at.isoformat()}"
    )

    if job.payload:
        info += f"\n  [cyan]Payload:[/cyan] {json.dumps(job.payload, indent=2)}"

    console.print(Panel(info, title=f"Job: {job.name}"))


@cli.command()
@click.argument("job_identifier")
@click.pass_context
def pause(ctx: click.Context, job_identifier: str) -> None:
    """Pause a scheduled job."""
    scheduler = get_scheduler(ctx.obj["data_dir"])
    job = scheduler.get_job(job_identifier) or scheduler.get_job_by_name(job_identifier)
    if job is None:
        console.print(f"[red]Job not found: {job_identifier}[/red]")
        sys.exit(1)
    scheduler.pause_job(job.id)
    console.print(f"[yellow]Job '{job.name}' paused.[/yellow]")


@cli.command()
@click.argument("job_identifier")
@click.pass_context
def resume(ctx: click.Context, job_identifier: str) -> None:
    """Resume a paused job."""
    scheduler = get_scheduler(ctx.obj["data_dir"])
    job = scheduler.get_job(job_identifier) or scheduler.get_job_by_name(job_identifier)
    if job is None:
        console.print(f"[red]Job not found: {job_identifier}[/red]")
        sys.exit(1)
    scheduler.resume_job(job.id)
    console.print(f"[green]Job '{job.name}' resumed.[/green]")


@cli.command()
@click.argument("job_identifier")
@click.option("--force", is_flag=True, help="Skip confirmation")
@click.pass_context
def delete(ctx: click.Context, job_identifier: str, force: bool) -> None:
    """Delete a scheduled job."""
    scheduler = get_scheduler(ctx.obj["data_dir"])
    job = scheduler.get_job(job_identifier) or scheduler.get_job_by_name(job_identifier)
    if job is None:
        console.print(f"[red]Job not found: {job_identifier}[/red]")
        sys.exit(1)

    if not force:
        if not click.confirm(f"Delete job '{job.name}'?"):
            return

    scheduler.delete_job(job.id)
    console.print(f"[red]Job '{job.name}' deleted.[/red]")


@cli.command()
@click.argument("job_identifier")
@click.pass_context
def run(ctx: click.Context, job_identifier: str) -> None:
    """Manually trigger a job execution."""
    scheduler = get_scheduler(ctx.obj["data_dir"])
    job = scheduler.get_job(job_identifier) or scheduler.get_job_by_name(job_identifier)
    if job is None:
        console.print(f"[red]Job not found: {job_identifier}[/red]")
        sys.exit(1)

    execution = asyncio.run(scheduler.run_job(job.id))
    if execution is None:
        console.print(f"[red]Failed to execute job '{job.name}'.[/red]")
        sys.exit(1)

    if execution.is_success:
        console.print(f"[green]Job '{job.name}' executed successfully![/green]")
        if execution.result:
            console.print(f"  Result: {json.dumps(execution.result, indent=2)}")
    else:
        console.print(f"[red]Job '{job.name}' failed: {execution.error_message}[/red]")


@cli.command()
@click.argument("job_identifier")
@click.option("--limit", default=20, type=int, help="Number of records to show")
@click.pass_context
def history(ctx: click.Context, job_identifier: str, limit: int) -> None:
    """Show execution history for a job."""
    scheduler = get_scheduler(ctx.obj["data_dir"])
    job = scheduler.get_job(job_identifier) or scheduler.get_job_by_name(job_identifier)
    if job is None:
        console.print(f"[red]Job not found: {job_identifier}[/red]")
        sys.exit(1)

    executions = scheduler.get_history(job.id, limit=limit)
    if not executions:
        console.print(f"[dim]No execution history for '{job.name}'.[/dim]")
        return

    table = Table(title=f"Execution History: {job.name}", show_lines=True)
    table.add_column("ID", style="cyan", no_wrap=True)
    table.add_column("Status", style="green")
    table.add_column("Started", style="white")
    table.add_column("Duration", justify="right")
    table.add_column("Attempt", justify="right")
    table.add_column("Error", style="red")

    for exec_record in executions:
        started = exec_record.started_at.strftime("%Y-%m-%d %H:%M:%S")
        duration = f"{exec_record.duration_seconds:.2f}s" if exec_record.duration_seconds else "—"
        status_style = "green" if exec_record.is_success else "red"
        error = (exec_record.error_message or "—")[:50]

        table.add_row(
            exec_record.id,
            f"[{status_style}]{exec_record.status.value}[/{status_style}]",
            started,
            duration,
            str(exec_record.retry_attempt),
            error,
        )

    console.print(table)


@cli.command("run-due")
@click.pass_context
def run_due(ctx: click.Context) -> None:
    """Execute all due jobs now."""
    scheduler = get_scheduler(ctx.obj["data_dir"])
    executions = asyncio.run(scheduler.run_due_jobs())

    if not executions:
        console.print("[dim]No due jobs to execute.[/dim]")
        return

    console.print(f"[green]Executed {len(executions)} job(s):[/green]")
    for exec_record in executions:
        status = "✓" if exec_record.is_success else "✗"
        console.print(f"  {status} {exec_record.job_name}: {exec_record.status.value}")


@cli.command()
@click.pass_context
def stats(ctx: click.Context) -> None:
    """Show scheduler statistics."""
    scheduler = get_scheduler(ctx.obj["data_dir"])
    s = scheduler.get_stats()

    console.print(Panel(
        f"  [cyan]Total Jobs:[/cyan] {s.total_jobs}\n"
        f"  [green]Active:[/green] {s.active_jobs}\n"
        f"  [yellow]Paused:[/yellow] {s.paused_jobs}\n"
        f"  [blue]Completed:[/blue] {s.completed_jobs}\n"
        f"  [red]Failed:[/red] {s.failed_jobs}\n"
        f"  [cyan]Upcoming:[/cyan] {s.upcoming_jobs}\n\n"
        f"  [cyan]Total Executions:[/cyan] {s.total_executions}\n"
        f"  [green]Successful:[/green] {s.successful_executions}\n"
        f"  [red]Failed:[/red] {s.failed_executions}\n\n"
        f"  [cyan]Tags:[/cyan] {', '.join(s.tags) if s.tags else 'None'}",
        title="Scheduler Statistics",
    ))


@cli.command("tags")
@click.pass_context
def list_tags(ctx: click.Context) -> None:
    """List all tags across all jobs."""
    scheduler = get_scheduler(ctx.obj["data_dir"])
    tags = scheduler.list_tags()
    if not tags:
        console.print("[dim]No tags found.[/dim]")
        return
    console.print(", ".join(tags))


@cli.command()
@click.argument("tag")
@click.pass_context
def by_tag(ctx: click.Context, tag: str) -> None:
    """List jobs by tag."""
    scheduler = get_scheduler(ctx.obj["data_dir"])
    jobs = scheduler.get_jobs_by_tag(tag)
    if not jobs:
        console.print(f"[dim]No jobs with tag '{tag}'.[/dim]")
        return
    for job in jobs:
        status = "✓" if job.enabled else "⏸"
        console.print(f"  {status} {job.name} ({job.id}) — {job.status.value}")


@cli.command()
@click.option("--port", default=8080, type=int, help="MCP server port")
@click.pass_context
def serve(ctx: click.Context, port: int) -> None:
    """Start the MCP server."""
    from agent_scheduler.mcp_server import run_mcp_server

    data_dir = ctx.obj["data_dir"]
    console.print(f"[green]Starting MCP server on port {port}...[/green]")
    asyncio.run(run_mcp_server(data_dir=data_dir, port=port))


@cli.command()
@click.pass_context
def start(ctx: click.Context) -> None:
    """Start the scheduler daemon (runs until interrupted)."""
    scheduler = get_scheduler(ctx.obj["data_dir"])
    console.print("[green]Starting scheduler daemon...[/green]")
    console.print("[dim]Press Ctrl+C to stop.[/dim]")

    async def _run() -> None:
        await scheduler.start()
        try:
            while scheduler.is_running:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
        finally:
            await scheduler.stop()

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        console.print("\n[yellow]Scheduler stopped.[/yellow]")


# ── Webhook Commands ───────────────────────────────────────────

@cli.group("webhook")
@click.pass_context
def webhook_group(ctx: click.Context) -> None:
    """Manage webhook notifications."""
    pass


@webhook_group.command("add")
@click.option("--name", required=True, help="Webhook name")
@click.option("--url", required=True, help="Webhook URL to POST to")
@click.option("--secret", default=None, help="HMAC signing secret")
@click.option("--events", default=None, help="Comma-separated event list (default: all)")
@click.option("--tags", default=None, help="Comma-separated job tags to filter (default: all)")
@click.option("--timeout", default=10.0, type=float, help="HTTP timeout in seconds")
@click.option("--max-retries", default=3, type=int, help="Max delivery retries")
@click.pass_context
def webhook_add(
    ctx: click.Context,
    name: str,
    url: str,
    secret: Optional[str],
    events: Optional[str],
    tags: Optional[str],
    timeout: float,
    max_retries: int,
) -> None:
    """Create a new webhook subscription."""
    scheduler = get_scheduler(ctx.obj["data_dir"])
    manager = scheduler.webhooks
    if manager is None:
        console.print("[red]Webhook support not available.[/red]")
        sys.exit(1)

    event_list = None
    if events:
        event_list = [WebhookEvent(e.strip()) for e in events.split(",") if e.strip()]

    tag_list = []
    if tags:
        tag_list = [t.strip() for t in tags.split(",") if t.strip()]

    webhook = Webhook(
        name=name,
        url=url,
        secret=secret,
        events=event_list if event_list else [e for e in WebhookEvent],
        tags=tag_list,
        timeout=timeout,
        max_retries=max_retries,
    )
    manager.create_webhook(webhook)
    console.print(Panel(
        f"[green]Webhook created![/green]\n\n"
        f"  ID: {webhook.id}\n"
        f"  Name: {webhook.name}\n"
        f"  URL: {webhook.url}\n"
        f"  Events: {', '.join(e.value for e in webhook.events)}\n"
        f"  Tags filter: {', '.join(webhook.tags) if webhook.tags else 'All'}\n"
        f"  Max retries: {webhook.max_retries}",
        title="Webhook Added",
    ))


@webhook_group.command("list")
@click.pass_context
def webhook_list(ctx: click.Context) -> None:
    """List all webhook subscriptions."""
    scheduler = get_scheduler(ctx.obj["data_dir"])
    manager = scheduler.webhooks
    if manager is None:
        console.print("[red]Webhook support not available.[/red]")
        sys.exit(1)
    webhooks = manager.list_webhooks()

    if not webhooks:
        console.print("[dim]No webhooks found.[/dim]")
        return

    table = Table(title="Webhook Subscriptions", show_lines=True)
    table.add_column("ID", style="cyan", no_wrap=True)
    table.add_column("Name", style="white")
    table.add_column("URL", style="blue")
    table.add_column("Events", style="magenta")
    table.add_column("Tags", style="yellow")
    table.add_column("Enabled", style="green")
    table.add_column("Retries", justify="right")

    for wh in webhooks:
        events_str = ", ".join(e.value for e in wh.events)
        if len(events_str) > 40:
            events_str = events_str[:37] + "..."
        tags_str = ", ".join(wh.tags) if wh.tags else "All"
        enabled = "[green]✓[/green]" if wh.enabled else "[red]✗[/red]"

        table.add_row(
            wh.id,
            wh.name,
            wh.url[:50] + "..." if len(wh.url) > 50 else wh.url,
            events_str,
            tags_str,
            enabled,
            str(wh.max_retries),
        )

    console.print(table)


@webhook_group.command("delete")
@click.argument("webhook_id")
@click.option("--force", is_flag=True, help="Skip confirmation")
@click.pass_context
def webhook_delete(ctx: click.Context, webhook_id: str, force: bool) -> None:
    """Delete a webhook subscription."""
    scheduler = get_scheduler(ctx.obj["data_dir"])
    manager = scheduler.webhooks
    if manager is None:
        console.print("[red]Webhook support not available.[/red]")
        sys.exit(1)
    webhook = manager.get_webhook(webhook_id)
    if webhook is None:
        console.print(f"[red]Webhook not found: {webhook_id}[/red]")
        sys.exit(1)

    if not force:
        if not click.confirm(f"Delete webhook '{webhook.name}'?"):
            return

    manager.delete_webhook(webhook_id)
    console.print(f"[red]Webhook '{webhook.name}' deleted.[/red]")


@webhook_group.command("deliveries")
@click.option("--webhook-id", default=None, help="Filter by webhook ID")
@click.option("--limit", default=20, type=int, help="Number of records")
@click.pass_context
def webhook_deliveries(ctx: click.Context, webhook_id: Optional[str], limit: int) -> None:
    """Show webhook delivery history."""
    scheduler = get_scheduler(ctx.obj["data_dir"])
    manager = scheduler.webhooks
    if manager is None:
        console.print("[red]Webhook support not available.[/red]")
        sys.exit(1)
    deliveries = manager.get_deliveries(webhook_id=webhook_id, limit=limit)

    if not deliveries:
        console.print("[dim]No webhook deliveries found.[/dim]")
        return

    table = Table(title="Webhook Deliveries", show_lines=True)
    table.add_column("ID", style="cyan", no_wrap=True)
    table.add_column("Webhook", style="white")
    table.add_column("Event", style="magenta")
    table.add_column("Job", style="blue")
    table.add_column("Status", style="green")
    table.add_column("HTTP Code", justify="right")
    table.add_column("Attempt", justify="right")
    table.add_column("Error", style="red")

    for d in deliveries:
        status_style = "green" if d.status.value == "delivered" else "red"
        error = (d.error or "—")[:40]

        table.add_row(
            d.id,
            d.webhook_id,
            d.event.value,
            d.job_name,
            f"[{status_style}]{d.status.value}[/{status_style}]",
            str(d.status_code or "—"),
            str(d.attempt),
            error,
        )

    console.print(table)


# ── Template Commands ──────────────────────────────────────────

@cli.group("template")
@click.pass_context
def template_group(ctx: click.Context) -> None:
    """Manage job templates."""
    pass


@template_group.command("list")
@click.option("--category", default=None, type=click.Choice([c.value for c in TemplateCategory]))
@click.pass_context
def template_list(ctx: click.Context, category: Optional[str]) -> None:
    """List available job templates."""
    manager = TemplateManager(store=JSONJobStore(data_dir=ctx.obj["data_dir"]))
    cat = TemplateCategory(category) if category else None
    templates = manager.list_templates(category=cat)

    if not templates:
        console.print("[dim]No templates found.[/dim]")
        return

    table = Table(title="Job Templates", show_lines=True)
    table.add_column("Name", style="cyan")
    table.add_column("Category", style="magenta")
    table.add_column("Handler", style="blue")
    table.add_column("Description", style="white")
    table.add_column("Default Cron", style="green")
    table.add_column("Priority", style="yellow")
    table.add_column("Required Fields", style="red")

    for t in templates:
        table.add_row(
            t.name,
            t.category.value,
            t.handler,
            t.description[:50] + "..." if len(t.description) > 50 else t.description,
            t.default_cron or "—",
            t.default_priority.value,
            ", ".join(t.required_fields) if t.required_fields else "—",
        )

    console.print(table)


@template_group.command("show")
@click.argument("template_name")
@click.pass_context
def template_show(ctx: click.Context, template_name: str) -> None:
    """Show detailed information about a template."""
    manager = TemplateManager(store=JSONJobStore(data_dir=ctx.obj["data_dir"]))
    template = manager.get_template_by_name(template_name)
    if template is None:
        console.print(f"[red]Template not found: {template_name}[/red]")
        sys.exit(1)

    info = (
        f"  [cyan]Name:[/cyan] {template.name}\n"
        f"  [cyan]Description:[/cyan] {template.description}\n"
        f"  [cyan]Category:[/cyan] {template.category.value}\n"
        f"  [cyan]Handler:[/cyan] {template.handler}\n"
        f"  [cyan]Default Cron:[/cyan] {template.default_cron or 'N/A'}\n"
        f"  [cyan]Default Priority:[/cyan] {template.default_priority.value}\n"
        f"  [cyan]Default Timeout:[/cyan] {template.default_timeout}s\n"
        f"  [cyan]Default Tags:[/cyan] {', '.join(template.default_tags) or 'None'}\n"
        f"  [cyan]Default Max Retries:[/cyan] {template.default_max_retries}\n"
        f"  [cyan]Default Payload:[/cyan] {json.dumps(template.default_payload, indent=2)}\n"
        f"  [cyan]Required Fields:[/cyan] {', '.join(template.required_fields) or 'None'}\n"
        f"  [cyan]Optional Fields:[/cyan] {', '.join(template.optional_fields) or 'None'}"
    )

    console.print(Panel(info, title=f"Template: {template.name}"))


@template_group.command("use")
@click.argument("template_name")
@click.option("--name", default=None, help="Job name (default: auto-generated)")
@click.option("--cron", default=None, help="Override cron expression")
@click.option("--priority", default=None, type=click.Choice(["low", "normal", "high"]))
@click.option("--payload", default=None, help="JSON payload overrides")
@click.option("--tags", default=None, help="Additional comma-separated tags")
@click.pass_context
def template_use(
    ctx: click.Context,
    template_name: str,
    name: Optional[str],
    cron: Optional[str],
    priority: Optional[str],
    payload: Optional[str],
    tags: Optional[str],
) -> None:
    """Create a job from a template."""
    scheduler = get_scheduler(ctx.obj["data_dir"])
    manager = TemplateManager(store=JSONJobStore(data_dir=ctx.obj["data_dir"]))

    # Build overrides
    overrides: dict = {}
    if name:
        overrides["name"] = name
    if cron:
        overrides["cron"] = cron
    if priority:
        overrides["priority"] = Priority(priority)
    if tags:
        overrides["tags"] = [t.strip() for t in tags.split(",") if t.strip()]
    if payload:
        try:
            overrides["payload"] = json.loads(payload)
        except json.JSONDecodeError as e:
            console.print(f"[red]Invalid JSON payload: {e}[/red]")
            sys.exit(1)

    try:
        job = manager.instantiate(template_name, **overrides)
    except ValueError as e:
        console.print(f"[red]{e}[/red]")
        sys.exit(1)

    scheduler.add_job(job)
    console.print(Panel(
        f"[green]Job created from template '{template_name}'![/green]\n\n"
        f"  ID: {job.id}\n"
        f"  Name: {job.name}\n"
        f"  Handler: {job.handler}\n"
        f"  Cron: {job.cron or 'N/A'}\n"
        f"  Priority: {job.priority.value}",
        title="Job Created from Template",
    ))


@template_group.command("add")
@click.option("--name", required=True, help="Template name")
@click.option("--handler", required=True, help="Default handler function")
@click.option("--description", default="", help="Template description")
@click.option("--category", default="custom", type=click.Choice([c.value for c in TemplateCategory]))
@click.option("--cron", default=None, help="Default cron expression")
@click.option("--priority", default="normal", type=click.Choice(["low", "normal", "high"]))
@click.option("--timeout", default=300, type=float, help="Default timeout")
@click.option("--max-retries", default=0, type=int, help="Default max retries")
@click.option("--tags", default=None, help="Default comma-separated tags")
@click.option("--required-fields", default=None, help="Comma-separated required field names")
@click.option("--payload", default=None, help="Default JSON payload")
@click.pass_context
def template_add(
    ctx: click.Context,
    name: str,
    handler: str,
    description: str,
    category: str,
    cron: Optional[str],
    priority: str,
    timeout: float,
    max_retries: int,
    tags: Optional[str],
    required_fields: Optional[str],
    payload: Optional[str],
) -> None:
    """Create a new custom job template."""
    manager = TemplateManager(store=JSONJobStore(data_dir=ctx.obj["data_dir"]))

    tag_list = []
    if tags:
        tag_list = [t.strip() for t in tags.split(",") if t.strip()]

    required = []
    if required_fields:
        required = [f.strip() for f in required_fields.split(",") if f.strip()]

    default_payload = {}
    if payload:
        try:
            default_payload = json.loads(payload)
        except json.JSONDecodeError as e:
            console.print(f"[red]Invalid JSON payload: {e}[/red]")
            sys.exit(1)

    template = JobTemplate(
        name=name,
        description=description,
        category=TemplateCategory(category),
        handler=handler,
        default_cron=cron,
        default_priority=Priority(priority),
        default_timeout=timeout,
        default_max_retries=max_retries,
        default_tags=tag_list,
        default_payload=default_payload,
        required_fields=required,
    )
    manager.create_template(template)
    console.print(Panel(
        f"[green]Template created![/green]\n\n"
        f"  ID: {template.id}\n"
        f"  Name: {template.name}\n"
        f"  Handler: {template.handler}\n"
        f"  Category: {template.category.value}",
        title="Template Added",
    ))


# ── API Server Command ─────────────────────────────────────────

@cli.command("api")
@click.option("--host", default="0.0.0.0", help="Bind host")
@click.option("--port", default=8080, type=int, help="Bind port")
@click.option("--sqlite", is_flag=True, help="Use SQLite backend")
@click.option("--auth", is_flag=True, help="Enable API key authentication")
@click.pass_context
def api_server(ctx: click.Context, host: str, port: int, sqlite: bool, auth: bool) -> None:
    """Start the REST API server."""
    from agent_scheduler.api import run_api_server

    data_dir = ctx.obj["data_dir"]
    backend = "SQLite" if sqlite or auth else "JSON"
    console.print(f"[green]Starting REST API server on {host}:{port}...[/green]")
    console.print(f"[dim]Backend: {backend} | Auth: {'enabled' if auth else 'disabled'}[/dim]")
    console.print("[dim]Press Ctrl+C to stop.[/dim]")

    try:
        asyncio.run(run_api_server(host=host, port=port, data_dir=data_dir, use_sqlite=sqlite, enable_auth=auth))
    except KeyboardInterrupt:
        console.print("\n[yellow]API server stopped.[/yellow]")


# ── Group Commands ──────────────────────────────────────────────

@cli.group("group")
@click.pass_context
def group_group(ctx: click.Context) -> None:
    """Manage job groups for multi-agent scheduling."""
    pass


@group_group.command("create")
@click.option("--name", required=True, help="Group name")
@click.option("--description", default="", help="Group description")
@click.option("--tags", default=None, help="Comma-separated default tags")
@click.option("--max-jobs", default=None, type=int, help="Max jobs quota")
@click.option("--max-concurrent", default=None, type=int, help="Max concurrent executions")
@click.pass_context
def group_create(ctx: click.Context, name: str, description: str, tags: Optional[str], max_jobs: Optional[int], max_concurrent: Optional[int]) -> None:
    """Create a new job group."""
    from agent_scheduler.groups import GroupManager, GroupQuota
    scheduler = get_scheduler(ctx.obj["data_dir"])
    manager = GroupManager(store=scheduler.store, scheduler=scheduler)

    tag_list = [t.strip() for t in tags.split(",") if t.strip()] if tags else []
    quota = None
    if max_jobs is not None or max_concurrent is not None:
        quota = GroupQuota(max_jobs=max_jobs, max_concurrent=max_concurrent)

    try:
        group = manager.create_group(name=name, description=description, tags=tag_list, quota=quota)
        console.print(Panel(
            f"[green]Group created![/green]\n\n"
            f"  ID: {group.id}\n"
            f"  Name: {group.name}\n"
            f"  Tags: {', '.join(group.tags) or 'None'}\n"
            f"  Max Jobs: {group.quota.max_jobs or 'Unlimited'}",
            title="Job Group",
        ))
    except ValueError as e:
        console.print(f"[red]{e}[/red]")
        sys.exit(1)


@group_group.command("list")
@click.pass_context
def group_list(ctx: click.Context) -> None:
    """List all job groups."""
    from agent_scheduler.groups import GroupManager
    scheduler = get_scheduler(ctx.obj["data_dir"])
    manager = GroupManager(store=scheduler.store, scheduler=scheduler)
    groups = manager.list_groups()

    if not groups:
        console.print("[dim]No groups found.[/dim]")
        return

    table = Table(title="Job Groups", show_lines=True)
    table.add_column("ID", style="cyan", no_wrap=True)
    table.add_column("Name", style="white")
    table.add_column("Description", style="dim")
    table.add_column("Enabled", style="green")
    table.add_column("Max Jobs", style="yellow")

    for g in groups:
        table.add_row(g.id, g.name, g.description or "—", "✓" if g.enabled else "✗", str(g.quota.max_jobs or "∞"))
    console.print(table)


@group_group.command("show")
@click.argument("identifier")
@click.pass_context
def group_show(ctx: click.Context, identifier: str) -> None:
    """Show group details and stats."""
    from agent_scheduler.groups import GroupManager
    scheduler = get_scheduler(ctx.obj["data_dir"])
    manager = GroupManager(store=scheduler.store, scheduler=scheduler)
    group = manager.get_group(identifier)
    if group is None:
        console.print(f"[red]Group not found: {identifier}[/red]")
        sys.exit(1)

    stats = manager.get_stats(identifier)
    info = (
        f"  [cyan]ID:[/cyan] {group.id}\n"
        f"  [cyan]Name:[/cyan] {group.name}\n"
        f"  [cyan]Description:[/cyan] {group.description or 'None'}\n"
        f"  [cyan]Enabled:[/cyan] {group.enabled}\n"
        f"  [cyan]Tags:[/cyan] {', '.join(group.tags) or 'None'}\n"
        f"  [cyan]Max Jobs:[/cyan] {group.quota.max_jobs or 'Unlimited'}\n"
    )
    if stats:
        info += (
            f"\n  [green]Jobs:[/green] {stats.total_jobs} (active: {stats.active_jobs}, paused: {stats.paused_jobs})\n"
            f"  [green]Executions:[/green] {stats.total_executions} (ok: {stats.successful_executions}, fail: {stats.failed_executions})\n"
            f"  [yellow]Quota Usage:[/yellow] {stats.quota_usage_pct}%"
        )
    console.print(Panel(info, title=f"Group: {group.name}"))


@group_group.command("pause")
@click.argument("identifier")
@click.pass_context
def group_pause(ctx: click.Context, identifier: str) -> None:
    """Pause all jobs in a group."""
    from agent_scheduler.groups import GroupManager
    scheduler = get_scheduler(ctx.obj["data_dir"])
    manager = GroupManager(store=scheduler.store, scheduler=scheduler)
    count = manager.pause_group(identifier)
    console.print(f"[yellow]Paused {count} job(s) in group.[/yellow]")


@group_group.command("resume")
@click.argument("identifier")
@click.pass_context
def group_resume(ctx: click.Context, identifier: str) -> None:
    """Resume all paused jobs in a group."""
    from agent_scheduler.groups import GroupManager
    scheduler = get_scheduler(ctx.obj["data_dir"])
    manager = GroupManager(store=scheduler.store, scheduler=scheduler)
    count = manager.resume_group(identifier)
    console.print(f"[green]Resumed {count} job(s) in group.[/green]")


@group_group.command("delete")
@click.argument("identifier")
@click.option("--force", is_flag=True, help="Skip confirmation")
@click.pass_context
def group_delete(ctx: click.Context, identifier: str, force: bool) -> None:
    """Delete a job group (keeps jobs)."""
    from agent_scheduler.groups import GroupManager
    scheduler = get_scheduler(ctx.obj["data_dir"])
    manager = GroupManager(store=scheduler.store, scheduler=scheduler)

    if not force:
        if not click.confirm("Delete this group? (Jobs will be kept)"):
            return

    if manager.delete_group(identifier):
        console.print("[red]Group deleted.[/red]")
    else:
        console.print(f"[red]Group not found: {identifier}[/red]")
        sys.exit(1)


# ── Analytics Commands (v0.4.0) ──────────────────────────────


@cli.command("analytics")
@click.pass_context
def analytics_dashboard(ctx: click.Context) -> None:
    """Show execution analytics and health dashboard."""
    from agent_scheduler.analytics import AnalyticsEngine

    scheduler = get_scheduler(ctx.obj["data_dir"])
    engine = AnalyticsEngine(scheduler=scheduler)
    report = engine.dashboard()

    # Overall health
    health_color = {
        "A": "bright_green", "B": "green", "C": "yellow",
        "D": "red", "F": "bright_red",
    }.get(report.overall_health_grade, "white")

    console.print("\n[bold cyan]Scheduler Analytics Dashboard[/bold cyan]")
    console.print(f"\nOverall Health: [{health_color}]{report.overall_health_grade}[/{health_color}] "
                  f"({report.overall_health_score}/100)")
    console.print(f"Success Rate: {report.overall_success_rate}%")
    console.print(f"Total Executions: {report.total_executions}")

    # Period table
    period_table = Table(title="Execution Summary")
    period_table.add_column("Period", style="cyan")
    period_table.add_column("Total", justify="right")
    period_table.add_column("Success", justify="right", style="green")
    period_table.add_column("Failed", justify="right", style="red")
    period_table.add_row("Last 24h", str(report.last_24h.get("total", 0)),
                         str(report.last_24h.get("success", 0)),
                         str(report.last_24h.get("failed", 0)))
    period_table.add_row("Last 7d", str(report.last_7d.get("total", 0)),
                         str(report.last_7d.get("success", 0)),
                         str(report.last_7d.get("failed", 0)))
    period_table.add_row("All Time", str(report.total_executions),
                         str(report.successful_executions),
                         str(report.failed_executions))
    console.print(period_table)

    # Duration stats
    if report.duration_stats.count > 0:
        ds = report.duration_stats
        dur_table = Table(title="Duration Statistics")
        dur_table.add_column("Metric", style="cyan")
        dur_table.add_column("Value", justify="right")
        dur_table.add_row("Count", str(ds.count))
        dur_table.add_row("Average", f"{ds.avg_seconds:.4f}s")
        dur_table.add_row("Median", f"{ds.median_seconds:.4f}s")
        dur_table.add_row("P95", f"{ds.p95_seconds:.4f}s")
        dur_table.add_row("P99", f"{ds.p99_seconds:.4f}s")
        dur_table.add_row("Min", f"{ds.min_seconds:.4f}s")
        dur_table.add_row("Max", f"{ds.max_seconds:.4f}s")
        console.print(dur_table)

    # Healthiest / unhealthiest
    if report.unhealthiest_jobs:
        uh_table = Table(title="Unhealthiest Jobs")
        uh_table.add_column("Job", style="red")
        uh_table.add_column("Health", justify="right")
        uh_table.add_column("Grade", justify="center")
        uh_table.add_column("Success Rate", justify="right")
        uh_table.add_column("Executions", justify="right")
        for r in report.unhealthiest_jobs:
            uh_table.add_row(r.job_name, f"{r.health_score}", r.health_grade,
                             f"{r.success_rate}%", str(r.total_executions))
        console.print(uh_table)

    if report.healthiest_jobs:
        h_table = Table(title="Healthiest Jobs")
        h_table.add_column("Job", style="green")
        h_table.add_column("Health", justify="right")
        h_table.add_column("Grade", justify="center")
        h_table.add_column("Success Rate", justify="right")
        h_table.add_column("Executions", justify="right")
        for r in report.healthiest_jobs:
            h_table.add_row(r.job_name, f"{r.health_score}", r.health_grade,
                            f"{r.success_rate}%", str(r.total_executions))
        console.print(h_table)

    # Failure patterns
    if report.top_failures:
        f_table = Table(title="Top Failure Patterns")
        f_table.add_column("#", justify="right", style="dim")
        f_table.add_column("Error", style="red")
        f_table.add_column("Count", justify="right")
        f_table.add_column("Jobs Affected", justify="right")
        for i, fp in enumerate(report.top_failures, 1):
            f_table.add_row(str(i), fp.error[:80], str(fp.count), str(len(fp.affected_jobs)))
        console.print(f_table)

    # At-risk and stale
    if report.at_risk_jobs:
        console.print(f"\n[red]⚠ At-risk jobs (health < 50): {', '.join(report.at_risk_jobs)}[/red]")
    if report.stale_jobs:
        console.print(f"[yellow]⚠ Stale jobs (not running): {', '.join(report.stale_jobs)}[/yellow]")


@cli.command("health")
@click.argument("job_identifier")
@click.pass_context
def job_health(ctx: click.Context, job_identifier: str) -> None:
    """Show health report for a specific job."""
    from agent_scheduler.analytics import AnalyticsEngine

    scheduler = get_scheduler(ctx.obj["data_dir"])
    job = scheduler.get_job(job_identifier) or scheduler.get_job_by_name(job_identifier)
    if not job:
        console.print(f"[red]Job not found: {job_identifier}[/red]")
        sys.exit(1)

    engine = AnalyticsEngine(scheduler=scheduler)
    report = engine.job_report(job)

    health_color = {
        "A": "bright_green", "B": "green", "C": "yellow",
        "D": "red", "F": "bright_red",
    }.get(report.health_grade, "white")

    console.print(f"\n[bold cyan]Health Report: {job.name}[/bold cyan]\n")
    console.print(f"Health Score: [{health_color}]{report.health_grade}[/{health_color}] "
                  f"({report.health_score}/100)")
    console.print(f"Status: {report.status.value}")
    console.print(f"Total Executions: {report.total_executions}")
    console.print(f"Success Rate: {report.success_rate}%")
    console.print(f"Successful: {report.successful_executions} | Failed: {report.failed_executions} | Retries: {report.retry_count}")
    if report.avg_duration_seconds is not None:
        console.print(f"Avg Duration: {report.avg_duration_seconds:.4f}s")
    if report.last_run_at:
        console.print(f"Last Run: {report.last_run_at.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    if report.last_error:
        console.print(f"[red]Last Error: {report.last_error}[/red]")
    if report.last_5_statuses:
        console.print(f"Last 5 Runs: {' → '.join(report.last_5_statuses)}")
    if report.is_stale:
        console.print("[yellow]⚠ This job appears stale (not running as expected)[/yellow]")


# ── Cron Helper Commands (v0.4.0) ────────────────────────────


@cli.group("cron")
@click.pass_context
def cron_group(ctx: click.Context) -> None:
    """Cron expression utilities."""
    pass


@cron_group.command("validate")
@click.argument("expression")
def cron_validate(ctx: click.Context, expression: str) -> None:
    """Validate a cron expression."""
    from agent_scheduler.cron_helper import validate_cron
    result = validate_cron(expression)
    if result.is_valid:
        console.print(f"[green]✓ Valid cron expression: {expression}[/green]")
    else:
        console.print(f"[red]✗ Invalid: {result.error}[/red]")
        sys.exit(1)


@cron_group.command("describe")
@click.argument("expression")
def cron_describe(ctx: click.Context, expression: str) -> None:
    """Describe a cron expression in human-readable English."""
    from agent_scheduler.cron_helper import describe_cron
    description = describe_cron(expression)
    console.print(f"[cyan]{expression}[/cyan] → {description}")


@cron_group.command("preview")
@click.argument("expression")
@click.option("--count", default=5, type=int, help="Number of upcoming runs to show")
def cron_preview(ctx: click.Context, expression: str, count: int) -> None:
    """Preview the next N run times for a cron expression."""
    from agent_scheduler.cron_helper import preview_runs, validate_cron

    validation = validate_cron(expression)
    if not validation.is_valid:
        console.print(f"[red]Invalid cron: {validation.error}[/red]")
        sys.exit(1)

    runs = preview_runs(expression, n=count)
    console.print(f"\n[bold]Next {count} runs for '{expression}':[/bold]\n")
    for i, run_time in enumerate(runs, 1):
        console.print(f"  {i}. {run_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")


@cron_group.command("build")
@click.option("--frequency",
              type=click.Choice([
                  "every-minute", "every-n-minutes", "hourly", "every-n-hours",
                  "daily", "weekly", "weekdays", "weekends", "monthly",
              ]),
              required=True,
              help="Schedule frequency")
@click.option("--hour", default=0, type=int, help="Hour (0-23)")
@click.option("--minute", default=0, type=int, help="Minute (0-59)")
@click.option("--day", default=None, help="Day (day-of-week name or day-of-month number)")
@click.option("--n", default=None, type=int, help="Interval N for every-N patterns")
def cron_build(
    ctx: click.Context,
    frequency: str,
    hour: int,
    minute: int,
    day: Optional[str],
    n: Optional[int],
) -> None:
    """Build a cron expression from parameters."""
    from agent_scheduler.cron_helper import suggest_cron

    kwargs: dict[str, Any] = {"hour": hour, "minute": minute}
    if day:
        kwargs["day"] = day
    if n:
        kwargs["n"] = n

    try:
        expression = suggest_cron(frequency, **kwargs)
        console.print(f"[green]{expression}[/green]")
        # Also show description
        from agent_scheduler.cron_helper import describe_cron
        desc = describe_cron(expression)
        console.print(f"[dim]({desc})[/dim]")
    except ValueError as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


# ── DLQ Commands (v0.5.0) ─────────────────────────────────────


@cli.group()
@click.pass_context
def dlq(ctx: click.Context) -> None:
    """Dead Letter Queue management for failed jobs."""
    pass


@dlq.command(name="list")
@click.option("--unresolved-only", is_flag=True, help="Show only unresolved entries")
@click.option("--reason", type=click.Choice(["max_retries_exhausted", "timeout", "handler_not_found", "manual"]), default=None)
@click.option("--limit", default=20, type=int, help="Max entries to show")
@click.pass_context
def dlq_list(ctx: click.Context, unresolved_only: bool, reason: Optional[str], limit: int) -> None:
    """List dead-lettered jobs."""
    from agent_scheduler.dlq import DLQReason
    scheduler = get_scheduler(ctx.obj["data_dir"])
    if scheduler.dlq is None:
        console.print("[yellow]DLQ is not enabled.[/yellow]")
        return

    reason_enum = DLQReason(reason) if reason else None
    entries = scheduler.dlq.list_entries(unresolved_only=unresolved_only, reason=reason_enum, limit=limit)

    if not entries:
        console.print("[dim]No dead-lettered jobs.[/dim]")
        return

    table = Table(title="Dead Letter Queue")
    table.add_column("ID", style="cyan", no_wrap=True)
    table.add_column("Job", style="white")
    table.add_column("Reason", style="yellow")
    table.add_column("Error", style="red")
    table.add_column("Retries", justify="right")
    table.add_column("Status")

    for entry in entries:
        error_short = (entry.error_message[:60] + "...") if len(entry.error_message) > 60 else entry.error_message
        status_str = "[green]resolved[/green]" if entry.resolved else "[red]unresolved[/red]"
        table.add_row(
            entry.id,
            entry.job_name,
            entry.reason.value,
            error_short,
            str(entry.retry_attempts),
            status_str,
        )

    console.print(table)
    console.print(f"\n[dim]Total: {scheduler.dlq.count()} ({scheduler.dlq.count(unresolved_only=True)} unresolved)[/dim]")


@dlq.command(name="show")
@click.argument("entry_id")
@click.pass_context
def dlq_show(ctx: click.Context, entry_id: str) -> None:
    """Show details of a specific DLQ entry."""
    scheduler = get_scheduler(ctx.obj["data_dir"])
    if scheduler.dlq is None:
        console.print("[yellow]DLQ is not enabled.[/yellow]")
        return

    entry = scheduler.dlq.get(entry_id)
    if entry is None:
        console.print(f"[red]DLQ entry '{entry_id}' not found.[/red]")
        sys.exit(1)

    info_lines = [
        f"[cyan]ID:[/cyan] {entry.id}",
        f"[cyan]Job:[/cyan] {entry.job_name} ({entry.job_id})",
        f"[cyan]Handler:[/cyan] {entry.handler}",
        f"[cyan]Reason:[/cyan] [yellow]{entry.reason.value}[/yellow]",
        f"[cyan]Error:[/cyan] [red]{entry.error_message}[/red]",
        f"[cyan]Retry Attempts:[/cyan] {entry.retry_attempts}",
        f"[cyan]Created:[/cyan] {entry.created_at}",
        f"[cyan]Status:[/cyan] {'resolved' if entry.resolved else 'unresolved'}",
    ]
    if entry.resolution:
        info_lines.append(f"[cyan]Resolution:[/cyan] {entry.resolution}")
    if entry.payload:
        info_lines.append(f"[cyan]Payload:[/cyan] {json.dumps(entry.payload, indent=2)}")

    console.print(Panel("\n".join(info_lines), title=f"DLQ Entry — {entry.job_name}"))


@dlq.command(name="replay")
@click.argument("entry_id")
@click.option("--payload", default=None, help="JSON payload override")
@click.pass_context
def dlq_replay(ctx: click.Context, entry_id: str, payload: Optional[str]) -> None:
    """Replay a dead-lettered job."""
    scheduler = get_scheduler(ctx.obj["data_dir"])
    if scheduler.dlq is None:
        console.print("[yellow]DLQ is not enabled.[/yellow]")
        return

    payload_override = None
    if payload:
        try:
            payload_override = json.loads(payload)
        except json.JSONDecodeError as e:
            console.print(f"[red]Invalid JSON: {e}[/red]")
            sys.exit(1)

    job = scheduler.dlq.replay(entry_id, payload_override=payload_override)
    if job is None:
        console.print(f"[red]Entry '{entry_id}' not found.[/red]")
        sys.exit(1)

    console.print(f"[green]✓[/green] Replayed job '{job.name}' (id={job.id}) — rescheduled for execution")


@dlq.command(name="discard")
@click.argument("entry_id")
@click.pass_context
def dlq_discard(ctx: click.Context, entry_id: str) -> None:
    """Discard a DLQ entry (mark resolved without replaying)."""
    scheduler = get_scheduler(ctx.obj["data_dir"])
    if scheduler.dlq is None:
        console.print("[yellow]DLQ is not enabled.[/yellow]")
        return

    if scheduler.dlq.discard(entry_id):
        console.print(f"[green]✓[/green] Discarded DLQ entry '{entry_id}'")
    else:
        console.print(f"[red]Entry '{entry_id}' not found.[/red]")
        sys.exit(1)


@dlq.command(name="stats")
@click.pass_context
def dlq_stats(ctx: click.Context) -> None:
    """Show DLQ statistics."""
    scheduler = get_scheduler(ctx.obj["data_dir"])
    if scheduler.dlq is None:
        console.print("[yellow]DLQ is not enabled.[/yellow]")
        return

    stats = scheduler.dlq.get_stats()

    table = Table(title="Dead Letter Queue — Statistics")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", justify="right")
    table.add_row("Total Entries", str(stats.total_entries))
    table.add_row("Unresolved", str(stats.unresolved))
    table.add_row("Resolved", str(stats.resolved))

    if stats.by_reason:
        for reason, count in sorted(stats.by_reason.items()):
            table.add_row(f"  {reason}", str(count))

    if stats.oldest_unresolved_age_seconds is not None:
        table.add_row("Oldest Unresolved (s)", f"{stats.oldest_unresolved_age_seconds:.1f}")

    console.print(table)


@dlq.command(name="purge")
@click.option("--all", "purge_all", is_flag=True, help="Purge ALL entries (including unresolved)")
@click.pass_context
def dlq_purge(ctx: click.Context, purge_all: bool) -> None:
    """Remove resolved DLQ entries from storage."""
    scheduler = get_scheduler(ctx.obj["data_dir"])
    if scheduler.dlq is None:
        console.print("[yellow]DLQ is not enabled.[/yellow]")
        return

    purged = scheduler.dlq.purge(resolved_only=not purge_all)
    if purge_all:
        console.print(f"[yellow]⚠[/yellow] Purged {purged} entries (ALL)")
    else:
        console.print(f"[green]✓[/green] Purged {purged} resolved entries")


# ── Pipeline Commands (v0.5.0) ────────────────────────────────


@cli.group()
@click.pass_context
def pipeline(ctx: click.Context) -> None:
    """Pipeline management for multi-step job chains."""
    pass


@pipeline.command(name="create")
@click.option("--name", required=True, help="Pipeline name")
@click.option("--description", default="", help="Pipeline description")
@click.pass_context
def pipeline_create(ctx: click.Context, name: str, description: str) -> None:
    """Create a new pipeline."""
    from agent_scheduler.result_chain import ResultChainManager
    scheduler = get_scheduler(ctx.obj["data_dir"])
    if scheduler.result_chains is None:
        console.print("[yellow]Result chaining is not enabled.[/yellow]")
        return

    p = scheduler.result_chains.create_pipeline(name=name, description=description)
    console.print(f"[green]✓[/green] Created pipeline '{p.name}' (id={p.id})")


@pipeline.command(name="list")
@click.pass_context
def pipeline_list(ctx: click.Context) -> None:
    """List all pipelines."""
    scheduler = get_scheduler(ctx.obj["data_dir"])
    if scheduler.result_chains is None:
        console.print("[yellow]Result chaining is not enabled.[/yellow]")
        return

    pipelines = scheduler.result_chains.list_pipelines()
    if not pipelines:
        console.print("[dim]No pipelines.[/dim]")
        return

    table = Table(title="Pipelines")
    table.add_column("ID", style="cyan")
    table.add_column("Name", style="white")
    table.add_column("Steps", justify="right")
    table.add_column("Started")
    table.add_column("Completed")

    for p in pipelines:
        started = "[green]yes[/green]" if p.started else "[dim]no[/dim]"
        completed = "[green]yes[/green]" if p.completed else "[dim]no[/dim]"
        table.add_row(p.id, p.name, str(p.step_count), started, completed)

    console.print(table)


@pipeline.command(name="show")
@click.argument("pipeline_id")
@click.pass_context
def pipeline_show(ctx: click.Context, pipeline_id: str) -> None:
    """Show pipeline details including steps and status."""
    scheduler = get_scheduler(ctx.obj["data_dir"])
    if scheduler.result_chains is None:
        console.print("[yellow]Result chaining is not enabled.[/yellow]")
        return

    p = scheduler.result_chains.get_pipeline(pipeline_id) or scheduler.result_chains.get_pipeline_by_name(pipeline_id)
    if p is None:
        console.print(f"[red]Pipeline '{pipeline_id}' not found.[/red]")
        sys.exit(1)

    info = [
        f"[cyan]ID:[/cyan] {p.id}",
        f"[cyan]Name:[/cyan] {p.name}",
        f"[cyan]Description:[/cyan] {p.description}",
        f"[cyan]Steps:[/cyan] {p.step_count}",
        f"[cyan]Started:[/cyan] {'yes' if p.started else 'no'}",
        f"[cyan]Completed:[/cyan] {'yes' if p.completed else 'no'}",
    ]
    console.print(Panel("\n".join(info), title=f"Pipeline — {p.name}"))

    if p.steps:
        table = Table(title="Pipeline Steps")
        table.add_column("#", style="dim", justify="right")
        table.add_column("Step Name", style="white")
        table.add_column("Job ID", style="cyan")
        table.add_column("Chaining")

        for step in p.steps:
            chaining = "[green]yes[/green]" if step.result_config else "[dim]no[/dim]"
            table.add_row(str(step.step_index), step.step_name, step.job_id, chaining)

        console.print(table)

    # Show status if started
    status = scheduler.result_chains.get_pipeline_status(p.id)
    if status:
        console.print(f"\n[cyan]Progress:[/cyan] {status.progress_pct}% ({status.completed_steps}/{status.total_steps})")
        if status.current_step:
            console.print(f"[cyan]Current Step:[/cyan] {status.current_step}")


@pipeline.command(name="add-step")
@click.argument("pipeline_id")
@click.option("--job-id", required=True, help="Job ID for this step")
@click.option("--name", default="", help="Human-readable step name")
@click.option("--merge", type=click.Choice(["merge", "child_first", "replace", "prefix"]), default="merge")
@click.option("--keys", default=None, help="Comma-separated result keys to extract from parent")
@click.pass_context
def pipeline_add_step(
    ctx: click.Context,
    pipeline_id: str,
    job_id: str,
    name: str,
    merge: str,
    keys: Optional[str],
) -> None:
    """Add a step to a pipeline."""
    from agent_scheduler.result_chain import ResultConfig, ResultMergeStrategy
    scheduler = get_scheduler(ctx.obj["data_dir"])
    if scheduler.result_chains is None:
        console.print("[yellow]Result chaining is not enabled.[/yellow]")
        return

    config = None
    if merge or keys:
        result_keys = [k.strip() for k in keys.split(",")] if keys else None
        config = ResultConfig(
            merge_strategy=ResultMergeStrategy(merge),
            result_keys=result_keys,
        )

    step = scheduler.result_chains.add_step(pipeline_id, job_id, name, config)
    if step is None:
        console.print(f"[red]Pipeline '{pipeline_id}' not found.[/red]")
        sys.exit(1)

    console.print(f"[green]✓[/green] Added step '{step.step_name}' (index={step.step_index}) to pipeline")


@pipeline.command(name="delete")
@click.argument("pipeline_id")
@click.pass_context
def pipeline_delete(ctx: click.Context, pipeline_id: str) -> None:
    """Delete a pipeline."""
    scheduler = get_scheduler(ctx.obj["data_dir"])
    if scheduler.result_chains is None:
        console.print("[yellow]Result chaining is not enabled.[/yellow]")
        return

    if scheduler.result_chains.delete_pipeline(pipeline_id):
        console.print(f"[green]✓[/green] Deleted pipeline '{pipeline_id}'")
    else:
        console.print(f"[red]Pipeline '{pipeline_id}' not found.[/red]")
        sys.exit(1)


@cli.group()
@click.pass_context
def chain(ctx: click.Context) -> None:
    """Configure result chaining between jobs."""
    pass


@chain.command(name="link")
@click.option("--parent", required=True, help="Parent job ID")
@click.option("--child", required=True, help="Child job ID")
@click.option("--merge", type=click.Choice(["merge", "child_first", "replace", "prefix"]), default="merge")
@click.option("--keys", default=None, help="Comma-separated result keys to pass from parent")
@click.option("--prefix", default="parent_", help="Key prefix for 'prefix' strategy")
@click.pass_context
def chain_link(
    ctx: click.Context,
    parent: str,
    child: str,
    merge: str,
    keys: Optional[str],
    prefix: str,
) -> None:
    """Configure how a parent job's result flows into a child job."""
    from agent_scheduler.result_chain import ResultConfig, ResultMergeStrategy
    scheduler = get_scheduler(ctx.obj["data_dir"])
    if scheduler.result_chains is None:
        console.print("[yellow]Result chaining is not enabled.[/yellow]")
        return

    result_keys = [k.strip() for k in keys.split(",")] if keys else None
    config = ResultConfig(
        merge_strategy=ResultMergeStrategy(merge),
        result_keys=result_keys,
        key_prefix=prefix,
    )
    scheduler.result_chains.configure_link(parent, child, config)
    console.print(f"[green]✓[/green] Linked {parent} → {child} (merge={merge})")


@chain.command(name="list")
@click.pass_context
def chain_list(ctx: click.Context) -> None:
    """List all result chain links."""
    scheduler = get_scheduler(ctx.obj["data_dir"])
    if scheduler.result_chains is None:
        console.print("[yellow]Result chaining is not enabled.[/yellow]")
        return

    links = scheduler.result_chains.list_links()
    if not links:
        console.print("[dim]No chain links configured.[/dim]")
        return

    table = Table(title="Result Chain Links")
    table.add_column("Parent", style="cyan")
    table.add_column("Child", style="cyan")
    table.add_column("Strategy", style="yellow")
    table.add_column("Keys")

    for link in links:
        config = link["config"]
        keys = ", ".join(config.get("result_keys", [])) if config.get("result_keys") else "[dim]all[/dim]"
        table.add_row(
            link["parent_job_id"],
            link["child_job_id"],
            config.get("merge_strategy", "merge"),
            keys,
        )

    console.print(table)


@chain.command(name="unlink")
@click.option("--parent", required=True, help="Parent job ID")
@click.option("--child", required=True, help="Child job ID")
@click.pass_context
def chain_unlink(ctx: click.Context, parent: str, child: str) -> None:
    """Remove a result chain link."""
    scheduler = get_scheduler(ctx.obj["data_dir"])
    if scheduler.result_chains is None:
        console.print("[yellow]Result chaining is not enabled.[/yellow]")
        return

    if scheduler.result_chains.remove_link(parent, child):
        console.print(f"[green]✓[/green] Removed link {parent} → {child}")
    else:
        console.print(f"[red]Link not found.[/red]")
        sys.exit(1)


if __name__ == "__main__":
    cli()
