"""CLI interface for agent-scheduler."""

from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

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


if __name__ == "__main__":
    cli()
