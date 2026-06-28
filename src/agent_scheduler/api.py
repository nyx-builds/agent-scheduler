"""REST API server for agent-scheduler.

Provides an HTTP API for remote agent integration,
complementing the MCP server (which uses stdio).
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Optional

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


def create_app(scheduler: Scheduler, webhook_manager: Optional[WebhookManager] = None) -> Any:
    """Create a Starlette/FastAPI-like ASGI app.

    Uses a lightweight ASGI framework approach — no heavy dependencies.
    Supports JSON request/response, CORS headers, and proper HTTP methods.
    """
    try:
        from starlette.applications import Starlette
        from starlette.routing import Route
        from starlette.responses import JSONResponse
        from starlette.requests import Request
        _HAS_STARLETTE = True
    except ImportError:
        _HAS_STARLETTE = False

    if _HAS_STARLETTE:
        return _create_starlette_app(scheduler, webhook_manager)
    else:
        return _create_raw_asgi_app(scheduler, webhook_manager)


def _job_to_dict(job: Job) -> dict[str, Any]:
    """Convert a Job to a JSON-serializable dict."""
    d = job.model_dump(mode="json")
    d["is_recurring"] = job.is_recurring
    d["is_one_time"] = job.is_one_time
    d["is_immediate"] = job.is_immediate
    return d


# ── Starlette-based implementation ─────────────────────────────

def _create_starlette_app(scheduler: Scheduler, webhook_manager: Optional[WebhookManager] = None) -> Any:
    from starlette.applications import Starlette
    from starlette.routing import Route
    from starlette.responses import JSONResponse
    from starlette.requests import Request

    async def _resolve_job(job_identifier: str) -> Optional[Job]:
        job = scheduler.get_job(job_identifier)
        if job is None:
            job = scheduler.get_job_by_name(job_identifier)
        return job

    # ── Jobs ───────────────────────────────────────────────

    async def list_jobs(request: Request) -> JSONResponse:
        tag = request.query_params.get("tag")
        status_str = request.query_params.get("status")
        enabled_only = request.query_params.get("enabled_only", "").lower() in ("true", "1", "yes")
        status = JobStatus(status_str) if status_str else None
        jobs = scheduler.list_jobs(enabled_only=enabled_only, tag=tag, status=status)
        return JSONResponse({"jobs": [_job_to_dict(j) for j in jobs], "count": len(jobs)})

    async def create_job(request: Request) -> JSONResponse:
        body = await request.json()
        retry_policy = None
        if body.get("max_retries", 0) > 0:
            retry_policy = RetryPolicy(max_retries=body["max_retries"])
        run_at_dt = None
        if body.get("run_at"):
            run_at_dt = datetime.fromisoformat(body["run_at"])
            if run_at_dt.tzinfo is None:
                run_at_dt = run_at_dt.replace(tzinfo=timezone.utc)
        job = Job(
            name=body["name"],
            handler=body["handler"],
            payload=body.get("payload", {}),
            cron=body.get("cron"),
            delay=body.get("delay"),
            run_at=run_at_dt,
            priority=Priority(body.get("priority", "normal")),
            retry_policy=retry_policy,
            timeout=body.get("timeout", 300),
            max_runs=body.get("max_runs"),
            tags=body.get("tags", []),
        )
        scheduler.add_job(job)

        # Fire webhook
        if webhook_manager:
            await webhook_manager.fire_event(WebhookEvent.JOB_CREATED, job)

        return JSONResponse({"job": _job_to_dict(job), "message": f"Job '{job.name}' created"}, status_code=201)

    async def get_job(request: Request) -> JSONResponse:
        job_identifier = request.path_params["job_identifier"]
        job = await _resolve_job(job_identifier)
        if job is None:
            return JSONResponse({"error": f"Job not found: {job_identifier}"}, status_code=404)
        return JSONResponse({"job": _job_to_dict(job)})

    async def update_job(request: Request) -> JSONResponse:
        job_identifier = request.path_params["job_identifier"]
        job = await _resolve_job(job_identifier)
        if job is None:
            return JSONResponse({"error": f"Job not found: {job_identifier}"}, status_code=404)
        body = await request.json()
        updates = {k: v for k, v in body.items() if v is not None}
        if "priority" in updates:
            updates["priority"] = Priority(updates["priority"])
        updated = scheduler.update_job(job.id, **updates)
        if updated is None:
            return JSONResponse({"error": "Failed to update job"}, status_code=500)
        return JSONResponse({"job": _job_to_dict(updated), "message": f"Job '{updated.name}' updated"})

    async def delete_job(request: Request) -> JSONResponse:
        job_identifier = request.path_params["job_identifier"]
        job = await _resolve_job(job_identifier)
        if job is None:
            return JSONResponse({"error": f"Job not found: {job_identifier}"}, status_code=404)

        # Fire webhook before deleting
        if webhook_manager:
            await webhook_manager.fire_event(WebhookEvent.JOB_DELETED, job)

        scheduler.delete_job(job.id)
        return JSONResponse({"message": f"Job '{job.name}' deleted"})

    async def pause_job(request: Request) -> JSONResponse:
        job_identifier = request.path_params["job_identifier"]
        job = await _resolve_job(job_identifier)
        if job is None:
            return JSONResponse({"error": f"Job not found: {job_identifier}"}, status_code=404)
        scheduler.pause_job(job.id)

        if webhook_manager:
            await webhook_manager.fire_event(WebhookEvent.JOB_PAUSED, job)

        return JSONResponse({"message": f"Job '{job.name}' paused"})

    async def resume_job(request: Request) -> JSONResponse:
        job_identifier = request.path_params["job_identifier"]
        job = await _resolve_job(job_identifier)
        if job is None:
            return JSONResponse({"error": f"Job not found: {job_identifier}"}, status_code=404)
        scheduler.resume_job(job.id)

        if webhook_manager:
            await webhook_manager.fire_event(WebhookEvent.JOB_RESUMED, job)

        return JSONResponse({"message": f"Job '{job.name}' resumed"})

    async def run_job(request: Request) -> JSONResponse:
        job_identifier = request.path_params["job_identifier"]
        job = await _resolve_job(job_identifier)
        if job is None:
            return JSONResponse({"error": f"Job not found: {job_identifier}"}, status_code=404)
        execution = await scheduler.run_job(job.id)
        if execution is None:
            return JSONResponse({"error": "Failed to execute job"}, status_code=500)
        return JSONResponse({"execution": execution.model_dump(mode="json")})

    # ── History & Stats ────────────────────────────────────

    async def get_history(request: Request) -> JSONResponse:
        job_id = None
        job_identifier = request.query_params.get("job_identifier")
        if job_identifier:
            job = await _resolve_job(job_identifier)
            if job:
                job_id = job.id
            else:
                return JSONResponse({"error": f"Job not found: {job_identifier}"}, status_code=404)
        limit = int(request.query_params.get("limit", "50"))
        executions = scheduler.get_history(job_id=job_id, limit=limit)
        return JSONResponse({
            "executions": [e.model_dump(mode="json") for e in executions],
            "count": len(executions),
        })

    async def get_stats(request: Request) -> JSONResponse:
        stats = scheduler.get_stats()
        return JSONResponse({"stats": stats.model_dump(mode="json")})

    async def get_next_run(request: Request) -> JSONResponse:
        job_identifier = request.path_params["job_identifier"]
        job = await _resolve_job(job_identifier)
        if job is None:
            return JSONResponse({"error": f"Job not found: {job_identifier}"}, status_code=404)
        next_run = scheduler.get_next_run(job.id)
        return JSONResponse({"next_run_at": next_run.isoformat() if next_run else None})

    # ── Tags ───────────────────────────────────────────────

    async def list_tags(request: Request) -> JSONResponse:
        tags = scheduler.list_tags()
        return JSONResponse({"tags": tags})

    async def get_jobs_by_tag(request: Request) -> JSONResponse:
        tag = request.path_params["tag"]
        jobs = scheduler.get_jobs_by_tag(tag)
        return JSONResponse({"jobs": [_job_to_dict(j) for j in jobs], "count": len(jobs)})

    # ── Dependencies ───────────────────────────────────────

    async def create_dependency(request: Request) -> JSONResponse:
        body = await request.json()
        job = await _resolve_job(body.get("job_identifier", ""))
        depends_on = await _resolve_job(body.get("depends_on_identifier", ""))
        if job is None:
            return JSONResponse({"error": f"Job not found: {body.get('job_identifier')}"}, status_code=404)
        if depends_on is None:
            return JSONResponse({"error": f"Dependency job not found: {body.get('depends_on_identifier')}"}, status_code=404)
        on_status = ExecutionStatus(body.get("on_status", "success"))
        dep = scheduler.add_dependency(job.id, depends_on.id, on_status)
        return JSONResponse({"dependency": dep.model_dump(mode="json")}, status_code=201)

    async def get_dependencies(request: Request) -> JSONResponse:
        job_identifier = request.path_params["job_identifier"]
        job = await _resolve_job(job_identifier)
        if job is None:
            return JSONResponse({"error": f"Job not found: {job_identifier}"}, status_code=404)
        deps = scheduler.get_dependencies(job.id)
        return JSONResponse({"dependencies": [d.model_dump(mode="json") for d in deps]})

    # ── Webhooks ───────────────────────────────────────────

    async def list_webhooks(request: Request) -> JSONResponse:
        if webhook_manager is None:
            return JSONResponse({"error": "Webhook support not enabled"}, status_code=501)
        enabled_only = request.query_params.get("enabled_only", "").lower() in ("true", "1", "yes")
        webhooks = webhook_manager.list_webhooks(enabled_only=enabled_only)
        return JSONResponse({
            "webhooks": [w.model_dump(mode="json") for w in webhooks],
            "count": len(webhooks),
        })

    async def create_webhook(request: Request) -> JSONResponse:
        if webhook_manager is None:
            return JSONResponse({"error": "Webhook support not enabled"}, status_code=501)
        body = await request.json()
        events = [WebhookEvent(e) for e in body.get("events", [e.value for e in WebhookEvent])]
        webhook = Webhook(
            name=body["name"],
            url=body["url"],
            secret=body.get("secret"),
            events=events,
            tags=body.get("tags", []),
            headers=body.get("headers", {}),
            timeout=body.get("timeout", 10.0),
            max_retries=body.get("max_retries", 3),
        )
        webhook_manager.create_webhook(webhook)
        return JSONResponse({"webhook": webhook.model_dump(mode="json")}, status_code=201)

    async def delete_webhook(request: Request) -> JSONResponse:
        if webhook_manager is None:
            return JSONResponse({"error": "Webhook support not enabled"}, status_code=501)
        webhook_id = request.path_params["webhook_id"]
        deleted = webhook_manager.delete_webhook(webhook_id)
        if not deleted:
            return JSONResponse({"error": f"Webhook not found: {webhook_id}"}, status_code=404)
        return JSONResponse({"message": f"Webhook {webhook_id} deleted"})

    async def get_webhook_deliveries(request: Request) -> JSONResponse:
        if webhook_manager is None:
            return JSONResponse({"error": "Webhook support not enabled"}, status_code=501)
        webhook_id = request.query_params.get("webhook_id")
        limit = int(request.query_params.get("limit", "50"))
        deliveries = webhook_manager.get_deliveries(webhook_id=webhook_id, limit=limit)
        return JSONResponse({
            "deliveries": [d.model_dump(mode="json") for d in deliveries],
            "count": len(deliveries),
        })

    # ── Run Due ────────────────────────────────────────────

    async def run_due(request: Request) -> JSONResponse:
        executions = await scheduler.run_due_jobs()
        return JSONResponse({
            "executions": [e.model_dump(mode="json") for e in executions],
            "count": len(executions),
        })

    # ── Health ─────────────────────────────────────────────

    async def health(request: Request) -> JSONResponse:
        return JSONResponse({"status": "ok", "service": "agent-scheduler", "version": "0.2.0"})

    routes = [
        Route("/health", health),
        Route("/api/v1/jobs", list_jobs, methods=["GET"]),
        Route("/api/v1/jobs", create_job, methods=["POST"]),
        Route("/api/v1/jobs/{job_identifier}", get_job, methods=["GET"]),
        Route("/api/v1/jobs/{job_identifier}", update_job, methods=["PATCH"]),
        Route("/api/v1/jobs/{job_identifier}", delete_job, methods=["DELETE"]),
        Route("/api/v1/jobs/{job_identifier}/pause", pause_job, methods=["POST"]),
        Route("/api/v1/jobs/{job_identifier}/resume", resume_job, methods=["POST"]),
        Route("/api/v1/jobs/{job_identifier}/run", run_job, methods=["POST"]),
        Route("/api/v1/jobs/{job_identifier}/next-run", get_next_run, methods=["GET"]),
        Route("/api/v1/jobs/{job_identifier}/dependencies", get_dependencies, methods=["GET"]),
        Route("/api/v1/history", get_history, methods=["GET"]),
        Route("/api/v1/stats", get_stats, methods=["GET"]),
        Route("/api/v1/tags", list_tags, methods=["GET"]),
        Route("/api/v1/tags/{tag}", get_jobs_by_tag, methods=["GET"]),
        Route("/api/v1/dependencies", create_dependency, methods=["POST"]),
        Route("/api/v1/run-due", run_due, methods=["POST"]),
        Route("/api/v1/webhooks", list_webhooks, methods=["GET"]),
        Route("/api/v1/webhooks", create_webhook, methods=["POST"]),
        Route("/api/v1/webhooks/{webhook_id}", delete_webhook, methods=["DELETE"]),
        Route("/api/v1/webhook-deliveries", get_webhook_deliveries, methods=["GET"]),
    ]

    app = Starlette(routes=routes)
    return app


# ── Lightweight raw ASGI fallback (no starlette) ───────────────

def _create_raw_asgi_app(scheduler: Scheduler, webhook_manager: Optional[WebhookManager] = None) -> Any:
    """Create a minimal ASGI app without starlette dependency."""

    class RawASGIApp:
        def __init__(self, scheduler: Scheduler, webhook_manager: Optional[WebhookManager]) -> None:
            self.scheduler = scheduler
            self.webhook_manager = webhook_manager

        async def __call__(self, scope: dict, receive: callable, send: callable) -> None:  # type: ignore[type-arg]
            if scope["type"] != "http":
                return

            path = scope.get("path", "")
            method = scope.get("method", "")

            # Read body
            body = b""
            while True:
                message = await receive()
                body += message.get("body", b"")
                if not message.get("more_body", False):
                    break

            # Parse query params
            query_string = scope.get("query_string", b"").decode()
            params = dict(
                pair.split("=", 1) if "=" in pair else (pair, "")
                for pair in query_string.split("&")
                if pair
            ) if query_string else {}

            # Route
            response = await self._route(path, method, body, params)
            await self._send_response(send, response["status"], response["body"])

        async def _route(self, path: str, method: str, body: bytes, params: dict) -> dict:
            try:
                if path == "/health":
                    return {"status": 200, "body": {"status": "ok", "service": "agent-scheduler", "version": "0.2.0"}}

                if path == "/api/v1/jobs" and method == "GET":
                    tag = params.get("tag")
                    status_str = params.get("status")
                    enabled_only = params.get("enabled_only", "").lower() in ("true", "1")
                    status = JobStatus(status_str) if status_str else None
                    jobs = self.scheduler.list_jobs(enabled_only=enabled_only, tag=tag, status=status)
                    return {"status": 200, "body": {"jobs": [_job_to_dict(j) for j in jobs], "count": len(jobs)}}

                if path == "/api/v1/jobs" and method == "POST":
                    data = json.loads(body) if body else {}
                    retry_policy = None
                    if data.get("max_retries", 0) > 0:
                        retry_policy = RetryPolicy(max_retries=data["max_retries"])
                    run_at_dt = None
                    if data.get("run_at"):
                        run_at_dt = datetime.fromisoformat(data["run_at"])
                        if run_at_dt.tzinfo is None:
                            run_at_dt = run_at_dt.replace(tzinfo=timezone.utc)
                    job = Job(
                        name=data["name"],
                        handler=data["handler"],
                        payload=data.get("payload", {}),
                        cron=data.get("cron"),
                        delay=data.get("delay"),
                        run_at=run_at_dt,
                        priority=Priority(data.get("priority", "normal")),
                        retry_policy=retry_policy,
                        timeout=data.get("timeout", 300),
                        max_runs=data.get("max_runs"),
                        tags=data.get("tags", []),
                    )
                    self.scheduler.add_job(job)
                    return {"status": 201, "body": {"job": _job_to_dict(job), "message": f"Job '{job.name}' created"}}

                if path == "/api/v1/stats" and method == "GET":
                    stats = self.scheduler.get_stats()
                    return {"status": 200, "body": {"stats": stats.model_dump(mode="json")}}

                if path == "/api/v1/tags" and method == "GET":
                    tags = self.scheduler.list_tags()
                    return {"status": 200, "body": {"tags": tags}}

                if path == "/api/v1/run-due" and method == "POST":
                    executions = await self.scheduler.run_due_jobs()
                    return {"status": 200, "body": {"executions": [e.model_dump(mode="json") for e in executions], "count": len(executions)}}

                if path == "/api/v1/history" and method == "GET":
                    job_identifier = params.get("job_identifier")
                    job_id = None
                    if job_identifier:
                        job = self.scheduler.get_job(job_identifier) or self.scheduler.get_job_by_name(job_identifier)
                        if job:
                            job_id = job.id
                    limit = int(params.get("limit", "50"))
                    executions = self.scheduler.get_history(job_id=job_id, limit=limit)
                    return {"status": 200, "body": {"executions": [e.model_dump(mode="json") for e in executions], "count": len(executions)}}

                return {"status": 404, "body": {"error": f"Not found: {method} {path}"}}
            except Exception as e:
                return {"status": 500, "body": {"error": str(e)}}

        async def _send_response(self, send: callable, status: int, body: dict) -> None:
            body_bytes = json.dumps(body, default=str).encode()
            await send({
                "type": "http.response.start",
                "status": status,
                "headers": [
                    [b"content-type", b"application/json"],
                    [b"access-control-allow-origin", b"*"],
                ],
            })
            await send({
                "type": "http.response.body",
                "body": body_bytes,
            })

    return RawASGIApp(scheduler, webhook_manager)


async def run_api_server(
    host: str = "0.0.0.0",
    port: int = 8080,
    data_dir: Optional[str] = None,
) -> None:
    """Run the REST API server."""
    import uvicorn

    store = JSONJobStore(data_dir=data_dir)
    scheduler = Scheduler(store=store)
    webhook_manager = WebhookManager(store=store)
    app = create_app(scheduler, webhook_manager)

    import logging as _logging
    _logger = _logging.getLogger(__name__)
    _logger.info(f"Starting agent-scheduler REST API on {host}:{port}")

    config = uvicorn.Config(app, host=host, port=port, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()
