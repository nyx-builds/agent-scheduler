"""Webhook notifications for agent-scheduler.

Fires HTTP callbacks when jobs complete, fail, timeout, or are retried.
Supports configurable retry, HMAC signatures, and event filtering.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

import httpx
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class WebhookEvent(str, Enum):
    """Events that can trigger a webhook."""

    JOB_CREATED = "job.created"
    JOB_COMPLETED = "job.completed"
    JOB_FAILED = "job.failed"
    JOB_TIMEOUT = "job.timeout"
    JOB_RETRY = "job.retry"
    JOB_PAUSED = "job.paused"
    JOB_RESUMED = "job.resumed"
    JOB_CANCELLED = "job.cancelled"
    JOB_DELETED = "job.deleted"


class WebhookStatus(str, Enum):
    """Delivery status of a webhook attempt."""

    PENDING = "pending"
    DELIVERED = "delivered"
    FAILED = "failed"


class Webhook(BaseModel):
    """A webhook subscription — defines a URL and which events to listen for."""

    id: str = Field(default_factory=lambda: uuid.uuid4().hex[:12])
    name: str = Field(..., min_length=1, description="Human-readable webhook name")
    url: str = Field(..., min_length=1, description="HTTP(S) URL to POST events to")
    secret: Optional[str] = Field(
        default=None,
        description="HMAC-SHA256 signing secret for payload verification",
    )
    events: list[WebhookEvent] = Field(
        default_factory=lambda: [e for e in WebhookEvent],
        description="Events to listen for (default: all)",
    )
    tags: list[str] = Field(
        default_factory=list,
        description="Only fire for jobs with these tags (empty = all jobs)",
    )
    enabled: bool = Field(default=True, description="Whether this webhook is active")
    headers: dict[str, str] = Field(
        default_factory=dict, description="Custom HTTP headers to send"
    )
    timeout: float = Field(default=10.0, ge=1.0, description="HTTP request timeout in seconds")
    max_retries: int = Field(default=3, ge=0, description="Max delivery retry attempts")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def matches_event(self, event: WebhookEvent) -> bool:
        """Check if this webhook subscribes to the given event."""
        return event in self.events

    def matches_tags(self, job_tags: list[str]) -> bool:
        """Check if this webhook should fire for a job with the given tags."""
        if not self.tags:
            return True  # No tag filter = match all
        return any(t in job_tags for t in self.tags)


class WebhookDelivery(BaseModel):
    """Record of a single webhook delivery attempt."""

    id: str = Field(default_factory=lambda: uuid.uuid4().hex[:12])
    webhook_id: str = Field(..., description="ID of the webhook")
    event: WebhookEvent = Field(..., description="Event that triggered delivery")
    job_id: str = Field(..., description="ID of the related job")
    job_name: str = Field(..., description="Name of the related job")
    payload: dict[str, Any] = Field(..., description="JSON payload sent")
    status: WebhookStatus = Field(default=WebhookStatus.PENDING)
    status_code: Optional[int] = Field(default=None, description="HTTP response status code")
    response_body: Optional[str] = Field(default=None, description="Response body (truncated)")
    error: Optional[str] = Field(default=None, description="Error message if delivery failed")
    attempt: int = Field(default=1, ge=1, description="Delivery attempt number")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    delivered_at: Optional[datetime] = Field(default=None)


def build_webhook_payload(
    event: WebhookEvent,
    job: Any,
    execution: Any = None,
    extra: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Build the JSON payload for a webhook delivery."""
    payload: dict[str, Any] = {
        "event": event.value,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "job": {
            "id": job.id,
            "name": job.name,
            "handler": job.handler,
            "status": job.status.value if hasattr(job.status, "value") else str(job.status),
            "tags": job.tags,
            "priority": job.priority.value if hasattr(job.priority, "value") else str(job.priority),
        },
    }
    if execution:
        payload["execution"] = {
            "id": execution.id,
            "status": execution.status.value if hasattr(execution.status, "value") else str(execution.status),
            "started_at": execution.started_at.isoformat() if execution.started_at else None,
            "finished_at": execution.finished_at.isoformat() if execution.finished_at else None,
            "duration_seconds": execution.duration_seconds,
            "error_message": execution.error_message,
            "retry_attempt": execution.retry_attempt,
        }
    if extra:
        payload.update(extra)
    return payload


def sign_payload(payload: dict[str, Any], secret: str) -> str:
    """Create HMAC-SHA256 signature for a webhook payload."""
    body = json.dumps(payload, sort_keys=True, default=str)
    return hmac.new(secret.encode(), body.encode(), hashlib.sha256).hexdigest()


class WebhookManager:
    """Manages webhook subscriptions and delivery."""

    def __init__(self, store: Any = None) -> None:
        # Store will be set externally to avoid circular imports
        self._store = store
        self._client: Optional[httpx.AsyncClient] = None

    def set_store(self, store: Any) -> None:
        """Set the webhook store."""
        self._store = store

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=30.0)
        return self._client

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    # ── Subscription CRUD ──────────────────────────────────────

    def create_webhook(self, webhook: Webhook) -> Webhook:
        """Create a new webhook subscription."""
        if self._store is None:
            raise RuntimeError("Webhook store not configured")
        self._store.save_webhook(webhook)
        logger.info(f"Created webhook '{webhook.name}' (id={webhook.id}, url={webhook.url})")
        return webhook

    def get_webhook(self, webhook_id: str) -> Optional[Webhook]:
        """Get a webhook by ID."""
        if self._store is None:
            raise RuntimeError("Webhook store not configured")
        return self._store.get_webhook(webhook_id)

    def update_webhook(self, webhook_id: str, **updates: Any) -> Optional[Webhook]:
        """Update a webhook subscription."""
        if self._store is None:
            raise RuntimeError("Webhook store not configured")
        webhook = self._store.get_webhook(webhook_id)
        if webhook is None:
            return None
        for key, value in updates.items():
            if hasattr(webhook, key):
                setattr(webhook, key, value)
        self._store.save_webhook(webhook)
        return webhook

    def delete_webhook(self, webhook_id: str) -> bool:
        """Delete a webhook subscription."""
        if self._store is None:
            raise RuntimeError("Webhook store not configured")
        return self._store.delete_webhook(webhook_id)

    def list_webhooks(self, enabled_only: bool = False) -> list[Webhook]:
        """List all webhook subscriptions."""
        if self._store is None:
            raise RuntimeError("Webhook store not configured")
        webhooks = self._store.list_webhooks()
        if enabled_only:
            webhooks = [w for w in webhooks if w.enabled]
        return webhooks

    # ── Event Firing ───────────────────────────────────────────

    async def fire_event(
        self,
        event: WebhookEvent,
        job: Any,
        execution: Any = None,
        extra: Optional[dict[str, Any]] = None,
    ) -> list[WebhookDelivery]:
        """Fire a webhook event to all matching subscriptions."""
        if self._store is None:
            return []

        webhooks = self._store.list_webhooks()
        matching = [
            w for w in webhooks
            if w.enabled and w.matches_event(event) and w.matches_tags(job.tags)
        ]

        deliveries = []
        for webhook in matching:
            delivery = await self._deliver(webhook, event, job, execution, extra)
            deliveries.append(delivery)

        return deliveries

    async def _deliver(
        self,
        webhook: Webhook,
        event: WebhookEvent,
        job: Any,
        execution: Any = None,
        extra: Optional[dict[str, Any]] = None,
    ) -> WebhookDelivery:
        """Deliver a webhook with retry logic."""
        payload = build_webhook_payload(event, job, execution, extra)
        signature = sign_payload(payload, webhook.secret) if webhook.secret else None

        headers = {
            "Content-Type": "application/json",
            "X-Webhook-Event": event.value,
            "X-Webhook-Delivery": "",  # Set below
            "X-Webhook-Signature": signature or "",
        }
        headers.update(webhook.headers)

        delivery = WebhookDelivery(
            webhook_id=webhook.id,
            event=event,
            job_id=job.id,
            job_name=job.name,
            payload=payload,
        )
        headers["X-Webhook-Delivery"] = delivery.id

        if signature:
            headers["X-Webhook-Signature"] = f"sha256={signature}"

        last_error = None
        for attempt in range(1, webhook.max_retries + 1):
            delivery.attempt = attempt
            try:
                client = await self._get_client()
                response = await client.post(
                    webhook.url,
                    json=payload,
                    headers=headers,
                    timeout=webhook.timeout,
                )
                delivery.status_code = response.status_code
                delivery.response_body = response.text[:500]

                if 200 <= response.status_code < 300:
                    delivery.status = WebhookStatus.DELIVERED
                    delivery.delivered_at = datetime.now(timezone.utc)
                    self._save_delivery(delivery)
                    logger.info(
                        f"Webhook '{webhook.name}' delivered (status={response.status_code})"
                    )
                    return delivery
                else:
                    last_error = f"HTTP {response.status_code}: {response.text[:200]}"
            except httpx.TimeoutException:
                last_error = f"Timeout after {webhook.timeout}s"
            except Exception as e:
                last_error = str(e)

            logger.warning(
                f"Webhook '{webhook.name}' delivery attempt {attempt} failed: {last_error}"
            )

        delivery.status = WebhookStatus.FAILED
        delivery.error = last_error
        self._save_delivery(delivery)
        logger.error(f"Webhook '{webhook.name}' delivery failed after {webhook.max_retries} attempts")
        return delivery

    def _save_delivery(self, delivery: WebhookDelivery) -> None:
        """Save a delivery record."""
        if self._store is not None:
            self._store.save_webhook_delivery(delivery)

    def get_deliveries(
        self,
        webhook_id: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[WebhookDelivery]:
        """Get webhook delivery history."""
        if self._store is None:
            return []
        return self._store.get_webhook_deliveries(webhook_id=webhook_id, limit=limit, offset=offset)

    def get_delivery(self, delivery_id: str) -> Optional[WebhookDelivery]:
        """Get a specific delivery by ID."""
        if self._store is None:
            return None
        return self._store.get_webhook_delivery(delivery_id)

    async def retry_delivery(self, delivery_id: str) -> Optional[WebhookDelivery]:
        """Retry a failed delivery."""
        if self._store is None:
            return None
        delivery = self._store.get_webhook_delivery(delivery_id)
        if delivery is None:
            return None
        webhook = self._store.get_webhook(delivery.webhook_id)
        if webhook is None:
            return None

        # Re-derive job and execution info from the stored payload
        job_info = delivery.payload.get("job", {})
        exec_info = delivery.payload.get("execution")

        class _JobStub:
            def __init__(self, data: dict) -> None:
                self.id = data.get("id", "")
                self.name = data.get("name", "")
                self.handler = data.get("handler", "")
                self.status = data.get("status", "")
                self.tags = data.get("tags", [])
                self.priority = data.get("priority", "normal")

        class _ExecStub:
            def __init__(self, data: dict) -> None:
                self.id = data.get("id", "")
                self.status = data.get("status", "")
                self.started_at = None
                self.finished_at = None
                self.duration_seconds = data.get("duration_seconds")
                self.error_message = data.get("error_message")
                self.retry_attempt = data.get("retry_attempt", 0)

        job = _JobStub(job_info)
        execution = _ExecStub(exec_info) if exec_info else None

        return await self._deliver(webhook, delivery.event, job, execution)
