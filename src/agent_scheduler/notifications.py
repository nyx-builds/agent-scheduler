"""Notification channels for agent-scheduler.

Built-in notification integrations that go beyond raw webhooks:
- **Slack** — Send messages to Slack channels via Incoming Webhooks
- **Discord** — Send messages to Discord channels via Webhook URLs
- **Email** — Send notification emails via SMTP
- **Generic HTTP** — POST to any URL (similar to webhooks but simpler)
- **Channel manager** — Register multiple channels and route events to all
"""

from __future__ import annotations

import asyncio
import json
import logging
import smtplib
import ssl
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from enum import Enum
from typing import Any, Optional, Protocol
from urllib.parse import urlparse

from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)


class NotificationLevel(str, Enum):
    """Severity level for notifications."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    SUCCESS = "success"

    @property
    def slack_emoji(self) -> str:
        return {
            NotificationLevel.INFO: "ℹ️",
            NotificationLevel.WARNING: "⚠️",
            NotificationLevel.ERROR: "🚨",
            NotificationLevel.SUCCESS: "✅",
        }.get(self, "📢")

    @property
    def discord_color(self) -> int:
        """Discord embed color as decimal int."""
        return {
            NotificationLevel.INFO: 0x3498DB,      # Blue
            NotificationLevel.WARNING: 0xF39C12,    # Orange
            NotificationLevel.ERROR: 0xE74C3C,      # Red
            NotificationLevel.SUCCESS: 0x2ECC71,    # Green
        }.get(self, 0x95A5A6)  # Grey


class Notification(BaseModel):
    """A notification to be sent via one or more channels."""

    title: str = Field(..., min_length=1, description="Notification title/summary")
    message: str = Field(default="", description="Detailed message body")
    level: NotificationLevel = Field(default=NotificationLevel.INFO)
    job_name: Optional[str] = Field(default=None, description="Related job name")
    job_id: Optional[str] = Field(default=None, description="Related job ID")
    event_type: Optional[str] = Field(default=None, description="Event that triggered this (job.completed, etc.)")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: dict[str, Any] = Field(default_factory=dict, description="Extra context data")


class DeliveryResult(BaseModel):
    """Result of sending a notification."""

    channel_name: str
    channel_type: str
    success: bool
    error: Optional[str] = Field(default=None)
    sent_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class NotificationChannel(Protocol):
    """Protocol for notification channels."""

    name: str
    channel_type: str

    async def send(self, notification: Notification) -> DeliveryResult:
        ...


# ── Slack Channel ───────────────────────────────────────────────


class SlackChannel:
    """Send notifications to Slack via Incoming Webhooks.

    Uses httpx (already a dependency) for async HTTP requests.
    """

    def __init__(
        self,
        webhook_url: str,
        name: str = "slack",
        channel: Optional[str] = None,
        timeout: float = 10.0,
    ) -> None:
        self.webhook_url = webhook_url
        self.name = name
        self.channel_type = "slack"
        self._channel = channel
        self._timeout = timeout

    def _build_payload(self, notification: Notification) -> dict[str, Any]:
        """Build Slack message payload."""
        blocks: list[dict[str, Any]] = []

        # Header block
        blocks.append({
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{notification.level.slack_emoji} {notification.title}",
            },
        })

        # Message context
        context_items: list[dict[str, Any]] = []
        if notification.job_name:
            context_items.append({"type": "mrkdwn", "text": f"*Job:* `{notification.job_name}`"})
        if notification.event_type:
            context_items.append({"type": "mrkdwn", "text": f"*Event:* `{notification.event_type}`"})
        context_items.append({
            "type": "mrkdwn",
            "text": f"*Time:* {notification.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}",
        })
        if context_items:
            blocks.append({"type": "context", "elements": context_items})

        # Body section
        if notification.message:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": notification.message},
            })

        # Metadata as fields
        if notification.metadata:
            fields = []
            for key, value in list(notification.metadata.items())[:10]:
                fields.append({
                    "type": "mrkdwn",
                    "text": f"*{key}:*\n{value}",
                })
            if fields:
                blocks.append({"type": "section", "fields": fields})

        payload: dict[str, Any] = {"blocks": blocks}
        if self._channel:
            payload["channel"] = self._channel
        return payload

    async def send(self, notification: Notification) -> DeliveryResult:
        """Send notification to Slack."""
        try:
            import httpx

            payload = self._build_payload(notification)
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                response = await client.post(self.webhook_url, json=payload)

            if response.status_code == 200 and "ok" in response.text.lower():
                return DeliveryResult(
                    channel_name=self.name,
                    channel_type=self.channel_type,
                    success=True,
                )
            else:
                return DeliveryResult(
                    channel_name=self.name,
                    channel_type=self.channel_type,
                    success=False,
                    error=f"Slack returned {response.status_code}: {response.text[:200]}",
                )
        except Exception as e:
            if "httpx" in str(type(e).__module__):
                return DeliveryResult(
                    channel_name=self.name,
                    channel_type=self.channel_type,
                    success=False,
                    error=f"HTTP error: {e}",
                )
            return DeliveryResult(
                channel_name=self.name,
                channel_type=self.channel_type,
                success=False,
                error=str(e),
            )


# ── Discord Channel ─────────────────────────────────────────────


class DiscordChannel:
    """Send notifications to Discord via Webhook URLs.

    Uses rich embeds for formatted messages.
    """

    def __init__(
        self,
        webhook_url: str,
        name: str = "discord",
        timeout: float = 10.0,
    ) -> None:
        self.webhook_url = webhook_url
        self.name = name
        self.channel_type = "discord"
        self._timeout = timeout

    def _build_payload(self, notification: Notification) -> dict[str, Any]:
        """Build Discord webhook payload with embed."""
        embed: dict[str, Any] = {
            "title": notification.title,
            "description": notification.message or None,
            "color": notification.level.discord_color,
            "timestamp": notification.timestamp.isoformat(),
        }

        # Add fields
        fields: list[dict[str, Any]] = []
        if notification.job_name:
            fields.append({"name": "Job", "value": notification.job_name, "inline": True})
        if notification.event_type:
            fields.append({"name": "Event", "value": notification.event_type, "inline": True})

        for key, value in list(notification.metadata.items())[:20]:
            fields.append({"name": key, "value": str(value)[:1024], "inline": True})

        if fields:
            embed["fields"] = fields

        # Footer
        embed["footer"] = {"text": "Agent Scheduler"}

        payload: dict[str, Any] = {"embeds": [embed]}
        if notification.level == NotificationLevel.ERROR:
            payload["content"] = "@here"  # Mention for errors
        return payload

    async def send(self, notification: Notification) -> DeliveryResult:
        """Send notification to Discord."""
        try:
            import httpx

            payload = self._build_payload(self._clean_notification(notification))
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                response = await client.post(self.webhook_url, json=payload)

            # Discord returns 204 No Content on success
            if response.status_code in (200, 204):
                return DeliveryResult(
                    channel_name=self.name,
                    channel_type=self.channel_type,
                    success=True,
                )
            else:
                return DeliveryResult(
                    channel_name=self.name,
                    channel_type=self.channel_type,
                    success=False,
                    error=f"Discord returned {response.status_code}: {response.text[:200]}",
                )
        except Exception as e:
            if "httpx" in str(type(e).__module__):
                return DeliveryResult(
                    channel_name=self.name,
                    channel_type=self.channel_type,
                    success=False,
                    error=f"HTTP error: {e}",
                )
            return DeliveryResult(
                channel_name=self.name,
                channel_type=self.channel_type,
                success=False,
                error=str(e),
            )

    def _clean_notification(self, notification: Notification) -> Notification:
        """Clean notification data for Discord limits."""
        # Discord embed title max: 256 chars
        # Discord embed description max: 4096 chars
        # Discord field name max: 256, value max: 1024
        n = notification.model_copy(deep=True)
        if len(n.title) > 256:
            n.title = n.title[:253] + "..."
        if n.message and len(n.message) > 4096:
            n.message = n.message[:4093] + "..."
        return n


# ── Email Channel ───────────────────────────────────────────────


class EmailChannel:
    """Send notifications via SMTP email.

    Uses synchronous smtplib wrapped in a thread executor for async compat.
    """

    def __init__(
        self,
        smtp_host: str,
        smtp_port: int = 587,
        username: Optional[str] = None,
        password: Optional[str] = None,
        from_addr: Optional[str] = None,
        to_addrs: Optional[list[str]] = None,
        use_tls: bool = True,
        use_ssl: bool = False,
        name: str = "email",
    ) -> None:
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.from_addr = from_addr or username or "agent-scheduler@localhost"
        self.to_addrs = to_addrs or []
        self.use_tls = use_tls
        self.use_ssl = use_ssl
        self.name = name
        self.channel_type = "email"

    def _build_message(self, notification: Notification) -> MIMEMultipart:
        """Build email message."""
        msg = MIMEMultipart("alternative")
        msg["From"] = self.from_addr
        msg["To"] = ", ".join(self.to_addrs)
        msg["Subject"] = f"[{notification.level.value.upper()}] {notification.title}"

        # Plain text body
        lines = [
            f"Title: {notification.title}",
            f"Level: {notification.level.value.upper()}",
            f"Time: {notification.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}",
        ]
        if notification.job_name:
            lines.append(f"Job: {notification.job_name}")
        if notification.event_type:
            lines.append(f"Event: {notification.event_type}")
        lines.append("")
        if notification.message:
            lines.append(notification.message)
        if notification.metadata:
            lines.append("")
            lines.append("--- Details ---")
            for k, v in notification.metadata.items():
                lines.append(f"{k}: {v}")

        text_body = "\n".join(lines)
        msg.attach(MIMEText(text_body, "plain"))

        # HTML body
        html_lines = [
            "<html><body>",
            f"<h2>{notification.level.slack_emoji} {notification.title}</h2>",
            "<table>",
        ]
        if notification.job_name:
            html_lines.append(f"<tr><td><b>Job:</b></td><td><code>{notification.job_name}</code></td></tr>")
        if notification.event_type:
            html_lines.append(f"<tr><td><b>Event:</b></td><td><code>{notification.event_type}</code></td></tr>")
        html_lines.append(f"<tr><td><b>Time:</b></td><td>{notification.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}</td></tr>")
        html_lines.append("</table>")
        if notification.message:
            html_lines.append(f"<p>{notification.message}</p>")
        if notification.metadata:
            html_lines.append("<h3>Details</h3><ul>")
            for k, v in notification.metadata.items():
                html_lines.append(f"<li><b>{k}:</b> {v}</li>")
            html_lines.append("</ul>")
        html_lines.append("</body></html>")
        msg.attach(MIMEText("\n".join(html_lines), "html"))

        return msg

    async def send(self, notification: Notification) -> DeliveryResult:
        """Send notification via email."""
        if not self.to_addrs:
            return DeliveryResult(
                channel_name=self.name,
                channel_type=self.channel_type,
                success=False,
                error="No recipients configured",
            )

        loop = asyncio.get_event_loop()
        try:
            result = await loop.run_in_executor(None, self._send_sync, notification)
            return result
        except Exception as e:
            return DeliveryResult(
                channel_name=self.name,
                channel_type=self.channel_type,
                success=False,
                error=str(e),
            )

    def _send_sync(self, notification: Notification) -> DeliveryResult:
        """Synchronous email send (called from executor)."""
        msg = self._build_message(notification)
        try:
            if self.use_ssl:
                context = ssl.create_default_context()
                server = smtplib.SMTP_SSL(self.smtp_host, self.smtp_port, context=context)
            else:
                server = smtplib.SMTP(self.smtp_host, self.smtp_port)
                if self.use_tls:
                    server.starttls()

            if self.username and self.password:
                server.login(self.username, self.password)

            server.sendmail(self.from_addr, self.to_addrs, msg.as_string())
            server.quit()

            return DeliveryResult(
                channel_name=self.name,
                channel_type=self.channel_type,
                success=True,
            )
        except smtplib.SMTPException as e:
            return DeliveryResult(
                channel_name=self.name,
                channel_type=self.channel_type,
                success=False,
                error=f"SMTP error: {e}",
            )


# ── Generic HTTP Channel ────────────────────────────────────────


class HttpChannel:
    """Send notifications to any HTTP endpoint as JSON POST."""

    def __init__(
        self,
        url: str,
        name: str = "http",
        headers: Optional[dict[str, str]] = None,
        timeout: float = 10.0,
        secret: Optional[str] = None,
    ) -> None:
        self.url = url
        self.name = name
        self.channel_type = "http"
        self._headers = headers or {"Content-Type": "application/json"}
        self._timeout = timeout
        self._secret = secret

    async def send(self, notification: Notification) -> DeliveryResult:
        """Send notification as JSON POST."""
        try:
            import httpx

            payload = notification.model_dump(mode="json")

            headers = dict(self._headers)
            if self._secret:
                import hmac
                import hashlib

                body = json.dumps(payload, sort_keys=True).encode()
                signature = hmac.new(
                    self._secret.encode(),
                    body,
                    hashlib.sha256,
                ).hexdigest()
                headers["X-Signature-256"] = f"sha256={signature}"

            async with httpx.AsyncClient(timeout=self._timeout) as client:
                response = await client.post(self.url, json=payload, headers=headers)

            if response.status_code < 400:
                return DeliveryResult(
                    channel_name=self.name,
                    channel_type=self.channel_type,
                    success=True,
                )
            else:
                return DeliveryResult(
                    channel_name=self.name,
                    channel_type=self.channel_type,
                    success=False,
                    error=f"HTTP {response.status_code}: {response.text[:200]}",
                )
        except Exception as e:
            return DeliveryResult(
                channel_name=self.name,
                channel_type=self.channel_type,
                success=False,
                error=str(e),
            )


# ── Channel Manager ─────────────────────────────────────────────


class ChannelManager:
    """Manages multiple notification channels and routes events.

    Channels can be filtered by level — e.g., only send errors to PagerDuty,
    but send all events to Slack.
    """

    def __init__(self) -> None:
        self._channels: dict[str, NotificationChannel] = {}
        # Maps channel name → set of levels it should receive (None = all)
        self._filters: dict[str, Optional[set[NotificationLevel]]] = {}

    def add_channel(
        self,
        channel: NotificationChannel,
        levels: Optional[list[NotificationLevel]] = None,
    ) -> None:
        """Register a notification channel.

        Args:
            channel: The channel to add
            levels: If provided, only notifications at these levels
                    will be sent to this channel. None means all levels.
        """
        self._channels[channel.name] = channel
        self._filters[channel.name] = set(levels) if levels else None

    def remove_channel(self, name: str) -> bool:
        """Remove a channel by name."""
        existed = name in self._channels
        self._channels.pop(name, None)
        self._filters.pop(name, None)
        return existed

    def get_channel(self, name: str) -> Optional[NotificationChannel]:
        """Get a channel by name."""
        return self._channels.get(name)

    def list_channels(self) -> list[dict[str, Any]]:
        """List all registered channels with their configuration."""
        result = []
        for name, channel in self._channels.items():
            levels_filter = self._filters.get(name)
            result.append({
                "name": name,
                "type": getattr(channel, "channel_type", "unknown"),
                "levels": [l.value for l in levels_filter] if levels_filter else "all",
            })
        return result

    async def send(self, notification: Notification) -> list[DeliveryResult]:
        """Send a notification to all applicable channels.

        Channels that don't match the notification's level are skipped.
        """
        results: list[DeliveryResult] = []

        for name, channel in self._channels.items():
            # Check level filter
            allowed_levels = self._filters.get(name)
            if allowed_levels is not None and notification.level not in allowed_levels:
                continue

            try:
                result = await channel.send(notification)
            except Exception as e:
                result = DeliveryResult(
                    channel_name=name,
                    channel_type=getattr(channel, "channel_type", "unknown"),
                    success=False,
                    error=str(e),
                )
            results.append(result)

        return results

    async def send_batch(self, notifications: list[Notification]) -> list[list[DeliveryResult]]:
        """Send multiple notifications to all applicable channels."""
        all_results = []
        for n in notifications:
            results = await self.send(n)
            all_results.append(results)
        return all_results


# ── Factory ─────────────────────────────────────────────────────


def create_channel_from_config(config: dict[str, Any]) -> NotificationChannel:
    """Create a notification channel from a configuration dict.

    The ``type`` key determines the channel type. Supported types:
    ``slack``, ``discord``, ``email``, ``http``.

    Examples::

        create_channel_from_config({
            "type": "slack",
            "name": "ops-slack",
            "webhook_url": "https://hooks.slack.com/services/...",
        })

        create_channel_from_config({
            "type": "email",
            "name": "alerts",
            "smtp_host": "smtp.gmail.com",
            "smtp_port": 587,
            "username": "bot@example.com",
            "password": "app-password",
            "from_addr": "bot@example.com",
            "to_addrs": ["ops@example.com"],
            "use_tls": True,
        })
    """
    channel_type = config.get("type", "").lower()
    name = config.get("name", channel_type)

    if channel_type == "slack":
        return SlackChannel(
            webhook_url=config["webhook_url"],
            name=name,
            channel=config.get("channel"),
            timeout=config.get("timeout", 10.0),
        )
    elif channel_type == "discord":
        return DiscordChannel(
            webhook_url=config["webhook_url"],
            name=name,
            timeout=config.get("timeout", 10.0),
        )
    elif channel_type == "email":
        return EmailChannel(
            smtp_host=config["smtp_host"],
            smtp_port=config.get("smtp_port", 587),
            username=config.get("username"),
            password=config.get("password"),
            from_addr=config.get("from_addr"),
            to_addrs=config.get("to_addrs", []),
            use_tls=config.get("use_tls", True),
            use_ssl=config.get("use_ssl", False),
            name=name,
        )
    elif channel_type == "http":
        return HttpChannel(
            url=config["url"],
            name=name,
            headers=config.get("headers"),
            timeout=config.get("timeout", 10.0),
            secret=config.get("secret"),
        )
    else:
        raise ValueError(
            f"Unknown channel type: {channel_type!r}. "
            f"Supported: slack, discord, email, http"
        )
