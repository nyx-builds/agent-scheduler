"""Tests for the notifications module."""

import asyncio
import json
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from agent_scheduler.notifications import (
    Notification,
    NotificationLevel,
    DeliveryResult,
    ChannelManager,
    SlackChannel,
    DiscordChannel,
    EmailChannel,
    HttpChannel,
    create_channel_from_config,
)


# ── NotificationLevel ─────────────────────────────────────


class TestNotificationLevel:
    def test_slack_emoji(self):
        assert NotificationLevel.INFO.slack_emoji == "ℹ️"
        assert NotificationLevel.ERROR.slack_emoji == "🚨"
        assert NotificationLevel.SUCCESS.slack_emoji == "✅"
        assert NotificationLevel.WARNING.slack_emoji == "⚠️"

    def test_discord_color(self):
        assert isinstance(NotificationLevel.INFO.discord_color, int)
        assert NotificationLevel.ERROR.discord_color == 0xE74C3C


# ── Notification ───────────────────────────────────────────


class TestNotification:
    def test_basic(self):
        n = Notification(title="Test Alert")
        assert n.title == "Test Alert"
        assert n.level == NotificationLevel.INFO
        assert n.message == ""

    def test_with_fields(self):
        n = Notification(
            title="Job Failed",
            message="Handler raised exception",
            level=NotificationLevel.ERROR,
            job_name="data-pipeline",
            job_id="job-123",
            event_type="job.failed",
            metadata={"retry_count": 3},
        )
        assert n.job_name == "data-pipeline"
        assert n.metadata["retry_count"] == 3
        assert n.timestamp is not None


# ── SlackChannel ───────────────────────────────────────────


class TestSlackChannel:
    def test_init(self):
        ch = SlackChannel(webhook_url="https://hooks.slack.com/test")
        assert ch.name == "slack"
        assert ch.channel_type == "slack"

    def test_build_payload(self):
        ch = SlackChannel(webhook_url="https://hooks.slack.com/test")
        n = Notification(title="Test", message="Body", level=NotificationLevel.ERROR)
        payload = ch._build_payload(n)
        assert "blocks" in payload
        assert len(payload["blocks"]) >= 1

    def test_build_payload_with_channel(self):
        ch = SlackChannel(webhook_url="https://hooks.slack.com/test", channel="#ops")
        n = Notification(title="Test")
        payload = ch._build_payload(n)
        assert payload["channel"] == "#ops"

    def test_build_payload_with_metadata(self):
        ch = SlackChannel(webhook_url="https://hooks.slack.com/test")
        n = Notification(title="Test", metadata={"key": "value"})
        payload = ch._build_payload(n)
        # Should have metadata as fields
        blocks_str = json.dumps(payload)
        assert "key" in blocks_str

    @pytest.mark.asyncio
    async def test_send_success(self):
        ch = SlackChannel(webhook_url="https://hooks.slack.com/test")
        n = Notification(title="Test")

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "ok"

        with patch("httpx.AsyncClient") as mock_client:
            mock_instance = AsyncMock()
            mock_instance.post = AsyncMock(return_value=mock_response)
            mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_instance.__aexit__ = AsyncMock(return_value=None)
            mock_client.return_value = mock_instance

            result = await ch.send(n)
            assert result.success is True

    @pytest.mark.asyncio
    async def test_send_failure(self):
        ch = SlackChannel(webhook_url="https://hooks.slack.com/test")
        n = Notification(title="Test")

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"

        with patch("httpx.AsyncClient") as mock_client:
            mock_instance = AsyncMock()
            mock_instance.post = AsyncMock(return_value=mock_response)
            mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_instance.__aexit__ = AsyncMock(return_value=None)
            mock_client.return_value = mock_instance

            result = await ch.send(n)
            assert result.success is False
            assert "500" in result.error


# ── DiscordChannel ─────────────────────────────────────────


class TestDiscordChannel:
    def test_init(self):
        ch = DiscordChannel(webhook_url="https://discord.com/api/webhooks/test")
        assert ch.name == "discord"
        assert ch.channel_type == "discord"

    def test_build_payload(self):
        ch = DiscordChannel(webhook_url="https://discord.com/test")
        n = Notification(title="Test", message="Body", level=NotificationLevel.ERROR)
        payload = ch._build_payload(n)
        assert "embeds" in payload
        assert payload["embeds"][0]["title"] == "Test"
        assert payload["embeds"][0]["color"] == NotificationLevel.ERROR.discord_color

    def test_error_mentions_here(self):
        ch = DiscordChannel(webhook_url="https://discord.com/test")
        n = Notification(title="Test", level=NotificationLevel.ERROR)
        payload = ch._build_payload(n)
        assert payload.get("content") == "@here"

    def test_clean_notification_truncates_title(self):
        ch = DiscordChannel(webhook_url="https://discord.com/test")
        n = Notification(title="A" * 300, level=NotificationLevel.INFO)
        cleaned = ch._clean_notification(n)
        assert len(cleaned.title) <= 256

    @pytest.mark.asyncio
    async def test_send_success(self):
        ch = DiscordChannel(webhook_url="https://discord.com/test")
        n = Notification(title="Test")

        mock_response = MagicMock()
        mock_response.status_code = 204
        mock_response.text = ""

        with patch("httpx.AsyncClient") as mock_client:
            mock_instance = AsyncMock()
            mock_instance.post = AsyncMock(return_value=mock_response)
            mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_instance.__aexit__ = AsyncMock(return_value=None)
            mock_client.return_value = mock_instance

            result = await ch.send(n)
            assert result.success is True


# ── EmailChannel ───────────────────────────────────────────


class TestEmailChannel:
    def test_init(self):
        ch = EmailChannel(
            smtp_host="smtp.gmail.com",
            smtp_port=587,
            username="bot@test.com",
            to_addrs=["ops@test.com"],
        )
        assert ch.name == "email"
        assert ch.channel_type == "email"
        assert ch.from_addr == "bot@test.com"

    def test_init_defaults(self):
        ch = EmailChannel(smtp_host="localhost")
        assert ch.smtp_port == 587
        assert ch.use_tls is True
        assert ch.from_addr == "agent-scheduler@localhost"

    def test_build_message(self):
        ch = EmailChannel(
            smtp_host="smtp.test.com",
            from_addr="bot@test.com",
            to_addrs=["ops@test.com"],
        )
        n = Notification(
            title="Job Failed",
            message="Something went wrong",
            level=NotificationLevel.ERROR,
            job_name="test-job",
        )
        msg = ch._build_message(n)
        assert msg["From"] == "bot@test.com"
        assert msg["To"] == "ops@test.com"
        assert "[ERROR]" in msg["Subject"]

    @pytest.mark.asyncio
    async def test_send_no_recipients(self):
        ch = EmailChannel(smtp_host="localhost")
        n = Notification(title="Test")
        result = await ch.send(n)
        assert result.success is False
        assert "No recipients" in result.error

    @pytest.mark.asyncio
    async def test_send_success_mocked(self):
        ch = EmailChannel(
            smtp_host="smtp.test.com",
            from_addr="bot@test.com",
            to_addrs=["ops@test.com"],
        )
        n = Notification(title="Test")

        with patch.object(ch, "_send_sync") as mock_send:
            mock_send.return_value = DeliveryResult(
                channel_name="email", channel_type="email", success=True
            )
            result = await ch.send(n)
            assert result.success is True


# ── HttpChannel ────────────────────────────────────────────


class TestHttpChannel:
    def test_init(self):
        ch = HttpChannel(url="https://example.com/webhook")
        assert ch.name == "http"
        assert ch.channel_type == "http"

    def test_init_with_headers_and_secret(self):
        ch = HttpChannel(
            url="https://example.com/webhook",
            headers={"Authorization": "Bearer token"},
            secret="mysecret",
        )
        assert ch._headers["Authorization"] == "Bearer token"
        assert ch._secret == "mysecret"

    @pytest.mark.asyncio
    async def test_send_success(self):
        ch = HttpChannel(url="https://example.com/webhook")
        n = Notification(title="Test")

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "OK"

        with patch("httpx.AsyncClient") as mock_client:
            mock_instance = AsyncMock()
            mock_instance.post = AsyncMock(return_value=mock_response)
            mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_instance.__aexit__ = AsyncMock(return_value=None)
            mock_client.return_value = mock_instance

            result = await ch.send(n)
            assert result.success is True

    @pytest.mark.asyncio
    async def test_send_failure(self):
        ch = HttpChannel(url="https://example.com/webhook")
        n = Notification(title="Test")

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Error"

        with patch("httpx.AsyncClient") as mock_client:
            mock_instance = AsyncMock()
            mock_instance.post = AsyncMock(return_value=mock_response)
            mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_instance.__aexit__ = AsyncMock(return_value=None)
            mock_client.return_value = mock_instance

            result = await ch.send(n)
            assert result.success is False

    @pytest.mark.asyncio
    async def test_send_with_secret_adds_signature(self):
        ch = HttpChannel(url="https://example.com/webhook", secret="test-secret")
        n = Notification(title="Test")

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "OK"

        captured_headers = {}

        async def capture_post(url, json=None, headers=None):
            captured_headers.update(headers or {})
            return mock_response

        with patch("httpx.AsyncClient") as mock_client:
            mock_instance = AsyncMock()
            mock_instance.post = capture_post
            mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_instance.__aexit__ = AsyncMock(return_value=None)
            mock_client.return_value = mock_instance

            result = await ch.send(n)
            assert result.success is True
            assert "X-Signature-256" in captured_headers
            assert captured_headers["X-Signature-256"].startswith("sha256=")


# ── ChannelManager ─────────────────────────────────────────


class TestChannelManager:
    def test_add_remove_channel(self):
        mgr = ChannelManager()
        ch = HttpChannel(url="https://example.com/test", name="test-http")
        mgr.add_channel(ch)
        assert mgr.get_channel("test-http") is not None

        assert mgr.remove_channel("test-http") is True
        assert mgr.get_channel("test-http") is None
        assert mgr.remove_channel("nonexistent") is False

    def test_list_channels(self):
        mgr = ChannelManager()
        mgr.add_channel(HttpChannel(url="https://a.com", name="ch1"))
        mgr.add_channel(SlackChannel(webhook_url="https://slack.com", name="ch2"))
        channels = mgr.list_channels()
        assert len(channels) == 2

    def test_list_channels_with_levels(self):
        mgr = ChannelManager()
        mgr.add_channel(
            HttpChannel(url="https://a.com", name="errors-only"),
            levels=[NotificationLevel.ERROR],
        )
        channels = mgr.list_channels()
        assert channels[0]["levels"] == ["error"]

    @pytest.mark.asyncio
    async def test_send_filters_by_level(self):
        mgr = ChannelManager()
        mock_channel = AsyncMock()
        mock_channel.name = "mock"
        mock_channel.channel_type = "mock"
        mock_channel.send = AsyncMock(return_value=DeliveryResult(
            channel_name="mock", channel_type="mock", success=True
        ))
        mgr.add_channel(mock_channel, levels=[NotificationLevel.ERROR])

        # INFO notification should be filtered out
        n_info = Notification(title="Info", level=NotificationLevel.INFO)
        results = await mgr.send(n_info)
        assert len(results) == 0

        # ERROR notification should go through
        n_error = Notification(title="Error", level=NotificationLevel.ERROR)
        results = await mgr.send(n_error)
        assert len(results) == 1
        assert results[0].success is True

    @pytest.mark.asyncio
    async def test_send_all_levels(self):
        mgr = ChannelManager()
        mock_channel = AsyncMock()
        mock_channel.name = "mock"
        mock_channel.channel_type = "mock"
        mock_channel.send = AsyncMock(return_value=DeliveryResult(
            channel_name="mock", channel_type="mock", success=True
        ))
        mgr.add_channel(mock_channel)  # No level filter = all

        for level in NotificationLevel:
            n = Notification(title="Test", level=level)
            results = await mgr.send(n)
            assert len(results) == 1

    @pytest.mark.asyncio
    async def test_send_handles_exception(self):
        mgr = ChannelManager()
        mock_channel = AsyncMock()
        mock_channel.name = "mock"
        mock_channel.channel_type = "mock"
        mock_channel.send = AsyncMock(side_effect=Exception("Network error"))
        mgr.add_channel(mock_channel)

        n = Notification(title="Test")
        results = await mgr.send(n)
        assert len(results) == 1
        assert results[0].success is False
        assert "Network error" in results[0].error

    @pytest.mark.asyncio
    async def test_send_batch(self):
        mgr = ChannelManager()
        mock_channel = AsyncMock()
        mock_channel.name = "mock"
        mock_channel.channel_type = "mock"
        mock_channel.send = AsyncMock(return_value=DeliveryResult(
            channel_name="mock", channel_type="mock", success=True
        ))
        mgr.add_channel(mock_channel)

        notifications = [
            Notification(title=f"Test {i}") for i in range(3)
        ]
        all_results = await mgr.send_batch(notifications)
        assert len(all_results) == 3
        assert all(len(r) == 1 for r in all_results)


# ── create_channel_from_config ─────────────────────────────


class TestCreateFromConfig:
    def test_slack(self):
        ch = create_channel_from_config({
            "type": "slack",
            "webhook_url": "https://hooks.slack.com/test",
        })
        assert isinstance(ch, SlackChannel)

    def test_discord(self):
        ch = create_channel_from_config({
            "type": "discord",
            "webhook_url": "https://discord.com/test",
        })
        assert isinstance(ch, DiscordChannel)

    def test_email(self):
        ch = create_channel_from_config({
            "type": "email",
            "smtp_host": "smtp.test.com",
            "to_addrs": ["ops@test.com"],
        })
        assert isinstance(ch, EmailChannel)

    def test_http(self):
        ch = create_channel_from_config({
            "type": "http",
            "url": "https://example.com/webhook",
        })
        assert isinstance(ch, HttpChannel)

    def test_custom_name(self):
        ch = create_channel_from_config({
            "type": "http",
            "name": "my-alerts",
            "url": "https://example.com/webhook",
        })
        assert ch.name == "my-alerts"

    def test_unknown_type_raises(self):
        with pytest.raises(ValueError, match="Unknown channel type"):
            create_channel_from_config({"type": "carrier-pigeon"})

    def test_slack_with_channel(self):
        ch = create_channel_from_config({
            "type": "slack",
            "webhook_url": "https://hooks.slack.com/test",
            "channel": "#alerts",
        })
        assert ch._channel == "#alerts"
