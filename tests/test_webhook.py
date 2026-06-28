"""Tests for webhook notifications."""

import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

from agent_scheduler.models import Job, JobStatus, Priority, RetryPolicy
from agent_scheduler.scheduler import Scheduler
from agent_scheduler.store import JSONJobStore
from agent_scheduler.webhook import (
    Webhook,
    WebhookDelivery,
    WebhookEvent,
    WebhookManager,
    WebhookStatus,
    build_webhook_payload,
    sign_payload,
)


@pytest.fixture
def webhook_manager(tmp_path):
    store = JSONJobStore(data_dir=str(tmp_path / "webhook-test"))
    return WebhookManager(store=store)


@pytest.fixture
def scheduler_with_webhooks(tmp_path):
    store = JSONJobStore(data_dir=str(tmp_path / "scheduler-webhook-test"))
    scheduler = Scheduler(store=store)
    return scheduler


class TestWebhookModel:
    def test_webhook_defaults(self):
        wh = Webhook(name="test", url="https://example.com/hook")
        assert wh.name == "test"
        assert wh.url == "https://example.com/hook"
        assert wh.enabled is True
        assert wh.max_retries == 3
        assert wh.timeout == 10.0
        assert wh.id  # Auto-generated

    def test_webhook_all_events_default(self):
        wh = Webhook(name="test", url="https://example.com/hook")
        assert len(wh.events) == len(WebhookEvent)

    def test_webhook_specific_events(self):
        events = [WebhookEvent.JOB_COMPLETED, WebhookEvent.JOB_FAILED]
        wh = Webhook(name="test", url="https://example.com/hook", events=events)
        assert len(wh.events) == 2
        assert WebhookEvent.JOB_COMPLETED in wh.events

    def test_matches_event(self):
        wh = Webhook(
            name="test",
            url="https://example.com/hook",
            events=[WebhookEvent.JOB_COMPLETED, WebhookEvent.JOB_FAILED],
        )
        assert wh.matches_event(WebhookEvent.JOB_COMPLETED) is True
        assert wh.matches_event(WebhookEvent.JOB_CREATED) is False

    def test_matches_tags_empty_matches_all(self):
        wh = Webhook(name="test", url="https://example.com/hook", tags=[])
        assert wh.matches_tags(["monitoring"]) is True
        assert wh.matches_tags([]) is True

    def test_matches_tags_with_filter(self):
        wh = Webhook(name="test", url="https://example.com/hook", tags=["monitoring", "backup"])
        assert wh.matches_tags(["monitoring"]) is True
        assert wh.matches_tags(["backup"]) is True
        assert wh.matches_tags(["reporting"]) is False
        assert wh.matches_tags([]) is False


class TestWebhookPayload:
    def test_build_payload_with_job(self):
        job = Job(name="test-job", handler="h", tags=["monitoring"])
        payload = build_webhook_payload(WebhookEvent.JOB_CREATED, job)
        assert payload["event"] == "job.created"
        assert payload["job"]["name"] == "test-job"
        assert "monitoring" in payload["job"]["tags"]
        assert "timestamp" in payload

    def test_build_payload_with_execution(self):
        from agent_scheduler.models import JobExecution, ExecutionStatus
        job = Job(name="test-job", handler="h")
        execution = JobExecution(
            job_id=job.id,
            job_name=job.name,
            status=ExecutionStatus.SUCCESS,
            duration_seconds=1.5,
        )
        payload = build_webhook_payload(WebhookEvent.JOB_COMPLETED, job, execution)
        assert "execution" in payload
        assert payload["execution"]["status"] == "success"
        assert payload["execution"]["duration_seconds"] == 1.5

    def test_build_payload_with_extra(self):
        job = Job(name="test-job", handler="h")
        payload = build_webhook_payload(WebhookEvent.JOB_FAILED, job, extra={"custom": "data"})
        assert payload["custom"] == "data"


class TestSignPayload:
    def test_sign_payload(self):
        payload = {"event": "test", "data": "value"}
        sig = sign_payload(payload, "my-secret")
        assert len(sig) == 64  # SHA-256 hex
        assert isinstance(sig, str)

    def test_sign_payload_deterministic(self):
        payload = {"event": "test"}
        sig1 = sign_payload(payload, "secret")
        sig2 = sign_payload(payload, "secret")
        assert sig1 == sig2

    def test_different_secrets_different_signatures(self):
        payload = {"event": "test"}
        sig1 = sign_payload(payload, "secret1")
        sig2 = sign_payload(payload, "secret2")
        assert sig1 != sig2


class TestWebhookManagerCRUD:
    def test_create_webhook(self, webhook_manager):
        wh = Webhook(name="test", url="https://example.com/hook")
        result = webhook_manager.create_webhook(wh)
        assert result.id == wh.id

    def test_get_webhook(self, webhook_manager):
        wh = Webhook(name="test", url="https://example.com/hook")
        webhook_manager.create_webhook(wh)
        retrieved = webhook_manager.get_webhook(wh.id)
        assert retrieved is not None
        assert retrieved.name == "test"

    def test_get_webhook_not_found(self, webhook_manager):
        assert webhook_manager.get_webhook("nonexistent") is None

    def test_update_webhook(self, webhook_manager):
        wh = Webhook(name="test", url="https://example.com/hook")
        webhook_manager.create_webhook(wh)
        updated = webhook_manager.update_webhook(wh.id, enabled=False)
        assert updated is not None
        assert updated.enabled is False

    def test_update_webhook_not_found(self, webhook_manager):
        assert webhook_manager.update_webhook("nonexistent", enabled=False) is None

    def test_delete_webhook(self, webhook_manager):
        wh = Webhook(name="test", url="https://example.com/hook")
        webhook_manager.create_webhook(wh)
        assert webhook_manager.delete_webhook(wh.id) is True
        assert webhook_manager.get_webhook(wh.id) is None

    def test_delete_webhook_not_found(self, webhook_manager):
        assert webhook_manager.delete_webhook("nonexistent") is False

    def test_list_webhooks(self, webhook_manager):
        webhook_manager.create_webhook(Webhook(name="wh1", url="https://a.com"))
        webhook_manager.create_webhook(Webhook(name="wh2", url="https://b.com"))
        assert len(webhook_manager.list_webhooks()) == 2

    def test_list_webhooks_enabled_only(self, webhook_manager):
        webhook_manager.create_webhook(Webhook(name="wh1", url="https://a.com", enabled=True))
        webhook_manager.create_webhook(Webhook(name="wh2", url="https://b.com", enabled=False))
        assert len(webhook_manager.list_webhooks(enabled_only=True)) == 1


class TestWebhookManagerDelivery:
    @pytest.mark.asyncio
    async def test_fire_event_no_matching_webhooks(self, webhook_manager):
        job = Job(name="test", handler="h", tags=["monitoring"])
        # No webhooks registered
        deliveries = await webhook_manager.fire_event(WebhookEvent.JOB_COMPLETED, job)
        assert deliveries == []

    @pytest.mark.asyncio
    async def test_fire_event_with_matching_webhook(self, webhook_manager):
        job = Job(name="test", handler="h", tags=["monitoring"])
        wh = Webhook(
            name="test-hook",
            url="https://example.com/hook",
            events=[WebhookEvent.JOB_COMPLETED],
            tags=["monitoring"],
        )
        webhook_manager.create_webhook(wh)

        # Mock the HTTP delivery
        with patch.object(webhook_manager, '_deliver') as mock_deliver:
            mock_delivery = WebhookDelivery(
                webhook_id=wh.id,
                event=WebhookEvent.JOB_COMPLETED,
                job_id=job.id,
                job_name=job.name,
                payload={},
                status=WebhookStatus.DELIVERED,
            )
            mock_deliver.return_value = mock_delivery
            deliveries = await webhook_manager.fire_event(WebhookEvent.JOB_COMPLETED, job)
            assert len(deliveries) == 1
            mock_deliver.assert_called_once()

    @pytest.mark.asyncio
    async def test_fire_event_skips_non_matching_tags(self, webhook_manager):
        job = Job(name="test", handler="h", tags=["monitoring"])
        wh = Webhook(
            name="test-hook",
            url="https://example.com/hook",
            events=[WebhookEvent.JOB_COMPLETED],
            tags=["backup"],  # Different tag
        )
        webhook_manager.create_webhook(wh)

        with patch.object(webhook_manager, '_deliver') as mock_deliver:
            deliveries = await webhook_manager.fire_event(WebhookEvent.JOB_COMPLETED, job)
            assert len(deliveries) == 0
            mock_deliver.assert_not_called()

    @pytest.mark.asyncio
    async def test_fire_event_skips_disabled_webhooks(self, webhook_manager):
        job = Job(name="test", handler="h")
        wh = Webhook(
            name="test-hook",
            url="https://example.com/hook",
            events=[WebhookEvent.JOB_COMPLETED],
            enabled=False,
        )
        webhook_manager.create_webhook(wh)

        with patch.object(webhook_manager, '_deliver') as mock_deliver:
            deliveries = await webhook_manager.fire_event(WebhookEvent.JOB_COMPLETED, job)
            assert len(deliveries) == 0

    @pytest.mark.asyncio
    async def test_fire_event_skips_non_matching_events(self, webhook_manager):
        job = Job(name="test", handler="h")
        wh = Webhook(
            name="test-hook",
            url="https://example.com/hook",
            events=[WebhookEvent.JOB_FAILED],  # Only failed
        )
        webhook_manager.create_webhook(wh)

        with patch.object(webhook_manager, '_deliver') as mock_deliver:
            deliveries = await webhook_manager.fire_event(WebhookEvent.JOB_COMPLETED, job)
            assert len(deliveries) == 0

    def test_get_deliveries(self, webhook_manager):
        # Manually save a delivery
        delivery = WebhookDelivery(
            webhook_id="wh1",
            event=WebhookEvent.JOB_COMPLETED,
            job_id="job1",
            job_name="test",
            payload={"event": "job.completed"},
            status=WebhookStatus.DELIVERED,
        )
        webhook_manager._save_delivery(delivery)
        deliveries = webhook_manager.get_deliveries()
        assert len(deliveries) == 1

    def test_get_deliveries_by_webhook(self, webhook_manager):
        d1 = WebhookDelivery(
            webhook_id="wh1", event=WebhookEvent.JOB_COMPLETED,
            job_id="job1", job_name="test", payload={}, status=WebhookStatus.DELIVERED,
        )
        d2 = WebhookDelivery(
            webhook_id="wh2", event=WebhookEvent.JOB_FAILED,
            job_id="job1", job_name="test", payload={}, status=WebhookStatus.FAILED,
        )
        webhook_manager._save_delivery(d1)
        webhook_manager._save_delivery(d2)
        filtered = webhook_manager.get_deliveries(webhook_id="wh1")
        assert len(filtered) == 1


class TestWebhookDeliveryModel:
    def test_delivery_defaults(self):
        d = WebhookDelivery(
            webhook_id="wh1",
            event=WebhookEvent.JOB_COMPLETED,
            job_id="job1",
            job_name="test",
            payload={},
        )
        assert d.status == WebhookStatus.PENDING
        assert d.attempt == 1
        assert d.id  # Auto-generated

    def test_delivery_success(self):
        d = WebhookDelivery(
            webhook_id="wh1",
            event=WebhookEvent.JOB_COMPLETED,
            job_id="job1",
            job_name="test",
            payload={},
            status=WebhookStatus.DELIVERED,
            status_code=200,
            delivered_at=datetime.now(timezone.utc),
        )
        assert d.status == WebhookStatus.DELIVERED
        assert d.status_code == 200


class TestWebhookIntegration:
    @pytest.mark.asyncio
    async def test_scheduler_auto_initializes_webhook_manager(self, tmp_path):
        store = JSONJobStore(data_dir=str(tmp_path / "integration-test"))
        scheduler = Scheduler(store=store)
        assert scheduler.webhooks is not None

    @pytest.mark.asyncio
    async def test_webhook_persisted_across_restart(self, tmp_path):
        data_dir = str(tmp_path / "persist-test")
        wh = Webhook(name="persistent", url="https://example.com/hook")

        # Create with one store
        store1 = JSONJobStore(data_dir=data_dir)
        manager1 = WebhookManager(store=store1)
        manager1.create_webhook(wh)

        # Verify with another store
        store2 = JSONJobStore(data_dir=data_dir)
        manager2 = WebhookManager(store=store2)
        retrieved = manager2.get_webhook(wh.id)
        assert retrieved is not None
        assert retrieved.name == "persistent"
