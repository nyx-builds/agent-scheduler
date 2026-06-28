"""Tests for job templates."""

import pytest
from datetime import datetime, timezone

from agent_scheduler.models import Job, JobStatus, Priority, RetryPolicy
from agent_scheduler.scheduler import Scheduler
from agent_scheduler.store import JSONJobStore
from agent_scheduler.templates import (
    BUILTIN_TEMPLATES,
    JobTemplate,
    TemplateCategory,
    TemplateManager,
)


@pytest.fixture
def template_manager(tmp_path):
    store = JSONJobStore(data_dir=str(tmp_path / "template-test"))
    return TemplateManager(store=store)


@pytest.fixture
def scheduler(tmp_path):
    store = JSONJobStore(data_dir=str(tmp_path / "scheduler-template-test"))
    return Scheduler(store=store)


class TestJobTemplateModel:
    def test_template_defaults(self):
        t = JobTemplate(name="test", handler="test.handler")
        assert t.name == "test"
        assert t.handler == "test.handler"
        assert t.category == TemplateCategory.CUSTOM
        assert t.default_priority == Priority.NORMAL
        assert t.default_timeout == 300
        assert t.id  # Auto-generated

    def test_template_with_all_fields(self):
        t = JobTemplate(
            name="daily-backup",
            handler="backup.run",
            description="Daily backup",
            category=TemplateCategory.BACKUP,
            default_cron="0 2 * * *",
            default_priority=Priority.NORMAL,
            default_timeout=3600,
            default_tags=["backup"],
            default_max_retries=3,
            default_payload={"source": "", "destination": ""},
            required_fields=["payload.source", "payload.destination"],
        )
        assert t.category == TemplateCategory.BACKUP
        assert t.default_cron == "0 2 * * *"
        assert len(t.required_fields) == 2

    def test_template_category_enum(self):
        assert TemplateCategory.MONITORING.value == "monitoring"
        assert TemplateCategory.BACKUP.value == "backup"
        assert TemplateCategory.REPORTING.value == "reporting"
        assert TemplateCategory.MAINTENANCE.value == "maintenance"
        assert TemplateCategory.NOTIFICATION.value == "notification"
        assert TemplateCategory.DATA_PIPELINE.value == "data-pipeline"
        assert TemplateCategory.CUSTOM.value == "custom"


class TestJobTemplateInstantiate:
    def test_basic_instantiation(self):
        t = JobTemplate(name="test", handler="h")
        job = t.create_job()
        assert job.handler == "h"
        assert "test" in job.name  # Auto-generated name includes template name
        assert job.priority == Priority.NORMAL

    def test_instantiation_with_name_override(self):
        t = JobTemplate(name="test", handler="h")
        job = t.create_job(name="my-job")
        assert job.name == "my-job"

    def test_instantiation_with_cron(self):
        t = JobTemplate(name="test", handler="h", default_cron="0 9 * * *")
        job = t.create_job()
        assert job.cron == "0 9 * * *"

    def test_instantiation_cron_override(self):
        t = JobTemplate(name="test", handler="h", default_cron="0 9 * * *")
        job = t.create_job(cron="0 18 * * *")
        assert job.cron == "0 18 * * *"

    def test_instantiation_with_priority(self):
        t = JobTemplate(name="test", handler="h", default_priority=Priority.HIGH)
        job = t.create_job()
        assert job.priority == Priority.HIGH

    def test_instantiation_priority_override(self):
        t = JobTemplate(name="test", handler="h", default_priority=Priority.HIGH)
        job = t.create_job(priority=Priority.LOW)
        assert job.priority == Priority.LOW

    def test_instantiation_with_tags(self):
        t = JobTemplate(name="test", handler="h", default_tags=["monitoring"])
        job = t.create_job()
        assert "monitoring" in job.tags

    def test_instantiation_tags_override(self):
        t = JobTemplate(name="test", handler="h", default_tags=["monitoring"])
        job = t.create_job(tags=["backup", "important"])
        assert "backup" in job.tags
        assert "important" in job.tags

    def test_instantiation_with_default_payload(self):
        t = JobTemplate(
            name="test",
            handler="h",
            default_payload={"endpoint": "", "timeout": 30},
        )
        job = t.create_job(payload={"endpoint": "https://example.com"})
        assert job.payload["endpoint"] == "https://example.com"
        assert job.payload["timeout"] == 30  # Default preserved

    def test_instantiation_with_retry_policy(self):
        t = JobTemplate(name="test", handler="h", default_max_retries=3, default_backoff_seconds=60)
        job = t.create_job()
        assert job.retry_policy is not None
        assert job.retry_policy.max_retries == 3
        assert job.retry_policy.backoff_seconds == 60

    def test_instantiation_no_retry_when_zero(self):
        t = JobTemplate(name="test", handler="h", default_max_retries=0)
        job = t.create_job()
        assert job.retry_policy is None

    def test_instantiation_required_fields_missing(self):
        t = JobTemplate(
            name="test",
            handler="h",
            required_fields=["name", "payload.endpoint"],
        )
        with pytest.raises(ValueError, match="Missing required fields"):
            t.create_job()

    def test_instantiation_required_fields_provided(self):
        t = JobTemplate(
            name="test",
            handler="h",
            required_fields=["name"],
        )
        job = t.create_job(name="my-job")
        assert job.name == "my-job"

    def test_instantiation_with_delay(self):
        t = JobTemplate(name="test", handler="h")
        job = t.create_job(delay=60)
        assert job.delay == 60

    def test_instantiation_with_timeout_override(self):
        t = JobTemplate(name="test", handler="h", default_timeout=300)
        job = t.create_job(timeout=600)
        assert job.timeout == 600

    def test_instantiation_with_metadata(self):
        t = JobTemplate(
            name="test",
            handler="h",
            default_metadata={"environment": "production"},
        )
        job = t.create_job()
        assert job.metadata["environment"] == "production"


class TestBuiltinTemplates:
    def test_builtin_templates_exist(self):
        assert len(BUILTIN_TEMPLATES) >= 5
        assert "health-check" in BUILTIN_TEMPLATES
        assert "daily-backup" in BUILTIN_TEMPLATES
        assert "weekly-report" in BUILTIN_TEMPLATES
        assert "data-pipeline" in BUILTIN_TEMPLATES
        assert "cleanup" in BUILTIN_TEMPLATES
        assert "notification" in BUILTIN_TEMPLATES

    def test_health_check_template(self):
        t = BUILTIN_TEMPLATES["health-check"]
        assert t.handler == "health.check"
        assert t.category == TemplateCategory.MONITORING
        assert t.default_cron == "*/5 * * * *"
        assert t.default_priority == Priority.HIGH
        assert "payload.endpoint" in t.required_fields

    def test_daily_backup_template(self):
        t = BUILTIN_TEMPLATES["daily-backup"]
        assert t.handler == "backup.run"
        assert t.category == TemplateCategory.BACKUP
        assert t.default_cron == "0 2 * * *"
        assert "payload.source" in t.required_fields

    def test_notification_template(self):
        t = BUILTIN_TEMPLATES["notification"]
        assert t.handler == "notify.send"
        assert t.category == TemplateCategory.NOTIFICATION
        assert t.default_cron is None  # Not scheduled by default

    def test_instantiate_builtin_health_check(self):
        t = BUILTIN_TEMPLATES["health-check"]
        job = t.create_job(payload={"endpoint": "https://api.example.com/health"})
        assert job.handler == "health.check"
        assert job.priority == Priority.HIGH
        assert job.cron == "*/5 * * * *"
        assert "monitoring" in job.tags
        assert job.payload["endpoint"] == "https://api.example.com/health"
        assert job.retry_policy is not None

    def test_instantiate_builtin_with_overrides(self):
        t = BUILTIN_TEMPLATES["daily-backup"]
        job = t.create_job(
            payload={"source": "/data", "destination": "s3://backup"},
            cron="0 3 * * *",
        )
        assert job.cron == "0 3 * * *"  # Overridden
        assert job.payload["source"] == "/data"
        assert job.payload["compress"] is True  # Default preserved


class TestTemplateManagerCRUD:
    def test_create_template(self, template_manager):
        t = JobTemplate(name="custom", handler="custom.handler")
        result = template_manager.create_template(t)
        assert result.name == "custom"

    def test_get_template_by_name_builtin(self, template_manager):
        t = template_manager.get_template_by_name("health-check")
        assert t is not None
        assert t.handler == "health.check"

    def test_get_template_by_name_custom(self, template_manager):
        t = JobTemplate(name="my-template", handler="my.handler")
        template_manager.create_template(t)
        retrieved = template_manager.get_template_by_name("my-template")
        assert retrieved is not None
        assert retrieved.handler == "my.handler"

    def test_get_template_by_name_not_found(self, template_manager):
        assert template_manager.get_template_by_name("nonexistent") is None

    def test_list_templates_includes_builtins(self, template_manager):
        templates = template_manager.list_templates()
        assert len(templates) >= len(BUILTIN_TEMPLATES)
        names = [t.name for t in templates]
        assert "health-check" in names
        assert "daily-backup" in names

    def test_list_templates_with_custom(self, template_manager):
        template_manager.create_template(JobTemplate(name="custom1", handler="h1"))
        templates = template_manager.list_templates()
        names = [t.name for t in templates]
        assert "custom1" in names
        assert "health-check" in names  # Built-in still there

    def test_list_templates_by_category(self, template_manager):
        templates = template_manager.list_templates(category=TemplateCategory.MONITORING)
        for t in templates:
            assert t.category == TemplateCategory.MONITORING

    def test_delete_builtin_not_allowed(self, template_manager):
        # Built-ins cannot be deleted
        result = template_manager.delete_template("health-check")
        assert result is False
        assert template_manager.get_template_by_name("health-check") is not None

    def test_delete_custom_template(self, template_manager):
        t = JobTemplate(name="deletable", handler="h")
        template_manager.create_template(t)
        assert template_manager.delete_template(t.id) is True
        assert template_manager.get_template_by_name("deletable") is None

    def test_update_template(self, template_manager):
        t = JobTemplate(name="updateable", handler="h")
        template_manager.create_template(t)
        updated = template_manager.update_template(t.id, description="Updated desc")
        assert updated is not None
        assert updated.description == "Updated desc"


class TestTemplateManagerInstantiation:
    def test_instantiate_builtin(self, template_manager, scheduler):
        job = template_manager.instantiate(
            "health-check",
            payload={"endpoint": "https://api.example.com/health"},
        )
        assert job.handler == "health.check"
        assert job.priority == Priority.HIGH

    def test_instantiate_with_scheduler(self, template_manager, scheduler):
        job = template_manager.instantiate(
            "cleanup",
            payload={"target": "/tmp"},
        )
        scheduler.add_job(job)
        assert scheduler.get_job(job.id) is not None

    def test_instantiate_not_found(self, template_manager):
        with pytest.raises(ValueError, match="Template not found"):
            template_manager.instantiate("nonexistent")

    def test_instantiate_missing_required(self, template_manager):
        with pytest.raises(ValueError, match="Missing required fields"):
            template_manager.instantiate("health-check")

    def test_instantiate_custom_template(self, template_manager):
        t = JobTemplate(
            name="my-pipeline",
            handler="pipeline.run",
            default_cron="0 */4 * * *",
            default_tags=["pipeline"],
            default_payload={"pipeline_id": "default", "stages": []},
        )
        template_manager.create_template(t)
        job = template_manager.instantiate("my-pipeline")
        assert job.handler == "pipeline.run"
        assert "pipeline" in job.tags


class TestTemplatePersistence:
    def test_template_persisted(self, tmp_path):
        data_dir = str(tmp_path / "persist-test")
        store1 = JSONJobStore(data_dir=data_dir)
        manager1 = TemplateManager(store=store1)
        t = JobTemplate(name="persistent", handler="h", description="Persists")
        manager1.create_template(t)

        # New manager with same store should find it
        store2 = JSONJobStore(data_dir=data_dir)
        manager2 = TemplateManager(store=store2)
        retrieved = manager2.get_template_by_name("persistent")
        assert retrieved is not None
        assert retrieved.description == "Persists"
