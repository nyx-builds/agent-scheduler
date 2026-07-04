"""Tests for Conditional Execution (v0.6.0)."""

from datetime import datetime, timezone

import pytest

from agent_scheduler.conditions import (
    AndCondition,
    ConditionContext,
    ConditionEvaluationError,
    ConditionOperator,
    ConditionRule,
    evaluate_condition,
    NotCondition,
    OrCondition,
)


class TestConditionContext:
    def test_resolve_payload(self):
        ctx = ConditionContext(payload={"env": "prod", "level": 5})
        assert ctx.resolve_path("payload.env") == "prod"
        assert ctx.resolve_path("payload.level") == 5

    def test_resolve_nested_payload(self):
        ctx = ConditionContext(payload={"server": {"host": "api.example.com", "port": 8080}})
        assert ctx.resolve_path("payload.server.host") == "api.example.com"
        assert ctx.resolve_path("payload.server.port") == 8080

    def test_resolve_missing_key(self):
        ctx = ConditionContext(payload={"a": 1})
        assert ctx.resolve_path("payload.nonexistent") is None

    def test_resolve_last_result(self):
        ctx = ConditionContext(last_result={"success": True, "count": 42})
        assert ctx.resolve_path("last_result.success") is True
        assert ctx.resolve_path("last_result.count") == 42

    def test_resolve_last_result_none(self):
        ctx = ConditionContext(last_result=None)
        assert ctx.resolve_path("last_result.anything") is None

    def test_resolve_last_status(self):
        ctx = ConditionContext(last_status="success")
        assert ctx.resolve_path("last_status") == "success"

    def test_resolve_tags(self):
        ctx = ConditionContext(job_tags=["urgent", "production"])
        assert ctx.resolve_path("job_tags") == ["urgent", "production"]

    def test_resolve_counts(self):
        ctx = ConditionContext(run_count=10, fail_count=2)
        assert ctx.resolve_path("run_count") == 10
        assert ctx.resolve_path("fail_count") == 2

    def test_resolve_time_fields(self):
        dt = datetime(2026, 1, 15, 14, 30, tzinfo=timezone.utc)  # Thursday
        ctx = ConditionContext(now=dt)
        assert ctx.resolve_path("time.hour") == 14
        assert ctx.resolve_path("time.minute") == 30
        assert ctx.resolve_path("time.weekday") == 3  # Thursday
        assert ctx.resolve_path("time.day") == 15
        assert ctx.resolve_path("time.month") == 1
        assert ctx.resolve_path("time.year") == 2026


class TestConditionRule:
    def test_eq(self):
        rule = ConditionRule(field_path="payload.env", operator="eq", value="prod")
        ctx = ConditionContext(payload={"env": "prod"})
        assert rule.evaluate(ctx) is True

        ctx2 = ConditionContext(payload={"env": "staging"})
        assert rule.evaluate(ctx2) is False

    def test_ne(self):
        rule = ConditionRule(field_path="payload.env", operator="ne", value="prod")
        ctx = ConditionContext(payload={"env": "staging"})
        assert rule.evaluate(ctx) is True

    def test_gt(self):
        rule = ConditionRule(field_path="payload.count", operator="gt", value=10)
        assert rule.evaluate(ConditionContext(payload={"count": 15})) is True
        assert rule.evaluate(ConditionContext(payload={"count": 5})) is False
        assert rule.evaluate(ConditionContext(payload={"count": 10})) is False

    def test_gte(self):
        rule = ConditionRule(field_path="payload.count", operator="gte", value=10)
        assert rule.evaluate(ConditionContext(payload={"count": 10})) is True
        assert rule.evaluate(ConditionContext(payload={"count": 9})) is False

    def test_lt(self):
        rule = ConditionRule(field_path="payload.count", operator="lt", value=10)
        assert rule.evaluate(ConditionContext(payload={"count": 5})) is True
        assert rule.evaluate(ConditionContext(payload={"count": 15})) is False

    def test_lte(self):
        rule = ConditionRule(field_path="payload.count", operator="lte", value=10)
        assert rule.evaluate(ConditionContext(payload={"count": 10})) is True
        assert rule.evaluate(ConditionContext(payload={"count": 11})) is False

    def test_in(self):
        rule = ConditionRule(
            field_path="payload.region",
            operator="in",
            value=["us", "eu"],
        )
        assert rule.evaluate(ConditionContext(payload={"region": "us"})) is True
        assert rule.evaluate(ConditionContext(payload={"region": "ap"})) is False

    def test_not_in(self):
        rule = ConditionRule(
            field_path="payload.region",
            operator="not_in",
            value=["us", "eu"],
        )
        assert rule.evaluate(ConditionContext(payload={"region": "ap"})) is True
        assert rule.evaluate(ConditionContext(payload={"region": "us"})) is False

    def test_contains(self):
        rule = ConditionRule(
            field_path="payload.tags",
            operator="contains",
            value="urgent",
        )
        ctx = ConditionContext(payload={"tags": ["urgent", "prod"]})
        assert rule.evaluate(ctx) is True

        ctx2 = ConditionContext(payload={"tags": ["low"]})
        assert rule.evaluate(ctx2) is False

    def test_starts_with(self):
        rule = ConditionRule(
            field_path="payload.url",
            operator="starts_with",
            value="https://",
        )
        assert rule.evaluate(ConditionContext(payload={"url": "https://api.com"})) is True
        assert rule.evaluate(ConditionContext(payload={"url": "http://api.com"})) is False

    def test_ends_with(self):
        rule = ConditionRule(
            field_path="payload.filename",
            operator="ends_with",
            value=".py",
        )
        assert rule.evaluate(ConditionContext(payload={"filename": "test.py"})) is True
        assert rule.evaluate(ConditionContext(payload={"filename": "test.js"})) is False

    def test_matches_regex(self):
        rule = ConditionRule(
            field_path="payload.email",
            operator="matches_regex",
            value=r".+@example\.com",
        )
        assert rule.evaluate(ConditionContext(payload={"email": "user@example.com"})) is True
        assert rule.evaluate(ConditionContext(payload={"email": "user@other.com"})) is False

    def test_is_none(self):
        rule = ConditionRule(field_path="payload.optional", operator="is_none")
        assert rule.evaluate(ConditionContext(payload={})) is True
        assert rule.evaluate(ConditionContext(payload={"optional": "x"})) is False

    def test_is_not_none(self):
        rule = ConditionRule(field_path="payload.optional", operator="is_not_none")
        assert rule.evaluate(ConditionContext(payload={"optional": "x"})) is True
        assert rule.evaluate(ConditionContext(payload={})) is False

    def test_is_true(self):
        rule = ConditionRule(field_path="payload.enabled", operator="is_true")
        assert rule.evaluate(ConditionContext(payload={"enabled": True})) is True
        assert rule.evaluate(ConditionContext(payload={"enabled": False})) is False
        assert rule.evaluate(ConditionContext(payload={"enabled": "yes"})) is True

    def test_is_false(self):
        rule = ConditionRule(field_path="payload.enabled", operator="is_false")
        assert rule.evaluate(ConditionContext(payload={"enabled": False})) is True
        assert rule.evaluate(ConditionContext(payload={"enabled": True})) is False

    def test_unknown_operator_raises(self):
        rule = ConditionRule(field_path="payload.x", operator="bogus")
        with pytest.raises(ConditionEvaluationError):
            rule.evaluate(ConditionContext(payload={"x": 1}))

    def test_type_mismatch_returns_false(self):
        rule = ConditionRule(field_path="payload.x", operator="gt", value=10)
        # Comparing string > int should not crash, returns False
        ctx = ConditionContext(payload={"x": "not a number"})
        assert rule.evaluate(ctx) is False


class TestAndCondition:
    def test_all_true(self):
        cond = AndCondition(conditions=[
            ConditionRule(field_path="payload.a", operator="eq", value=1),
            ConditionRule(field_path="payload.b", operator="eq", value=2),
        ])
        ctx = ConditionContext(payload={"a": 1, "b": 2})
        assert cond.evaluate(ctx) is True

    def test_one_false(self):
        cond = AndCondition(conditions=[
            ConditionRule(field_path="payload.a", operator="eq", value=1),
            ConditionRule(field_path="payload.b", operator="eq", value=99),
        ])
        ctx = ConditionContext(payload={"a": 1, "b": 2})
        assert cond.evaluate(ctx) is False

    def test_empty_and_is_true(self):
        cond = AndCondition(conditions=[])
        assert cond.evaluate(ConditionContext()) is True


class TestOrCondition:
    def test_any_true(self):
        cond = OrCondition(conditions=[
            ConditionRule(field_path="payload.a", operator="eq", value=99),
            ConditionRule(field_path="payload.b", operator="eq", value=2),
        ])
        ctx = ConditionContext(payload={"a": 1, "b": 2})
        assert cond.evaluate(ctx) is True

    def test_all_false(self):
        cond = OrCondition(conditions=[
            ConditionRule(field_path="payload.a", operator="eq", value=99),
            ConditionRule(field_path="payload.b", operator="eq", value=99),
        ])
        ctx = ConditionContext(payload={"a": 1, "b": 2})
        assert cond.evaluate(ctx) is False

    def test_empty_or_is_false(self):
        cond = OrCondition(conditions=[])
        assert cond.evaluate(ConditionContext()) is False


class TestNotCondition:
    def test_inverts_true(self):
        cond = NotCondition(
            condition=ConditionRule(field_path="payload.x", operator="eq", value=1)
        )
        ctx = ConditionContext(payload={"x": 1})
        assert cond.evaluate(ctx) is False

    def test_inverts_false(self):
        cond = NotCondition(
            condition=ConditionRule(field_path="payload.x", operator="eq", value=1)
        )
        ctx = ConditionContext(payload={"x": 2})
        assert cond.evaluate(ctx) is True


class TestCompositeConditions:
    def test_complex_composite(self):
        """Run if (env=prod AND priority=high) OR (weekend)."""
        from datetime import datetime

        dt_weekday = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)  # Thursday
        dt_weekend = datetime(2026, 1, 3, 12, 0, tzinfo=timezone.utc)  # Saturday

        # env=prod AND priority=high
        prod_high = AndCondition(conditions=[
            ConditionRule(field_path="payload.env", operator="eq", value="prod"),
            ConditionRule(field_path="payload.priority", operator="eq", value="high"),
        ])

        # Weekend check
        weekend = ConditionRule(field_path="time.weekday", operator="in", value=[5, 6])

        full = OrCondition(conditions=[prod_high, weekend])

        # Weekday + prod/high → True
        ctx1 = ConditionContext(
            payload={"env": "prod", "priority": "high"},
            now=dt_weekday,
        )
        assert full.evaluate(ctx1) is True

        # Weekend regardless of payload → True
        ctx2 = ConditionContext(payload={"env": "staging"}, now=dt_weekend)
        assert full.evaluate(ctx2) is True

        # Weekday + staging/low → False
        ctx3 = ConditionContext(
            payload={"env": "staging", "priority": "low"},
            now=dt_weekday,
        )
        assert full.evaluate(ctx3) is False


class TestEvaluateConditionFromDict:
    def test_rule_from_dict(self):
        condition_dict = {
            "type": "rule",
            "field_path": "payload.env",
            "operator": "eq",
            "value": "prod",
        }
        ctx = ConditionContext(payload={"env": "prod"})
        assert evaluate_condition(condition_dict, ctx) is True

    def test_and_from_dict(self):
        condition_dict = {
            "type": "and",
            "conditions": [
                {"type": "rule", "field_path": "payload.a", "operator": "eq", "value": 1},
                {"type": "rule", "field_path": "payload.b", "operator": "eq", "value": 2},
            ],
        }
        ctx = ConditionContext(payload={"a": 1, "b": 2})
        assert evaluate_condition(condition_dict, ctx) is True

    def test_or_from_dict(self):
        condition_dict = {
            "type": "or",
            "conditions": [
                {"type": "rule", "field_path": "payload.a", "operator": "eq", "value": 99},
                {"type": "rule", "field_path": "payload.b", "operator": "eq", "value": 2},
            ],
        }
        ctx = ConditionContext(payload={"a": 1, "b": 2})
        assert evaluate_condition(condition_dict, ctx) is True

    def test_not_from_dict(self):
        condition_dict = {
            "type": "not",
            "condition": {"type": "rule", "field_path": "payload.x", "operator": "eq", "value": 1},
        }
        ctx = ConditionContext(payload={"x": 2})
        assert evaluate_condition(condition_dict, ctx) is True

    def test_nested_composite_from_dict(self):
        condition_dict = {
            "type": "or",
            "conditions": [
                {
                    "type": "and",
                    "conditions": [
                        {"type": "rule", "field_path": "payload.urgent", "operator": "is_true"},
                        {"type": "rule", "field_path": "payload.env", "operator": "eq", "value": "prod"},
                    ],
                },
                {"type": "rule", "field_path": "run_count", "operator": "lt", "value": 3},
            ],
        }
        # run_count=1 → True (second branch)
        ctx = ConditionContext(payload={"urgent": False, "env": "staging"}, run_count=1)
        assert evaluate_condition(condition_dict, ctx) is True

        # run_count=10, not urgent, staging → False
        ctx2 = ConditionContext(payload={"urgent": False, "env": "staging"}, run_count=10)
        assert evaluate_condition(condition_dict, ctx2) is False

    def test_unknown_type_raises(self):
        with pytest.raises(ConditionEvaluationError):
            evaluate_condition({"type": "bogus"}, ConditionContext())

    def test_default_type_is_rule(self):
        """If no 'type' is specified, it defaults to 'rule'."""
        condition_dict = {
            "field_path": "payload.x",
            "operator": "eq",
            "value": 1,
        }
        ctx = ConditionContext(payload={"x": 1})
        assert evaluate_condition(condition_dict, ctx) is True


class TestSchedulerConditionIntegration:
    @pytest.mark.asyncio
    async def test_job_with_condition_runs_when_true(self):
        """A job with an execution_condition that evaluates True should run."""
        from agent_scheduler.scheduler import Scheduler
        from agent_scheduler.models import Job
        from agent_scheduler.store import JSONJobStore
        import tempfile

        tmpdir = tempfile.mkdtemp()
        store = JSONJobStore(data_dir=tmpdir)
        sched = Scheduler(store=store, enable_circuit_breaker=False)

        job = Job(
            name="conditional-job",
            handler="test-handler",
            delay=0,
            execution_condition={
                "type": "rule",
                "field_path": "payload.run",
                "operator": "is_true",
            },
            payload={"run": True},
        )
        sched.add_job(job)

        results = await sched.run_due_jobs()
        assert len(results) == 1
        assert results[0].is_success

    @pytest.mark.asyncio
    async def test_job_with_condition_skipped_when_false(self):
        """A job with an execution_condition that evaluates False should be skipped."""
        from agent_scheduler.scheduler import Scheduler
        from agent_scheduler.models import Job, JobStatus
        from agent_scheduler.store import JSONJobStore
        from datetime import datetime, timezone
        import tempfile

        tmpdir = tempfile.mkdtemp()
        store = JSONJobStore(data_dir=tmpdir)
        sched = Scheduler(store=store, enable_circuit_breaker=False)

        job = Job(
            name="conditional-job",
            handler="test-handler",
            cron="* * * * *",  # Recurring so it gets rescheduled
            execution_condition={
                "type": "rule",
                "field_path": "payload.run",
                "operator": "is_true",
            },
            payload={"run": False},
        )
        sched.add_job(job)

        # Force the job to be due now
        job.next_run_at = datetime.now(timezone.utc)
        sched.store.save_job(job)

        results = await sched.run_due_jobs()
        assert len(results) == 0  # Skipped

        # Skip count should be tracked
        assert sched.get_condition_skip_count(job.id) == 1

        # Job should still be active (recurring)
        updated = sched.get_job(job.id)
        assert updated is not None
        assert updated.status != JobStatus.COMPLETED
