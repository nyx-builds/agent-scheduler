"""Tests for the Circuit Breaker pattern (v0.6.0)."""

import time

import pytest

from agent_scheduler.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerRegistry,
    CircuitConfig,
    CircuitState,
)


# ── CircuitConfig ───────────────────────────────────────────


class TestCircuitConfig:
    def test_defaults(self):
        cfg = CircuitConfig()
        assert cfg.failure_threshold == 5
        assert cfg.cooldown_seconds == 60
        assert cfg.success_threshold == 2
        assert cfg.half_open_max_calls == 1
        assert cfg.is_default is True

    def test_custom_config(self):
        cfg = CircuitConfig(failure_threshold=3, cooldown_seconds=10)
        assert cfg.failure_threshold == 3
        assert cfg.cooldown_seconds == 10
        assert cfg.is_default is False

    def test_invalid_failure_threshold(self):
        with pytest.raises(Exception):
            CircuitConfig(failure_threshold=0)

    def test_invalid_cooldown(self):
        with pytest.raises(Exception):
            CircuitConfig(cooldown_seconds=-1)


# ── CircuitBreaker ──────────────────────────────────────────


class TestCircuitBreakerBasic:
    def test_starts_closed(self):
        cb = CircuitBreaker("test")
        assert cb.state == CircuitState.CLOSED
        assert cb.is_closed
        assert cb.allow() is True

    def test_name(self):
        cb = CircuitBreaker("my-handler")
        assert cb.name == "my-handler"

    def test_success_in_closed_resets_failures(self):
        cb = CircuitBreaker("test", CircuitConfig(failure_threshold=3))
        cb.record_failure("err1")
        cb.record_failure("err2")
        cb.record_success()
        # Should not trip after 2 more failures since counter reset
        cb.record_failure("err3")
        cb.record_failure("err4")
        assert cb.state == CircuitState.CLOSED


class TestCircuitBreakerTripping:
    def test_trips_after_threshold(self):
        cb = CircuitBreaker("test", CircuitConfig(failure_threshold=3))
        assert cb.is_closed

        cb.record_failure("e1")
        cb.record_failure("e2")
        assert cb.is_closed  # Not yet

        cb.record_failure("e3")
        assert cb.is_open
        assert cb.allow() is False

    def test_trip_stats(self):
        cb = CircuitBreaker("test", CircuitConfig(failure_threshold=2))
        cb.record_failure("e1")
        cb.record_failure("e2")
        assert cb.times_tripped == 1
        assert cb.total_failures == 2

    def test_error_message_stored(self):
        cb = CircuitBreaker("test", CircuitConfig(failure_threshold=2))
        cb.record_failure("network timeout")
        cb.record_failure("network timeout")
        assert cb.last_error == "network timeout"


class TestCircuitBreakerRecovery:
    def test_half_open_after_cooldown(self):
        cfg = CircuitConfig(failure_threshold=2, cooldown_seconds=0.1, success_threshold=1)
        cb = CircuitBreaker("test", cfg)

        # Trip the breaker
        cb.record_failure("e1")
        cb.record_failure("e2")
        assert cb.is_open

        # Wait for cooldown
        time.sleep(0.15)

        # Should transition to half-open on next state check
        assert cb.state == CircuitState.HALF_OPEN
        assert cb.allow() is True

    def test_half_open_success_closes_circuit(self):
        cfg = CircuitConfig(failure_threshold=2, cooldown_seconds=0.05, success_threshold=1)
        cb = CircuitBreaker("test", cfg)

        cb.record_failure("e1")
        cb.record_failure("e2")
        time.sleep(0.06)

        cb.allow()  # Triggers half-open
        cb.record_success()

        assert cb.is_closed
        assert cb.times_recovered == 1

    def test_half_open_failure_reopens(self):
        cfg = CircuitConfig(failure_threshold=2, cooldown_seconds=0.05, success_threshold=1)
        cb = CircuitBreaker("test", cfg)

        cb.record_failure("e1")
        cb.record_failure("e2")
        time.sleep(0.06)

        cb.allow()  # half-open
        cb.record_failure("e3")  # Failure in half-open → reopen

        assert cb.is_open
        assert cb.times_tripped == 2

    def test_multiple_successes_to_close(self):
        cfg = CircuitConfig(failure_threshold=1, cooldown_seconds=0.05, success_threshold=3)
        cb = CircuitBreaker("test", cfg)

        cb.record_failure("e1")
        assert cb.is_open
        time.sleep(0.06)

        # Need 3 consecutive successes to close
        cb.allow()
        cb.record_success()
        assert cb.is_half_open  # Not enough yet

        # In real life, the next poll cycle would allow() again
        cb._half_open_calls_in_flight = 0  # Simulate next cycle
        cb.allow()
        cb.record_success()
        assert cb.is_half_open  # Still need one more

        cb._half_open_calls_in_flight = 0
        cb.allow()
        cb.record_success()
        assert cb.is_closed


class TestCircuitBreakerReset:
    def test_manual_reset(self):
        cb = CircuitBreaker("test", CircuitConfig(failure_threshold=1))
        cb.record_failure("e1")
        assert cb.is_open

        cb.reset()
        assert cb.is_closed

    def test_reset_clears_counters(self):
        cb = CircuitBreaker("test", CircuitConfig(failure_threshold=2))
        cb.record_failure("e1")
        cb.record_failure("e2")
        cb.reset()
        # Should need 2 more failures to trip again
        cb.record_failure("e3")
        assert cb.is_closed
        cb.record_failure("e4")
        assert cb.is_open


class TestCircuitBreakerSerialization:
    def test_to_dict(self):
        cb = CircuitBreaker("test-handler", CircuitConfig(failure_threshold=3))
        cb.record_success()
        cb.record_failure("error")

        d = cb.to_dict()
        assert d["name"] == "test-handler"
        assert d["state"] == "closed"
        assert d["stats"]["total_successes"] == 1
        assert d["stats"]["total_failures"] == 1
        assert d["stats"]["last_error"] == "error"
        assert "config" in d
        assert d["config"]["failure_threshold"] == 3

    def test_to_dict_open_state(self):
        cb = CircuitBreaker("h", CircuitConfig(failure_threshold=1))
        cb.record_failure("boom")
        d = cb.to_dict()
        assert d["state"] == "open"
        assert d["stats"]["times_tripped"] == 1


# ── CircuitBreakerRegistry ──────────────────────────────────


class TestCircuitBreakerRegistry:
    def test_get_or_create(self):
        reg = CircuitBreakerRegistry()
        cb1 = reg.get_or_create("handler-a")
        cb2 = reg.get_or_create("handler-a")
        assert cb1 is cb2  # Same instance

    def test_get_returns_none_if_not_exists(self):
        reg = CircuitBreakerRegistry()
        assert reg.get("nonexistent") is None

    def test_list_breakers(self):
        reg = CircuitBreakerRegistry()
        reg.get_or_create("a")
        reg.get_or_create("b")
        assert len(reg.list_breakers()) == 2

    def test_remove(self):
        reg = CircuitBreakerRegistry()
        reg.get_or_create("a")
        assert reg.remove("a") is True
        assert reg.get("a") is None
        assert reg.remove("a") is False

    def test_list_by_state(self):
        reg = CircuitBreakerRegistry()
        cb_open = reg.get_or_create("flaky", CircuitConfig(failure_threshold=1))
        reg.get_or_create("stable")

        cb_open.record_failure("e1")
        assert len(reg.list_by_state(CircuitState.OPEN)) == 1
        assert len(reg.list_by_state(CircuitState.CLOSED)) == 1

    def test_reset_all(self):
        reg = CircuitBreakerRegistry()
        cb1 = reg.get_or_create("a", CircuitConfig(failure_threshold=1))
        cb2 = reg.get_or_create("b", CircuitConfig(failure_threshold=1))

        cb1.record_failure("e")
        cb2.record_failure("e")
        assert len(reg.list_by_state(CircuitState.OPEN)) == 2

        count = reg.reset_all()
        assert count == 2
        assert len(reg.list_by_state(CircuitState.CLOSED)) == 2

    def test_reset_all_no_change_for_closed(self):
        reg = CircuitBreakerRegistry()
        reg.get_or_create("stable")
        count = reg.reset_all()
        assert count == 0  # Already closed

    def test_stats(self):
        reg = CircuitBreakerRegistry()
        cb = reg.get_or_create("a", CircuitConfig(failure_threshold=2))
        cb.record_failure("e1")
        cb.record_failure("e2")

        stats = reg.stats()
        assert stats["total_breakers"] == 1
        assert stats["open"] == 1
        assert stats["closed"] == 0
        assert stats["total_tripped"] == 1

    def test_default_config_applied(self):
        cfg = CircuitConfig(failure_threshold=10, cooldown_seconds=30)
        reg = CircuitBreakerRegistry(default_config=cfg)
        cb = reg.get_or_create("handler")
        assert cb.config.failure_threshold == 10


# ── Integration with Scheduler ──────────────────────────────


class TestSchedulerCircuitBreakerIntegration:
    @pytest.fixture
    def scheduler(self):
        from agent_scheduler.scheduler import Scheduler
        from agent_scheduler.store import JSONJobStore
        import tempfile
        tmpdir = tempfile.mkdtemp()
        store = JSONJobStore(data_dir=tmpdir)
        return Scheduler(store=store, enable_circuit_breaker=True)

    def test_scheduler_has_circuit_breakers(self, scheduler):
        assert scheduler.circuit_breakers is not None

    def test_get_circuit_breaker_status_empty(self, scheduler):
        assert scheduler.get_circuit_breaker_status() == []

    def test_configure_circuit_breaker(self, scheduler):
        from agent_scheduler.circuit_breaker import CircuitConfig
        scheduler.configure_circuit_breaker("my-handler", CircuitConfig(failure_threshold=7))
        status = scheduler.get_circuit_breaker("my-handler")
        assert status is not None
        assert status["config"]["failure_threshold"] == 7

    def test_reset_circuit_breaker(self, scheduler):
        from agent_scheduler.circuit_breaker import CircuitConfig
        scheduler.configure_circuit_breaker("h", CircuitConfig(failure_threshold=1))
        cb = scheduler.circuit_breakers.get("h")
        cb.record_failure("e")
        assert cb.is_open

        result = scheduler.reset_circuit_breaker("h")
        assert result is True
        assert cb.is_closed

    def test_reset_nonexistent_breaker(self, scheduler):
        assert scheduler.reset_circuit_breaker("nonexistent") is False

    def test_reset_all_breakers(self, scheduler):
        from agent_scheduler.circuit_breaker import CircuitConfig
        scheduler.configure_circuit_breaker("a", CircuitConfig(failure_threshold=1))
        scheduler.configure_circuit_breaker("b", CircuitConfig(failure_threshold=1))
        scheduler.circuit_breakers.get("a").record_failure("e")
        scheduler.circuit_breakers.get("b").record_failure("e")

        count = scheduler.reset_all_circuit_breakers()
        assert count == 2


class TestSchedulerBreakerBlocksExecution:
    @pytest.mark.asyncio
    async def test_open_circuit_skips_job(self):
        """When a circuit is OPEN, the scheduler should skip that job."""
        from agent_scheduler.scheduler import Scheduler
        from agent_scheduler.models import Job, Priority
        from agent_scheduler.store import JSONJobStore
        from agent_scheduler.circuit_breaker import CircuitConfig
        import tempfile

        tmpdir = tempfile.mkdtemp()
        store = JSONJobStore(data_dir=tmpdir)
        sched = Scheduler(store=store, enable_circuit_breaker=True)

        # Configure breaker with low threshold
        sched.configure_circuit_breaker(
            "failing-handler",
            CircuitConfig(failure_threshold=1, cooldown_seconds=60),
        )

        # Trip the breaker manually
        cb = sched.circuit_breakers.get("failing-handler")
        cb.record_failure("boom")
        assert cb.is_open

        # Create a job that would be due
        job = Job(name="test-job", handler="failing-handler", delay=0)
        job.next_run_at = None  # Will be set by add_job
        sched.add_job(job)

        # Run due jobs — should be skipped due to open circuit
        results = await sched.run_due_jobs()
        assert len(results) == 0  # Job was skipped
