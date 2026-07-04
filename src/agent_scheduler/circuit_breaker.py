"""Circuit Breaker pattern for job handlers.

When a handler repeatedly fails, the circuit "trips" to stop sending
work to it for a cooldown period. After the cooldown, the circuit
enters a "half-open" state where a single trial execution is allowed.
If that succeeds, the circuit closes (resumes normal operation).
If it fails, the circuit re-opens for another cooldown.

States::

    CLOSED    →  normal operation, failures counted
    OPEN      →  all executions blocked, waiting for cooldown
    HALF_OPEN →  single trial execution permitted

This protects flaky downstream services from being hammered by
the retry loop, and prevents cascading failures.
"""

from __future__ import annotations

import time
import logging
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

__all__ = [
    "CircuitState",
    "CircuitConfig",
    "CircuitBreaker",
    "CircuitBreakerRegistry",
]


class CircuitState(str, Enum):
    """Circuit breaker states."""

    CLOSED = "closed"       # Normal operation
    OPEN = "open"           # Tripped — blocking all calls
    HALF_OPEN = "half_open" # Trial period — one call allowed


class CircuitConfig(BaseModel):
    """Configuration for a circuit breaker."""

    failure_threshold: int = Field(
        default=5,
        ge=1,
        description="Consecutive failures before tripping",
    )
    cooldown_seconds: float = Field(
        default=60,
        ge=0,
        description="Seconds to stay open before half-open trial",
    )
    success_threshold: int = Field(
        default=2,
        ge=1,
        description="Consecutive successes in half-open to close circuit",
    )
    half_open_max_calls: int = Field(
        default=1,
        ge=1,
        description="Max concurrent trial calls in half-open state",
    )

    @property
    def is_default(self) -> bool:
        return (
            self.failure_threshold == 5
            and self.cooldown_seconds == 60
            and self.success_threshold == 2
            and self.half_open_max_calls == 1
        )


class CircuitBreaker:
    """A single circuit breaker tracking one handler or job.

    Usage::

        cb = CircuitBreaker("my-handler", config=CircuitConfig(...))

        if not cb.allow():
            raise RuntimeError("Circuit is open — handler unavailable")

        try:
            result = do_work()
            cb.record_success()
        except Exception:
            cb.record_failure()
            raise
    """

    def __init__(
        self,
        name: str,
        config: Optional[CircuitConfig] = None,
    ) -> None:
        self.name = name
        self.config = config or CircuitConfig()

        self._state: CircuitState = CircuitState.CLOSED
        self._consecutive_failures: int = 0
        self._consecutive_successes: int = 0
        self._opened_at: float = 0.0
        self._half_open_calls_in_flight: int = 0

        # Stats
        self.total_successes: int = 0
        self.total_failures: int = 0
        self.times_tripped: int = 0
        self.times_recovered: int = 0
        self.last_failure_time: float = 0.0
        self.last_success_time: float = 0.0
        self.last_error: Optional[str] = None

    # ── State queries ────────────────────────────────────────

    @property
    def state(self) -> CircuitState:
        """Current state, auto-transitioning OPEN → HALF_OPEN if cooldown elapsed."""
        if self._state == CircuitState.OPEN:
            if time.monotonic() - self._opened_at >= self.config.cooldown_seconds:
                self._transition(CircuitState.HALF_OPEN)
        return self._state

    @property
    def is_closed(self) -> bool:
        return self.state == CircuitState.CLOSED

    @property
    def is_open(self) -> bool:
        return self.state == CircuitState.OPEN

    @property
    def is_half_open(self) -> bool:
        return self.state == CircuitState.HALF_OPEN

    # ── Core operations ───────────────────────────────────────

    def allow(self) -> bool:
        """Check if an execution is allowed right now.

        Returns True if the call should proceed, False if blocked
        (circuit is OPEN or HALF_OPEN has too many trial calls).
        """
        current_state = self.state

        if current_state == CircuitState.CLOSED:
            return True

        if current_state == CircuitState.HALF_OPEN:
            if self._half_open_calls_in_flight < self.config.half_open_max_calls:
                self._half_open_calls_in_flight += 1
                return True
            return False

        # OPEN
        return False

    def record_success(self) -> None:
        """Record a successful execution."""
        self.total_successes += 1
        self.last_success_time = time.time()

        if self._state == CircuitState.HALF_OPEN:
            self._half_open_calls_in_flight = max(0, self._half_open_calls_in_flight - 1)
            self._consecutive_successes += 1
            if self._consecutive_successes >= self.config.success_threshold:
                self._transition(CircuitState.CLOSED)
                logger.info(
                    f"Circuit '{self.name}' CLOSED after {self._consecutive_successes} "
                    f"consecutive successes in half-open"
                )
        elif self._state == CircuitState.CLOSED:
            self._consecutive_failures = 0  # Reset on success

    def record_failure(self, error: Optional[str] = None) -> None:
        """Record a failed execution."""
        self.total_failures += 1
        self.last_failure_time = time.time()
        self.last_error = error
        self._consecutive_successes = 0

        if self._state == CircuitState.HALF_OPEN:
            self._half_open_calls_in_flight = max(0, self._half_open_calls_in_flight - 1)
            # A failure in half-open immediately re-opens the circuit
            self._trip()
            logger.warning(
                f"Circuit '{self.name}' re-OPENED: half-open trial failed"
                + (f" ({error})" if error else "")
            )
        elif self._state == CircuitState.CLOSED:
            self._consecutive_failures += 1
            if self._consecutive_failures >= self.config.failure_threshold:
                self._trip()
                logger.warning(
                    f"Circuit '{self.name}' OPENED after {self._consecutive_failures} "
                    f"consecutive failures"
                )

    # ── Transitions ───────────────────────────────────────────

    def _trip(self) -> None:
        """Trip the circuit into OPEN state."""
        self._transition(CircuitState.OPEN)
        self.times_tripped += 1

    def _transition(self, new_state: CircuitState) -> None:
        old = self._state
        if old == new_state:
            return
        self._state = new_state
        if new_state == CircuitState.OPEN:
            self._opened_at = time.monotonic()
        elif new_state == CircuitState.HALF_OPEN:
            self._consecutive_successes = 0
            self._half_open_calls_in_flight = 0
        elif new_state == CircuitState.CLOSED:
            self._consecutive_failures = 0
            self._consecutive_successes = 0
            if old == CircuitState.HALF_OPEN:
                self.times_recovered += 1

    def reset(self) -> None:
        """Manually force the circuit to CLOSED (admin override)."""
        self._transition(CircuitState.CLOSED)
        logger.info(f"Circuit '{self.name}' manually reset to CLOSED")

    # ── Serialization ─────────────────────────────────────────

    def to_dict(self) -> dict[str, Any]:
        """Serialize breaker state for reporting."""
        return {
            "name": self.name,
            "state": self.state.value,
            "consecutive_failures": self._consecutive_failures,
            "consecutive_successes": self._consecutive_successes,
            "config": self.config.model_dump(),
            "stats": {
                "total_successes": self.total_successes,
                "total_failures": self.total_failures,
                "times_tripped": self.times_tripped,
                "times_recovered": self.times_recovered,
                "last_failure_time": self.last_failure_time or None,
                "last_success_time": self.last_success_time or None,
                "last_error": self.last_error,
            },
        }


class CircuitBreakerRegistry:
    """Registry of circuit breakers, keyed by handler name.

    The scheduler uses this to check whether a handler's circuit
    is closed before attempting execution.

    Usage in scheduler::

        cb_registry = CircuitBreakerRegistry()
        # ...
        cb = cb_registry.get_or_create(job.handler)
        if not cb.allow():
            # Skip execution — circuit is open
            return HandlerResult(success=False, error="Circuit open")
        try:
            result = handler(payload)
            if result.success:
                cb.record_success()
            else:
                cb.record_failure(result.error)
        except Exception as e:
            cb.record_failure(str(e))
            raise
    """

    def __init__(self, default_config: Optional[CircuitConfig] = None) -> None:
        self._breakers: dict[str, CircuitBreaker] = {}
        self._default_config = default_config or CircuitConfig()

    def get_or_create(
        self,
        name: str,
        config: Optional[CircuitConfig] = None,
    ) -> CircuitBreaker:
        """Get an existing breaker or create a new one."""
        if name not in self._breakers:
            self._breakers[name] = CircuitBreaker(
                name=name,
                config=config or self._default_config,
            )
        return self._breakers[name]

    def get(self, name: str) -> Optional[CircuitBreaker]:
        """Get an existing breaker (None if not found)."""
        return self._breakers.get(name)

    def remove(self, name: str) -> bool:
        """Remove a breaker from the registry."""
        return self._breakers.pop(name, None) is not None

    def list_breakers(self) -> list[CircuitBreaker]:
        """List all registered breakers."""
        return list(self._breakers.values())

    def list_by_state(self, state: CircuitState) -> list[CircuitBreaker]:
        """List breakers filtered by state."""
        return [cb for cb in self._breakers.values() if cb.state == state]

    def reset_all(self) -> int:
        """Force-reset all breakers to CLOSED. Returns count."""
        count = 0
        for cb in self._breakers.values():
            if cb.state != CircuitState.CLOSED:
                cb.reset()
                count += 1
        return count

    def stats(self) -> dict[str, Any]:
        """Aggregate stats across all breakers."""
        breakers = list(self._breakers.values())
        return {
            "total_breakers": len(breakers),
            "closed": sum(1 for b in breakers if b.is_closed),
            "open": sum(1 for b in breakers if b.is_open),
            "half_open": sum(1 for b in breakers if b.is_half_open),
            "total_tripped": sum(b.times_tripped for b in breakers),
            "total_recovered": sum(b.times_recovered for b in breakers),
        }
