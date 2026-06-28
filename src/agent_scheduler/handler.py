"""Handler registry for job execution."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class HandlerResult:
    """Result from a handler execution."""

    def __init__(self, success: bool, data: Optional[dict[str, Any]] = None, error: Optional[str] = None):
        self.success = success
        self.data = data or {}
        self.error = error

    def to_dict(self) -> dict[str, Any]:
        result = {"success": self.success}
        if self.data:
            result["data"] = self.data
        if self.error:
            result["error"] = self.error
        return result


class HandlerRegistry:
    """Registry of handler functions that can be invoked by the scheduler."""

    def __init__(self) -> None:
        self._handlers: dict[str, Callable] = {}
        self._async_handlers: dict[str, Callable] = {}

    def register(self, name: str, func: Callable) -> None:
        """Register a handler function (sync or async)."""
        if asyncio.iscoroutinefunction(func):
            self._async_handlers[name] = func
            logger.debug(f"Registered async handler: {name}")
        else:
            self._handlers[name] = func
            logger.debug(f"Registered sync handler: {name}")

    def unregister(self, name: str) -> None:
        """Remove a handler from the registry."""
        self._handlers.pop(name, None)
        self._async_handlers.pop(name, None)

    def has_handler(self, name: str) -> bool:
        """Check if a handler is registered."""
        return name in self._handlers or name in self._async_handlers

    def list_handlers(self) -> list[str]:
        """List all registered handler names."""
        return sorted(set(self._handlers.keys()) | set(self._async_handlers.keys()))

    async def execute(self, handler_name: str, payload: dict[str, Any], timeout: float = 300) -> HandlerResult:
        """Execute a handler by name with the given payload."""
        if handler_name in self._async_handlers:
            try:
                result = await asyncio.wait_for(
                    self._async_handlers[handler_name](payload),
                    timeout=timeout,
                )
                if isinstance(result, HandlerResult):
                    return result
                if isinstance(result, dict):
                    return HandlerResult(success=True, data=result)
                return HandlerResult(success=True, data={"result": str(result)})
            except asyncio.TimeoutError:
                return HandlerResult(success=False, error=f"Handler '{handler_name}' timed out after {timeout}s")
            except Exception as e:
                return HandlerResult(success=False, error=str(e))

        if handler_name in self._handlers:
            try:
                loop = asyncio.get_event_loop()
                result = await asyncio.wait_for(
                    loop.run_in_executor(None, self._handlers[handler_name], payload),
                    timeout=timeout,
                )
                if isinstance(result, HandlerResult):
                    return result
                if isinstance(result, dict):
                    return HandlerResult(success=True, data=result)
                return HandlerResult(success=True, data={"result": str(result)})
            except asyncio.TimeoutError:
                return HandlerResult(success=False, error=f"Handler '{handler_name}' timed out after {timeout}s")
            except Exception as e:
                return HandlerResult(success=False, error=str(e))

        # No handler registered — simulate execution for MCP/CLI use
        logger.info(f"No handler registered for '{handler_name}', simulating execution")
        return HandlerResult(
            success=True,
            data={"simulated": True, "handler": handler_name, "payload_keys": list(payload.keys())},
        )


# Global handler registry
_default_registry = HandlerRegistry()


def get_default_registry() -> HandlerRegistry:
    """Get the global default handler registry."""
    return _default_registry
