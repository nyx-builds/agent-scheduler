"""Tests for handler registry."""

import pytest
import asyncio

from agent_scheduler.handler import HandlerRegistry, HandlerResult


class TestHandlerResult:
    def test_success_result(self):
        result = HandlerResult(success=True, data={"key": "value"})
        assert result.success is True
        assert result.data == {"key": "value"}
        assert result.error is None
        d = result.to_dict()
        assert d["success"] is True
        assert d["data"] == {"key": "value"}

    def test_failure_result(self):
        result = HandlerResult(success=False, error="Something went wrong")
        assert result.success is False
        assert result.error == "Something went wrong"
        d = result.to_dict()
        assert d["success"] is False
        assert d["error"] == "Something went wrong"


class TestHandlerRegistry:
    def test_register_sync_handler(self):
        registry = HandlerRegistry()

        def my_handler(payload):
            return {"ok": True}

        registry.register("my.handler", my_handler)
        assert registry.has_handler("my.handler")
        assert "my.handler" in registry.list_handlers()

    def test_register_async_handler(self):
        registry = HandlerRegistry()

        async def my_async_handler(payload):
            return {"ok": True}

        registry.register("my.async", my_async_handler)
        assert registry.has_handler("my.async")

    def test_unregister_handler(self):
        registry = HandlerRegistry()

        def my_handler(payload):
            return {"ok": True}

        registry.register("my.handler", my_handler)
        registry.unregister("my.handler")
        assert not registry.has_handler("my.handler")

    def test_list_handlers(self):
        registry = HandlerRegistry()

        def h1(payload):
            return {}

        async def h2(payload):
            return {}

        registry.register("handler1", h1)
        registry.register("handler2", h2)
        handlers = registry.list_handlers()
        assert "handler1" in handlers
        assert "handler2" in handlers

    @pytest.mark.asyncio
    async def test_execute_sync_handler(self):
        registry = HandlerRegistry()

        def greet(payload):
            return {"greeting": f"Hello, {payload.get('name', 'World')}!"}

        registry.register("greet", greet)
        result = await registry.execute("greet", {"name": "Agent"})
        assert result.success is True
        assert result.data["greeting"] == "Hello, Agent!"

    @pytest.mark.asyncio
    async def test_execute_async_handler(self):
        registry = HandlerRegistry()

        async def async_greet(payload):
            return {"greeting": f"Hello, {payload.get('name', 'World')}!"}

        registry.register("async_greet", async_greet)
        result = await registry.execute("async_greet", {"name": "Agent"})
        assert result.success is True
        assert result.data["greeting"] == "Hello, Agent!"

    @pytest.mark.asyncio
    async def test_execute_handler_returning_handler_result(self):
        registry = HandlerRegistry()

        def custom_handler(payload):
            return HandlerResult(success=True, data={"custom": True})

        registry.register("custom", custom_handler)
        result = await registry.execute("custom", {})
        assert result.success is True
        assert result.data["custom"] is True

    @pytest.mark.asyncio
    async def test_execute_failing_handler(self):
        registry = HandlerRegistry()

        def failing_handler(payload):
            raise RuntimeError("Intentional failure")

        registry.register("fail", failing_handler)
        result = await registry.execute("fail", {})
        assert result.success is False
        assert "Intentional failure" in result.error

    @pytest.mark.asyncio
    async def test_execute_unregistered_simulates(self):
        registry = HandlerRegistry()
        result = await registry.execute("nonexistent.handler", {"key": "value"})
        assert result.success is True
        assert result.data.get("simulated") is True
        assert result.data.get("handler") == "nonexistent.handler"

    @pytest.mark.asyncio
    async def test_execute_handler_timeout(self):
        registry = HandlerRegistry()

        async def slow_handler(payload):
            await asyncio.sleep(10)
            return {"ok": True}

        registry.register("slow", slow_handler)
        result = await registry.execute("slow", {}, timeout=0.01)
        assert result.success is False
        assert "timed out" in result.error

    @pytest.mark.asyncio
    async def test_execute_handler_string_result(self):
        registry = HandlerRegistry()

        def string_handler(payload):
            return "simple string result"

        registry.register("string", string_handler)
        result = await registry.execute("string", {})
        assert result.success is True
        assert result.data["result"] == "simple string result"
