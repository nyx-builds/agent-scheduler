"""Tests for Result Chaining & Pipelines — v0.5.0 feature."""

import pytest
import asyncio
from datetime import datetime, timezone

from agent_scheduler.result_chain import (
    ChainStep,
    Pipeline,
    PipelineStatus,
    ResultChainManager,
    ResultConfig,
    ResultMergeStrategy,
)
from agent_scheduler.models import (
    ExecutionStatus,
    Job,
    JobDependency,
    JobStatus,
    Priority,
)
from agent_scheduler.scheduler import Scheduler
from agent_scheduler.store import JSONJobStore


# ── Fixtures ──────────────────────────────────────────────────


@pytest.fixture
def store(tmp_path):
    return JSONJobStore(data_dir=str(tmp_path / "chain-test"))


@pytest.fixture
def scheduler(store):
    return Scheduler(store=store)


@pytest.fixture
def manager(scheduler):
    return ResultChainManager(scheduler=scheduler)


@pytest.fixture
def standalone_manager():
    """Manager without scheduler for basic tests."""
    return ResultChainManager(scheduler=None)


# ── ResultConfig Tests ────────────────────────────────────────


class TestResultConfig:
    def test_defaults(self):
        config = ResultConfig()
        assert config.result_keys is None
        assert config.merge_strategy == ResultMergeStrategy.MERGE
        assert config.key_prefix == "parent_"
        assert config.wrap_key is None

    def test_apply_merge_strategy(self):
        config = ResultConfig(merge_strategy=ResultMergeStrategy.MERGE)
        result = config.apply(
            parent_result={"a": 1, "b": 2},
            child_payload={"b": 99, "c": 3},
        )
        # Parent wins on conflict
        assert result == {"a": 1, "b": 2, "c": 3}

    def test_apply_child_first_strategy(self):
        config = ResultConfig(merge_strategy=ResultMergeStrategy.CHILD_FIRST)
        result = config.apply(
            parent_result={"a": 1, "b": 2},
            child_payload={"b": 99, "c": 3},
        )
        # Child wins on conflict
        assert result == {"a": 1, "b": 99, "c": 3}

    def test_apply_replace_strategy(self):
        config = ResultConfig(merge_strategy=ResultMergeStrategy.REPLACE)
        result = config.apply(
            parent_result={"a": 1},
            child_payload={"b": 2, "c": 3},
        )
        # Parent entirely replaces
        assert result == {"a": 1}

    def test_apply_prefix_strategy(self):
        config = ResultConfig(
            merge_strategy=ResultMergeStrategy.PREFIX,
            key_prefix="prev_",
        )
        result = config.apply(
            parent_result={"url": "https://example.com", "status": 200},
            child_payload={"action": "process"},
        )
        assert result == {
            "action": "process",
            "prev_url": "https://example.com",
            "prev_status": 200,
        }

    def test_apply_with_result_keys(self):
        config = ResultConfig(
            result_keys=["data", "count"],
            merge_strategy=ResultMergeStrategy.MERGE,
        )
        result = config.apply(
            parent_result={"data": [1, 2, 3], "count": 3, "internal": "skip"},
            child_payload={"task": "validate"},
        )
        assert result == {"data": [1, 2, 3], "count": 3, "task": "validate"}
        assert "internal" not in result

    def test_apply_with_result_keys_missing(self):
        """Missing keys in parent result are simply not included."""
        config = ResultConfig(result_keys=["a", "missing"])
        result = config.apply(
            parent_result={"a": 1, "b": 2},
            child_payload={},
        )
        assert result == {"a": 1}

    def test_apply_with_wrap_key(self):
        config = ResultConfig(wrap_key="parent_result")
        result = config.apply(
            parent_result={"a": 1, "b": 2},
            child_payload={"own": "data"},
        )
        assert result == {
            "own": "data",
            "parent_result": {"a": 1, "b": 2},
        }

    def test_apply_replace_with_keys(self):
        config = ResultConfig(
            result_keys=["data"],
            merge_strategy=ResultMergeStrategy.REPLACE,
        )
        result = config.apply(
            parent_result={"data": "hello", "extra": "ignored"},
            child_payload={"existing": "data"},
        )
        assert result == {"data": "hello"}

    def test_does_not_mutate_inputs(self):
        """Verify apply() doesn't mutate the input dicts."""
        parent = {"a": 1}
        child = {"b": 2}
        config = ResultConfig(merge_strategy=ResultMergeStrategy.MERGE)
        config.apply(parent, child)
        assert parent == {"a": 1}
        assert child == {"b": 2}

    def test_all_merge_strategies(self):
        """Ensure all enum values are accessible."""
        assert ResultMergeStrategy.MERGE == "merge"
        assert ResultMergeStrategy.CHILD_FIRST == "child_first"
        assert ResultMergeStrategy.REPLACE == "replace"
        assert ResultMergeStrategy.PREFIX == "prefix"


# ── ResultChainManager Link Tests ─────────────────────────────


class TestChainLinks:
    def test_configure_and_get_link(self, manager):
        config = ResultConfig(merge_strategy=ResultMergeStrategy.PREFIX)
        manager.configure_link("parent1", "child1", config)

        fetched = manager.get_link_config("parent1", "child1")
        assert fetched is not None
        assert fetched.merge_strategy == ResultMergeStrategy.PREFIX

    def test_get_link_nonexistent(self, manager):
        assert manager.get_link_config("nope", "nope") is None

    def test_remove_link(self, manager):
        config = ResultConfig()
        manager.configure_link("p", "c", config)
        assert manager.remove_link("p", "c") is True
        assert manager.get_link_config("p", "c") is None

    def test_remove_link_nonexistent(self, manager):
        assert manager.remove_link("nope", "nope") is False

    def test_list_links(self, manager):
        manager.configure_link("p1", "c1", ResultConfig())
        manager.configure_link("p2", "c2", ResultConfig(merge_strategy=ResultMergeStrategy.REPLACE))

        links = manager.list_links()
        assert len(links) == 2
        assert links[0]["parent_job_id"] == "p1"
        assert links[1]["parent_job_id"] == "p2"

    def test_process_result_no_link(self, manager):
        """Without a configured link, child payload is returned unchanged."""
        result = manager.process_result(
            parent_job_id="p",
            child_job_id="c",
            parent_result={"data": "hello"},
            child_payload={"existing": "data"},
        )
        assert result == {"existing": "data"}

    def test_process_result_with_link(self, manager):
        manager.configure_link(
            "fetch_job",
            "process_job",
            ResultConfig(result_keys=["data"], merge_strategy=ResultMergeStrategy.MERGE),
        )
        result = manager.process_result(
            parent_job_id="fetch_job",
            child_job_id="process_job",
            parent_result={"data": [1, 2, 3], "internal": "skip"},
            child_payload={"action": "transform"},
        )
        assert result == {"data": [1, 2, 3], "action": "transform"}


# ── Pipeline Tests ────────────────────────────────────────────


class TestPipeline:
    def test_create_pipeline(self, manager):
        pipeline = manager.create_pipeline(name="ETL", description="Extract Transform Load")
        assert pipeline.id
        assert pipeline.name == "ETL"
        assert pipeline.description == "Extract Transform Load"
        assert pipeline.step_count == 0
        assert pipeline.is_empty is True

    def test_get_pipeline(self, manager):
        created = manager.create_pipeline(name="Test Pipeline")
        fetched = manager.get_pipeline(created.id)
        assert fetched is not None
        assert fetched.name == "Test Pipeline"

    def test_get_pipeline_by_name(self, manager):
        manager.create_pipeline(name="Named Pipeline")
        fetched = manager.get_pipeline_by_name("Named Pipeline")
        assert fetched is not None
        assert fetched.name == "Named Pipeline"

    def test_get_pipeline_nonexistent(self, manager):
        assert manager.get_pipeline("nonexistent") is None

    def test_list_pipelines(self, manager):
        manager.create_pipeline(name="P1")
        manager.create_pipeline(name="P2")
        pipelines = manager.list_pipelines()
        assert len(pipelines) == 2

    def test_delete_pipeline(self, manager):
        p = manager.create_pipeline(name="ToDelete")
        assert manager.delete_pipeline(p.id) is True
        assert manager.get_pipeline(p.id) is None

    def test_delete_pipeline_nonexistent(self, manager):
        assert manager.delete_pipeline("nonexistent") is False


# ── Pipeline Steps Tests ──────────────────────────────────────


class TestPipelineSteps:
    def test_add_step(self, manager):
        p = manager.create_pipeline(name="Multi-Step")
        step = manager.add_step(p.id, job_id="job1", step_name="Extract")
        assert step is not None
        assert step.job_id == "job1"
        assert step.step_name == "Extract"
        assert step.step_index == 0

    def test_add_multiple_steps(self, manager):
        p = manager.create_pipeline(name="Three Step")
        s1 = manager.add_step(p.id, "j1", "Extract")
        s2 = manager.add_step(p.id, "j2", "Transform")
        s3 = manager.add_step(p.id, "j3", "Load")

        pipeline = manager.get_pipeline(p.id)
        assert pipeline.step_count == 3
        assert pipeline.steps[0].step_index == 0
        assert pipeline.steps[1].step_index == 1
        assert pipeline.steps[2].step_index == 2

    def test_add_step_with_config(self, manager):
        p = manager.create_pipeline(name="With Config")
        config = ResultConfig(result_keys=["data"], merge_strategy=ResultMergeStrategy.REPLACE)
        step = manager.add_step(p.id, "j1", "Step 1", result_config=config)

        assert step.result_config is not None
        assert step.result_config.merge_strategy == ResultMergeStrategy.REPLACE

    def test_add_step_nonexistent_pipeline(self, manager):
        result = manager.add_step("nonexistent", "j1", "Step")
        assert result is None

    def test_step_default_name(self, manager):
        p = manager.create_pipeline(name="Default Names")
        step = manager.add_step(p.id, "j1")
        assert step.step_name == "Step 1"


# ── Pipeline Execution Tests ──────────────────────────────────


class TestPipelineExecution:
    def test_start_pipeline(self, manager):
        p = manager.create_pipeline(name="Runnable")
        manager.add_step(p.id, "j1", "First")
        manager.add_step(p.id, "j2", "Second")

        status = manager.start_pipeline(p.id)
        assert status is not None
        assert status.total_steps == 2
        assert status.completed_steps == 0
        assert status.current_step == "First"
        assert status.progress_pct == 0.0
        assert status.is_complete is False
        assert status.started_at is not None

    def test_start_empty_pipeline(self, manager):
        p = manager.create_pipeline(name="Empty")
        status = manager.start_pipeline(p.id)
        assert status is not None
        assert status.total_steps == 0
        assert status.progress_pct == 0.0

    def test_start_nonexistent_pipeline(self, manager):
        assert manager.start_pipeline("nonexistent") is None

    def test_record_success_result(self, manager):
        p = manager.create_pipeline(name="Success Flow")
        manager.add_step(p.id, "j1", "Step 1")
        manager.add_step(p.id, "j2", "Step 2")

        manager.start_pipeline(p.id)
        status = manager.record_step_result(
            p.id, step_index=0, result={"output": "step1 data"}, success=True,
        )

        assert status.completed_steps == 1
        assert status.progress_pct == 50.0
        assert status.current_step == "Step 2"
        assert status.is_complete is False

    def test_record_all_steps_complete(self, manager):
        p = manager.create_pipeline(name="Complete Flow")
        manager.add_step(p.id, "j1", "Only Step")

        manager.start_pipeline(p.id)
        status = manager.record_step_result(
            p.id, step_index=0, result={"done": True}, success=True,
        )

        assert status.completed_steps == 1
        assert status.progress_pct == 100.0
        assert status.is_complete is True
        assert status.finished_at is not None

    def test_record_failure(self, manager):
        p = manager.create_pipeline(name="Fail Flow")
        manager.add_step(p.id, "j1", "Failing Step")
        manager.add_step(p.id, "j2", "Never Runs")

        manager.start_pipeline(p.id)
        status = manager.record_step_result(
            p.id, step_index=0, result={}, success=False,
        )

        assert status.is_complete is False
        assert status.failed_step == "Failing Step"
        assert status.finished_at is not None

    def test_get_next_step_payload_no_config(self, manager):
        p = manager.create_pipeline(name="No Chain")
        manager.add_step(p.id, "j1", "Step 1")
        manager.add_step(p.id, "j2", "Step 2")

        manager.start_pipeline(p.id)
        manager.record_step_result(p.id, 0, {"data": "from step 1"}, True)

        # No result_config on step 2, so base payload returned
        payload = manager.get_next_step_payload(p.id, 0, {"base": "payload"})
        assert payload == {"base": "payload"}

    def test_get_next_step_payload_with_config(self, manager):
        p = manager.create_pipeline(name="Chained")
        manager.add_step(p.id, "j1", "Producer")
        manager.add_step(
            p.id, "j2", "Consumer",
            result_config=ResultConfig(merge_strategy=ResultMergeStrategy.MERGE),
        )

        manager.start_pipeline(p.id)
        manager.record_step_result(p.id, 0, {"produced": "value"}, True)

        payload = manager.get_next_step_payload(p.id, 0, {"consume": True})
        assert payload == {"produced": "value", "consume": True}

    def test_list_pipeline_status(self, manager):
        p1 = manager.create_pipeline(name="P1")
        p2 = manager.create_pipeline(name="P2")
        manager.add_step(p1.id, "j1", "S1")
        manager.start_pipeline(p1.id)

        statuses = manager.list_pipeline_status()
        assert len(statuses) == 1
        assert statuses[0].pipeline_name == "P1"


# ── Persistence Tests ─────────────────────────────────────────


class TestChainPersistence:
    def test_link_persistence(self, store, scheduler):
        m1 = ResultChainManager(scheduler=scheduler)
        m1.configure_link("p1", "c1", ResultConfig(merge_strategy=ResultMergeStrategy.REPLACE))

        m2 = ResultChainManager(scheduler=scheduler)
        config = m2.get_link_config("p1", "c1")
        assert config is not None
        assert config.merge_strategy == ResultMergeStrategy.REPLACE

    def test_pipeline_persistence(self, store, scheduler):
        m1 = ResultChainManager(scheduler=scheduler)
        p = m1.create_pipeline(name="Persistent Pipeline")
        m1.add_step(p.id, "j1", "Step 1")

        m2 = ResultChainManager(scheduler=scheduler)
        pipelines = m2.list_pipelines()
        assert len(pipelines) == 1
        assert pipelines[0].name == "Persistent Pipeline"
        assert pipelines[0].step_count == 1


# ── Scheduler Integration Tests ───────────────────────────────


class TestSchedulerIntegration:
    def test_result_flows_through_dependency(self, store):
        """End-to-end: job A's result flows into job B via dependency + chaining."""
        scheduler = Scheduler(store=store)
        assert scheduler.result_chains is not None

        received_payloads = []

        def producer(payload):
            return {"items": [1, 2, 3], "count": 3}

        def consumer(payload):
            received_payloads.append(dict(payload))
            return {"processed": True}

        scheduler.handlers.register("producer", producer)
        scheduler.handlers.register("consumer", consumer)

        # Create jobs
        job_a = Job(name="Producer", handler="producer", delay=0)
        job_b = Job(name="Consumer", handler="consumer", delay=0, payload={"task": "process"})
        scheduler.add_job(job_a)
        scheduler.add_job(job_b)

        # Add dependency: B runs after A succeeds
        scheduler.add_dependency(job_b.id, job_a.id, ExecutionStatus.SUCCESS)

        # Configure result chaining
        scheduler.result_chains.configure_link(
            job_a.id, job_b.id,
            ResultConfig(merge_strategy=ResultMergeStrategy.MERGE),
        )

        # Execute producer — this triggers consumer's scheduling
        asyncio.run(scheduler.run_job(job_a.id))

        # Now run due jobs — consumer should execute with merged payload
        asyncio.run(scheduler.run_due_jobs())

        assert len(received_payloads) == 1
        assert received_payloads[0]["items"] == [1, 2, 3]
        assert received_payloads[0]["count"] == 3
        assert received_payloads[0]["task"] == "process"

    def test_result_chaining_disabled(self, store):
        """Without result chaining, dependency still triggers but no data flows."""
        scheduler = Scheduler(store=store, enable_result_chaining=False)
        assert scheduler.result_chains is None

        received = []

        def producer(payload):
            return {"secret": "data"}

        def consumer(payload):
            received.append(dict(payload))
            return {}

        scheduler.handlers.register("p2", producer)
        scheduler.handlers.register("c2", consumer)

        job_a = Job(name="P2", handler="p2", delay=0)
        job_b = Job(name="C2", handler="c2", delay=0, payload={"own": "payload"})
        scheduler.add_job(job_a)
        scheduler.add_job(job_b)
        scheduler.add_dependency(job_b.id, job_a.id, ExecutionStatus.SUCCESS)

        asyncio.run(scheduler.run_job(job_a.id))
        asyncio.run(scheduler.run_due_jobs())

        # Consumer runs but without parent result
        assert len(received) == 1
        assert received[0] == {"own": "payload"}
        assert "secret" not in received[0]

    def test_partial_result_keys_flow(self, store):
        """Only configured result_keys flow to the child."""
        scheduler = Scheduler(store=store)

        received = []

        def producer(payload):
            return {"public": "share", "private": "secret", "count": 42}

        def consumer(payload):
            received.append(dict(payload))
            return {}

        scheduler.handlers.register("p3", producer)
        scheduler.handlers.register("c3", consumer)

        job_a = Job(name="P3", handler="p3", delay=0)
        job_b = Job(name="C3", handler="c3", delay=0, payload={"task": "summarize"})
        scheduler.add_job(job_a)
        scheduler.add_job(job_b)
        scheduler.add_dependency(job_b.id, job_a.id, ExecutionStatus.SUCCESS)

        # Only pass 'public' and 'count', not 'private'
        scheduler.result_chains.configure_link(
            job_a.id, job_b.id,
            ResultConfig(result_keys=["public", "count"]),
        )

        asyncio.run(scheduler.run_job(job_a.id))
        asyncio.run(scheduler.run_due_jobs())

        assert len(received) == 1
        assert received[0]["public"] == "share"
        assert received[0]["count"] == 42
        assert "private" not in received[0]
        assert received[0]["task"] == "summarize"
