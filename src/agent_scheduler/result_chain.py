"""Result chaining for job dependency pipelines.

When Job A triggers Job B via a dependency, A's result data can
automatically flow into B's payload. This enables real workflow
pipelines: fetch → transform → store, where each step's output
becomes the next step's input.

Supports:
- Automatic result passing (full result or specific keys)
- Result transformation functions
- Result merge strategies (replace, merge, prefix)
- Pipeline definitions (multi-step chains)
"""

from __future__ import annotations

import copy
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Optional
from uuid import uuid4

from pydantic import BaseModel, Field


class ResultMergeStrategy(str, Enum):
    """How to merge parent result data into child payload."""

    MERGE = "merge"          # Child payload + parent result (parent wins on conflict)
    CHILD_FIRST = "child_first"  # Child payload wins, parent fills gaps
    REPLACE = "replace"      # Parent result entirely replaces child payload
    PREFIX = "prefix"        # Parent result keys are prefixed before merging


class ResultConfig(BaseModel):
    """Configuration for how a dependency's result flows into a dependent job."""

    # What to pass from the parent's result
    result_keys: Optional[list[str]] = Field(
        default=None,
        description="Specific keys to extract from parent result (None = pass entire result)",
    )

    # How to merge the parent result into the child's payload
    merge_strategy: ResultMergeStrategy = Field(
        default=ResultMergeStrategy.MERGE,
        description="How parent result data combines with child payload",
    )

    # Key prefix for PREFIX strategy
    key_prefix: str = Field(
        default="parent_",
        description="Prefix for parent result keys when merge_strategy=PREFIX",
    )

    # Wrap the result under a specific key instead of flattening
    wrap_key: Optional[str] = Field(
        default=None,
        description="If set, parent result is nested under this key in child payload",
    )

    # Only pass the result if a condition is met
    condition: Optional[str] = Field(
        default=None,
        description="JMESPath-style condition — result passes only if truthy (not yet implemented, reserved)",
    )

    def apply(
        self,
        parent_result: dict[str, Any],
        child_payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Apply the result config to produce the child's final payload.

        Args:
            parent_result: The execution result from the parent job
            child_payload: The child job's existing payload

        Returns:
            The merged payload for the child job
        """
        # Extract specific keys if configured
        if self.result_keys:
            extracted = {k: parent_result[k] for k in self.result_keys if k in parent_result}
        else:
            extracted = copy.deepcopy(parent_result)

        # Wrap if configured
        if self.wrap_key:
            extracted = {self.wrap_key: extracted}

        # Apply merge strategy
        if self.merge_strategy == ResultMergeStrategy.REPLACE:
            return extracted

        elif self.merge_strategy == ResultMergeStrategy.MERGE:
            # Parent wins on conflict
            merged = copy.deepcopy(child_payload)
            merged.update(extracted)
            return merged

        elif self.merge_strategy == ResultMergeStrategy.CHILD_FIRST:
            # Child wins on conflict
            merged = copy.deepcopy(extracted)
            merged.update(child_payload)
            return merged

        elif self.merge_strategy == ResultMergeStrategy.PREFIX:
            merged = copy.deepcopy(child_payload)
            for key, value in extracted.items():
                merged[f"{self.key_prefix}{key}"] = value
            return merged

        return child_payload


class ChainStep(BaseModel):
    """A single step in a pipeline chain."""

    job_id: str = Field(..., description="Job ID for this step")
    step_name: str = Field(default="", description="Human-readable step name")
    step_index: int = Field(default=0, ge=0, description="Position in pipeline (0-indexed)")
    result_config: Optional[ResultConfig] = Field(
        default=None,
        description="How parent result flows into this step (None = no chaining)",
    )


class Pipeline(BaseModel):
    """A named pipeline of chained jobs.

    A pipeline defines a sequence of jobs where each step's result
    flows into the next step. Pipelines can be created, tracked,
    and monitored as a unit.
    """

    id: str = Field(default_factory=lambda: uuid4().hex[:12])
    name: str = Field(..., min_length=1, description="Pipeline name")
    description: str = Field(default="", description="What this pipeline does")
    steps: list[ChainStep] = Field(
        default_factory=list,
        description="Ordered list of pipeline steps",
    )
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # State tracking
    started: bool = Field(default=False, description="Whether the pipeline has been triggered")
    completed: bool = Field(default=False, description="Whether all steps completed")
    current_step_index: int = Field(default=0, description="Index of the currently executing step")

    def mark_updated(self) -> None:
        self.updated_at = datetime.now(timezone.utc)

    @property
    def step_count(self) -> int:
        return len(self.steps)

    @property
    def is_empty(self) -> bool:
        return len(self.steps) == 0


class PipelineStatus(BaseModel):
    """Runtime status of a pipeline execution."""

    pipeline_id: str
    pipeline_name: str
    total_steps: int
    completed_steps: int = 0
    failed_step: Optional[str] = None
    current_step: Optional[str] = None
    results: list[dict[str, Any]] = Field(default_factory=list)
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None

    @property
    def is_complete(self) -> bool:
        return self.completed_steps >= self.total_steps and self.failed_step is None

    @property
    def progress_pct(self) -> float:
        if self.total_steps == 0:
            return 0.0
        return round((self.completed_steps / self.total_steps) * 100, 1)


class ResultChainManager:
    """Manages result chaining between dependent jobs and pipelines.

    This integrates with the Scheduler's dependency system. When a
    job completes and triggers dependents, the chain manager processes
    the result according to the configured ResultConfig and injects
    it into the dependent job's payload before execution.

    Usage:
        manager = ResultChainManager(scheduler=sched)

        # Define how results flow
        manager.configure_link(
            parent_job_id="job_a",
            child_job_id="job_b",
            config=ResultConfig(
                result_keys=["data", "url"],
                merge_strategy=ResultMergeStrategy.MERGE,
            ),
        )

        # When job_a completes, its result automatically flows to job_b
    """

    def __init__(self, scheduler: Any = None) -> None:
        self._scheduler = scheduler
        self._links: dict[str, ResultConfig] = {}  # "parent_id:child_id" -> config
        self._pipelines: dict[str, Pipeline] = {}
        self._pipeline_status: dict[str, PipelineStatus] = {}
        self._load_from_store()

    def _get_store(self) -> Any:
        if self._scheduler is not None:
            return getattr(self._scheduler, "store", None)
        return None

    def _load_from_store(self) -> None:
        """Load links and pipelines from storage."""
        store = self._get_store()
        if store is None:
            return
        try:
            import json
            from pathlib import Path

            if hasattr(store, "data_dir"):
                base = Path(store.data_dir)

                # Load links
                links_file = base / "chain_links.json"
                if links_file.exists():
                    data = json.loads(links_file.read_text())
                    for key, config_dict in data.items():
                        self._links[key] = ResultConfig.model_validate(config_dict)

                # Load pipelines
                pipelines_file = base / "pipelines.json"
                if pipelines_file.exists():
                    data = json.loads(pipelines_file.read_text())
                    for p_dict in data:
                        pipeline = Pipeline.model_validate(p_dict)
                        self._pipelines[pipeline.id] = pipeline
        except Exception:
            pass

    def _save_links(self) -> None:
        store = self._get_store()
        if store is None:
            return
        try:
            import json
            from pathlib import Path

            if hasattr(store, "data_dir"):
                links_file = Path(store.data_dir) / "chain_links.json"
                links_file.write_text(
                    json.dumps(
                        {k: v.model_dump(mode="json") for k, v in self._links.items()},
                        indent=2,
                        default=str,
                    )
                )
        except Exception:
            pass

    def _save_pipelines(self) -> None:
        store = self._get_store()
        if store is None:
            return
        try:
            import json
            from pathlib import Path

            if hasattr(store, "data_dir"):
                pipelines_file = Path(store.data_dir) / "pipelines.json"
                pipelines_file.write_text(
                    json.dumps(
                        [p.model_dump(mode="json") for p in self._pipelines.values()],
                        indent=2,
                        default=str,
                    )
                )
        except Exception:
            pass

    # ── Link Management ───────────────────────────────────────

    @staticmethod
    def _link_key(parent_id: str, child_id: str) -> str:
        return f"{parent_id}:{child_id}"

    def configure_link(
        self,
        parent_job_id: str,
        child_job_id: str,
        config: ResultConfig,
    ) -> None:
        """Configure how a parent job's result flows into a child job.

        Args:
            parent_job_id: The job whose result will be passed
            child_job_id: The job that will receive the result
            config: Configuration for result passing
        """
        self._links[self._link_key(parent_job_id, child_job_id)] = config
        self._save_links()

    def get_link_config(
        self,
        parent_job_id: str,
        child_job_id: str,
    ) -> Optional[ResultConfig]:
        """Get the result configuration for a parent→child link."""
        return self._links.get(self._link_key(parent_job_id, child_job_id))

    def remove_link(self, parent_job_id: str, child_job_id: str) -> bool:
        """Remove a result chain link."""
        key = self._link_key(parent_job_id, child_job_id)
        if key in self._links:
            del self._links[key]
            self._save_links()
            return True
        return False

    def list_links(self) -> list[dict[str, Any]]:
        """List all configured chain links."""
        result = []
        for key, config in self._links.items():
            parent_id, child_id = key.split(":", 1)
            result.append({
                "parent_job_id": parent_id,
                "child_job_id": child_id,
                "config": config.model_dump(mode="json"),
            })
        return result

    # ── Result Processing ─────────────────────────────────────

    def process_result(
        self,
        parent_job_id: str,
        child_job_id: str,
        parent_result: dict[str, Any],
        child_payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Process a parent result and produce the child's final payload.

        If no link is configured, returns the child's original payload unchanged.

        Args:
            parent_job_id: The completed parent job ID
            child_job_id: The dependent child job ID
            parent_result: The parent job's execution result
            child_payload: The child job's existing payload

        Returns:
            The final payload to use for the child job
        """
        config = self.get_link_config(parent_job_id, child_job_id)
        if config is None:
            return child_payload
        return config.apply(parent_result, child_payload)

    # ── Pipeline Management ───────────────────────────────────

    def create_pipeline(
        self,
        name: str,
        description: str = "",
        steps: Optional[list[ChainStep]] = None,
    ) -> Pipeline:
        """Create a new pipeline.

        Args:
            name: Pipeline name
            description: What the pipeline does
            steps: Ordered list of pipeline steps

        Returns:
            The created Pipeline
        """
        pipeline = Pipeline(
            name=name,
            description=description,
            steps=steps or [],
        )
        self._pipelines[pipeline.id] = pipeline
        self._save_pipelines()
        return pipeline

    def get_pipeline(self, pipeline_id: str) -> Optional[Pipeline]:
        """Get a pipeline by ID."""
        return self._pipelines.get(pipeline_id)

    def get_pipeline_by_name(self, name: str) -> Optional[Pipeline]:
        """Get a pipeline by name."""
        for p in self._pipelines.values():
            if p.name == name:
                return p
        return None

    def list_pipelines(self) -> list[Pipeline]:
        """List all pipelines."""
        return list(self._pipelines.values())

    def delete_pipeline(self, pipeline_id: str) -> bool:
        """Delete a pipeline."""
        if pipeline_id in self._pipelines:
            del self._pipelines[pipeline_id]
            self._pipeline_status.pop(pipeline_id, None)
            self._save_pipelines()
            return True
        return False

    def add_step(
        self,
        pipeline_id: str,
        job_id: str,
        step_name: str = "",
        result_config: Optional[ResultConfig] = None,
    ) -> Optional[ChainStep]:
        """Add a step to an existing pipeline.

        Args:
            pipeline_id: Pipeline to add to
            job_id: Job ID for this step
            step_name: Human-readable step name
            result_config: How previous step's result flows in

        Returns:
            The added ChainStep, or None if pipeline not found
        """
        pipeline = self.get_pipeline(pipeline_id)
        if pipeline is None:
            return None

        step = ChainStep(
            job_id=job_id,
            step_name=step_name or f"Step {len(pipeline.steps) + 1}",
            step_index=len(pipeline.steps),
            result_config=result_config,
        )
        pipeline.steps.append(step)
        pipeline.mark_updated()
        self._save_pipelines()
        return step

    def start_pipeline(self, pipeline_id: str) -> Optional[PipelineStatus]:
        """Start pipeline execution tracking.

        This initializes the status tracker. The scheduler must
        separately trigger the first job.

        Args:
            pipeline_id: Pipeline to start

        Returns:
            PipelineStatus, or None if pipeline not found
        """
        pipeline = self.get_pipeline(pipeline_id)
        if pipeline is None:
            return None

        status = PipelineStatus(
            pipeline_id=pipeline.id,
            pipeline_name=pipeline.name,
            total_steps=len(pipeline.steps),
            started_at=datetime.now(timezone.utc),
        )

        if pipeline.steps:
            status.current_step = pipeline.steps[0].step_name

        self._pipeline_status[pipeline.id] = status

        pipeline.started = True
        pipeline.current_step_index = 0
        pipeline.mark_updated()
        self._save_pipelines()

        return status

    def record_step_result(
        self,
        pipeline_id: str,
        step_index: int,
        result: dict[str, Any],
        success: bool = True,
    ) -> Optional[PipelineStatus]:
        """Record the result of a completed pipeline step.

        Args:
            pipeline_id: Pipeline ID
            step_index: Index of the completed step
            result: Execution result data
            success: Whether the step succeeded

        Returns:
            Updated PipelineStatus, or None if not found
        """
        pipeline = self.get_pipeline(pipeline_id)
        if pipeline is None:
            return None

        status = self._pipeline_status.get(pipeline_id)
        if status is None:
            return None

        status.results.append(result)

        if success:
            status.completed_steps += 1
            pipeline.current_step_index = step_index + 1

            # Move to next step
            if step_index + 1 < len(pipeline.steps):
                status.current_step = pipeline.steps[step_index + 1].step_name
            else:
                status.current_step = None
                status.finished_at = datetime.now(timezone.utc)
                pipeline.completed = True
        else:
            status.failed_step = pipeline.steps[step_index].step_name if step_index < len(pipeline.steps) else None
            status.finished_at = datetime.now(timezone.utc)

        pipeline.mark_updated()
        self._save_pipelines()
        return status

    def get_pipeline_status(self, pipeline_id: str) -> Optional[PipelineStatus]:
        """Get the current status of a pipeline."""
        return self._pipeline_status.get(pipeline_id)

    def list_pipeline_status(self) -> list[PipelineStatus]:
        """Get status for all started pipelines."""
        return list(self._pipeline_status.values())

    def get_next_step_payload(
        self,
        pipeline_id: str,
        current_step_index: int,
        base_payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Compute the payload for the next step in a pipeline.

        Uses the result_config from the next step to merge the
        current step's result into the next step's base payload.

        Args:
            pipeline_id: Pipeline ID
            current_step_index: Index of the just-completed step
            base_payload: The next step's existing payload

        Returns:
            The merged payload for the next step
        """
        pipeline = self.get_pipeline(pipeline_id)
        if pipeline is None:
            return base_payload

        next_index = current_step_index + 1
        if next_index >= len(pipeline.steps):
            return base_payload

        next_step = pipeline.steps[next_index]
        if next_step.result_config is None:
            return base_payload

        status = self._pipeline_status.get(pipeline_id)
        if status is None or not status.results:
            return base_payload

        # Use the last recorded result
        last_result = status.results[-1]
        return next_step.result_config.apply(last_result, base_payload)
