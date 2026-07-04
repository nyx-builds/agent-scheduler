"""Conditional execution rules for jobs.

Allows defining rules that determine whether a job should actually
execute when it becomes due. Rules can evaluate:

1. **Payload conditions** — check job payload fields (e.g. only run
   if ``payload["priority"] == "urgent"``)
2. **Previous result conditions** — check the result of the last
   execution or a dependency's result
3. **Time-based conditions** — only run during certain hours/days
   (complements TimeWindow but allows more complex logic)
4. **Composite conditions** — combine multiple rules with AND / OR

Example::

    # Only run if payload contains {"env": "production"}
    rule = ConditionRule(
        field_path="payload.env",
        operator="eq",
        value="production",
    )

    # Only run if last execution succeeded
    rule = ConditionRule(
        field_path="last_result.success",
        operator="eq",
        value=True,
    )

    # Composite: run if it's a weekend OR the payload says urgent
    rule = OrCondition([
        ConditionRule(field_path="payload.urgent", operator="eq", value=True),
        AndCondition([
            ConditionRule(field_path="time.weekday", operator="in", value=[5, 6]),
        ]),
    ])
"""

from __future__ import annotations

import operator
from datetime import datetime, timezone
from typing import Any, Optional, Union

from pydantic import BaseModel, Field

__all__ = [
    "ConditionOperator",
    "ConditionRule",
    "AndCondition",
    "OrCondition",
    "NotCondition",
    "ConditionContext",
    "evaluate_condition",
    "ConditionEvaluationError",
]


class ConditionEvaluationError(Exception):
    """Raised when a condition cannot be evaluated."""


# ── Operators ───────────────────────────────────────────────


def _match_regex(a: Any, b: Any) -> bool:
    import re
    if not isinstance(a, str) or not isinstance(b, str):
        return False
    try:
        return re.search(b, a) is not None
    except re.error:
        return False


_OPS = {
    "eq": operator.eq,
    "ne": operator.ne,
    "gt": operator.gt,
    "gte": operator.ge,
    "lt": operator.lt,
    "lte": operator.le,
    "in": lambda a, b: a in b,
    "not_in": lambda a, b: a not in b,
    "contains": lambda a, b: b in a if a is not None else False,
    "not_contains": lambda a, b: b not in a if a is not None else True,
    "starts_with": lambda a, b: isinstance(a, str) and a.startswith(b),
    "ends_with": lambda a, b: isinstance(a, str) and a.endswith(b),
    "matches_regex": _match_regex,
    "is_none": lambda a, b: a is None,
    "is_not_none": lambda a, b: a is not None,
    "is_true": lambda a, b: bool(a),
    "is_false": lambda a, b: not bool(a),
}


class ConditionOperator:
    """String constants for condition operators."""

    EQ = "eq"
    NE = "ne"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
    IN = "in"
    NOT_IN = "not_in"
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    MATCHES_REGEX = "matches_regex"
    IS_NONE = "is_none"
    IS_NOT_NONE = "is_not_none"
    IS_TRUE = "is_true"
    IS_FALSE = "is_false"

    ALL = list(_OPS.keys())


# ── Condition context ───────────────────────────────────────

class ConditionContext(BaseModel):
    """Context data available to condition evaluators.

    Fields are accessed via dot-notation ``field_path`` in rules.
    """

    payload: dict[str, Any] = Field(default_factory=dict)
    last_result: Optional[dict[str, Any]] = None
    last_status: Optional[str] = None
    job_tags: list[str] = Field(default_factory=list)
    job_metadata: dict[str, Any] = Field(default_factory=dict)
    run_count: int = 0
    fail_count: int = 0
    now: Optional[datetime] = None

    def resolve_path(self, path: str) -> Any:
        """Resolve a dot-notation path to a value.

        Supported root prefixes:
        - ``payload.<key>``      — job payload
        - ``last_result.<key>``  — last execution result dict
        - ``last_status``        — last execution status string
        - ``job_tags``           — list of tags
        - ``job_metadata.<key>`` — job metadata
        - ``run_count``          — successful run count
        - ``fail_count``         — failure count
        - ``time.hour``          — current hour (0-23)
        - ``time.weekday``       — current weekday (0=Mon .. 6=Sun)
        - ``time.day``           — current day of month
        - ``time.month``         — current month (1-12)
        - ``time.year``          — current year
        """
        parts = path.split(".")
        root = parts[0]

        if root == "payload":
            return _nested_get(self.payload, parts[1:])
        if root == "last_result":
            if self.last_result is None:
                return None
            return _nested_get(self.last_result, parts[1:])
        if root == "last_status":
            return self.last_status
        if root == "job_tags":
            return self.job_tags
        if root == "job_metadata":
            return _nested_get(self.job_metadata, parts[1:])
        if root == "run_count":
            return self.run_count
        if root == "fail_count":
            return self.fail_count
        if root == "time":
            now = self.now or datetime.now(timezone.utc)
            if len(parts) < 2:
                return now.isoformat()
            attr = parts[1]
            if attr == "hour":
                return now.hour
            if attr == "weekday":
                return now.weekday()
            if attr == "day":
                return now.day
            if attr == "month":
                return now.month
            if attr == "year":
                return now.year
            if attr == "minute":
                return now.minute
            if attr == "second":
                return now.second
            if attr == "iso":
                return now.isoformat()
            return None

        return None


def _nested_get(data: dict[str, Any], keys: list[str]) -> Any:
    """Get a value from nested dicts using a list of keys."""
    current: Any = data
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key)
        else:
            return None
        if current is None:
            return None
    return current


# ── Condition types ─────────────────────────────────────────


class ConditionBase(BaseModel):
    """Base class for conditions."""

    def evaluate(self, context: ConditionContext) -> bool:  # pragma: no cover
        raise NotImplementedError

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class ConditionRule(ConditionBase):
    """A single field comparison rule.

    Evaluates to True if ``resolve_path(field_path) <operator> value``.
    """

    field_path: str = Field(..., description="Dot-notation path (e.g. 'payload.env')")
    operator: str = Field(
        default="eq",
        description="Comparison operator",
    )
    value: Any = Field(
        default=None,
        description="Value to compare against (ignored for is_none/is_not_none/is_true/is_false)",
    )
    description: str = Field(default="", description="Human-readable description")

    def evaluate(self, context: ConditionContext) -> bool:
        if self.operator not in _OPS:
            raise ConditionEvaluationError(f"Unknown operator: {self.operator}")

        actual = context.resolve_path(self.field_path)
        op_func = _OPS[self.operator]

        try:
            return bool(op_func(actual, self.value))
        except TypeError:
            return False


class AndCondition(ConditionBase):
    """All sub-conditions must be True."""

    conditions: list["ConditionType"] = Field(
        default_factory=list,
        description="Sub-conditions (all must pass)",
    )

    def evaluate(self, context: ConditionContext) -> bool:
        return all(c.evaluate(context) for c in self.conditions)


class OrCondition(ConditionBase):
    """At least one sub-condition must be True."""

    conditions: list["ConditionType"] = Field(
        default_factory=list,
        description="Sub-conditions (any must pass)",
    )

    def evaluate(self, context: ConditionContext) -> bool:
        return any(c.evaluate(context) for c in self.conditions)


class NotCondition(ConditionBase):
    """Inverts a sub-condition."""

    condition: "ConditionType" = Field(..., description="Condition to negate")

    def evaluate(self, context: ConditionContext) -> bool:
        return not self.condition.evaluate(context)


# Forward references
ConditionType = Union[ConditionRule, AndCondition, OrCondition, NotCondition]

# Update forward refs for recursive models
AndCondition.model_rebuild()
OrCondition.model_rebuild()
NotCondition.model_rebuild()


# ── Public evaluator ────────────────────────────────────────


def evaluate_condition(
    condition: Union[ConditionType, dict[str, Any]],
    context: ConditionContext,
) -> bool:
    """Evaluate a condition (or condition dict) against a context.

    Args:
        condition: A Condition object or a dict that will be parsed.
            Dicts must include a ``type`` key: ``"rule"``, ``"and"``,
            ``"or"``, or ``"not"``.

        context: The ConditionContext to evaluate against.

    Returns:
        True if the condition is satisfied.

    Raises:
        ConditionEvaluationError: If the condition is invalid.
    """
    if isinstance(condition, dict):
        condition = _condition_from_dict(condition)

    return condition.evaluate(context)


def _condition_from_dict(data: dict[str, Any]) -> ConditionType:
    """Parse a condition from a dict with a ``type`` discriminator."""
    cond_type = data.get("type", "rule")

    if cond_type == "rule":
        return ConditionRule(
            field_path=data["field_path"],
            operator=data.get("operator", "eq"),
            value=data.get("value"),
            description=data.get("description", ""),
        )
    if cond_type == "and":
        subs = [_condition_from_dict(c) for c in data.get("conditions", [])]
        return AndCondition(conditions=subs)
    if cond_type == "or":
        subs = [_condition_from_dict(c) for c in data.get("conditions", [])]
        return OrCondition(conditions=subs)
    if cond_type == "not":
        return NotCondition(condition=_condition_from_dict(data["condition"]))

    raise ConditionEvaluationError(f"Unknown condition type: {cond_type}")
