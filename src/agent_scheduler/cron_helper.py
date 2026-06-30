"""Cron expression utilities for agent-scheduler.

Provides:
- **Validation** — Validate cron expressions with detailed error messages
- **Human-readable descriptions** — Translate cron to English ("Every Monday at 9:00 AM")
- **Preview** — Show the next N run times
- **Builder** — Construct cron expressions from natural parameters
- **Parser** — Extract field meanings from a cron expression
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field


# ── Constants ────────────────────────────────────────────────────

_DAYS_OF_WEEK = {
    0: "Monday", 1: "Tuesday", 2: "Wednesday", 3: "Thursday",
    4: "Friday", 5: "Saturday", 6: "Sunday",
}
_MONTHS = {
    1: "January", 2: "February", 3: "March", 4: "April",
    5: "May", 6: "June", 7: "July", 8: "August",
    9: "September", 10: "October", 11: "November", 12: "December",
}
# croniter uses 0-6 where 0=Monday
_DAY_NAME_TO_NUM = {
    "mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4,
    "sat": 5, "sun": 6,
    "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
    "friday": 4, "saturday": 5, "sunday": 6,
}


class CronValidation(BaseModel):
    """Result of cron expression validation."""

    is_valid: bool = Field(..., description="Whether the expression is valid")
    error: Optional[str] = Field(default=None, description="Error message if invalid")
    expression: str = Field(..., description="The validated expression")


class CronInfo(BaseModel):
    """Parsed information about a cron expression."""

    expression: str
    minute: str
    hour: str
    day_of_month: str
    month: str
    day_of_week: str
    is_every_minute: bool = False
    is_every_hour: bool = False
    is_every_day: bool = False
    is_every_week: bool = False
    is_every_month: bool = False


class CronBuilder:
    """Build cron expressions from natural parameters.

    Examples::

        CronBuilder().daily_at(hour=9, minute=30).build()
        # => "30 9 * * *"

        CronBuilder().weekly_on("monday", hour=9).build()
        # => "0 9 * * 0"

        CronBuilder().every_n_hours(6).build()
        # => "0 */6 * * *"

        CronBuilder().every_n_minutes(15).build()
        # => "*/15 * * * *"
    """

    def __init__(self) -> None:
        self._minute: str = "*"
        self._hour: str = "*"
        self._dom: str = "*"
        self._month: str = "*"
        self._dow: str = "*"

    def every_minute(self) -> "CronBuilder":
        """Run every minute."""
        self._minute = "*"
        self._hour = "*"
        return self

    def every_n_minutes(self, n: int) -> "CronBuilder":
        """Run every N minutes."""
        if n < 1 or n > 59:
            raise ValueError("Minutes interval must be between 1 and 59")
        self._minute = f"*/{n}"
        self._hour = "*"
        return self

    def every_hour_at(self, minute: int = 0) -> "CronBuilder":
        """Run every hour at the specified minute."""
        if minute < 0 or minute > 59:
            raise ValueError("Minute must be between 0 and 59")
        self._minute = str(minute)
        self._hour = "*"
        return self

    def every_n_hours(self, n: int, minute: int = 0) -> "CronBuilder":
        """Run every N hours at the specified minute."""
        if n < 1 or n > 23:
            raise ValueError("Hour interval must be between 1 and 23")
        if minute < 0 or minute > 59:
            raise ValueError("Minute must be between 0 and 59")
        self._minute = str(minute)
        self._hour = f"*/{n}"
        return self

    def daily_at(self, hour: int, minute: int = 0) -> "CronBuilder":
        """Run every day at a specific time."""
        if hour < 0 or hour > 23:
            raise ValueError("Hour must be between 0 and 23")
        if minute < 0 or minute > 59:
            raise ValueError("Minute must be between 0 and 59")
        self._minute = str(minute)
        self._hour = str(hour)
        return self

    def weekly_on(self, day: str, hour: int = 0, minute: int = 0) -> "CronBuilder":
        """Run on a specific day of the week."""
        day_num = _DAY_NAME_TO_NUM.get(day.lower().strip())
        if day_num is None:
            raise ValueError(f"Invalid day: {day}. Use: {sorted(_DAY_NAME_TO_NUM.keys())}")
        if hour < 0 or hour > 23:
            raise ValueError("Hour must be between 0 and 23")
        if minute < 0 or minute > 59:
            raise ValueError("Minute must be between 0 and 59")
        self._minute = str(minute)
        self._hour = str(hour)
        self._dow = str(day_num)
        return self

    def monthly_on(self, day: int, hour: int = 0, minute: int = 0) -> "CronBuilder":
        """Run on a specific day of every month."""
        if day < 1 or day > 31:
            raise ValueError("Day of month must be between 1 and 31")
        if hour < 0 or hour > 23:
            raise ValueError("Hour must be between 0 and 23")
        if minute < 0 or minute > 59:
            raise ValueError("Minute must be between 0 and 59")
        self._minute = str(minute)
        self._hour = str(hour)
        self._dom = str(day)
        return self

    def weekdays(self, hour: int = 9, minute: int = 0) -> "CronBuilder":
        """Run Monday through Friday."""
        if hour < 0 or hour > 23:
            raise ValueError("Hour must be between 0 and 23")
        if minute < 0 or minute > 59:
            raise ValueError("Minute must be between 0 and 59")
        self._minute = str(minute)
        self._hour = str(hour)
        self._dow = "0-4"  # Mon-Fri in croniter (0=Monday)
        return self

    def weekends(self, hour: int = 9, minute: int = 0) -> "CronBuilder":
        """Run Saturday and Sunday."""
        if hour < 0 or hour > 23:
            raise ValueError("Hour must be between 0 and 23")
        if minute < 0 or minute > 59:
            raise ValueError("Minute must be between 0 and 59")
        self._minute = str(minute)
        self._hour = str(hour)
        self._dow = "5,6"  # Sat-Sun
        return self

    def build(self) -> str:
        """Build the final cron expression."""
        return f"{self._minute} {self._hour} {self._dom} {self._month} {self._dow}"

    def build_and_validate(self) -> str:
        """Build and validate the expression, raising on invalid."""
        expr = self.build()
        result = validate_cron(expr)
        if not result.is_valid:
            raise ValueError(f"Built invalid cron expression '{expr}': {result.error}")
        return expr


def validate_cron(expression: str) -> CronValidation:
    """Validate a cron expression.

    Returns a CronValidation with ``is_valid=True`` if the expression
    can be parsed by croniter, or an error message otherwise.
    """
    expr = expression.strip()
    if not expr:
        return CronValidation(is_valid=False, error="Empty expression", expression=expression)

    try:
        from croniter import croniter

        croniter(expr)
        return CronValidation(is_valid=True, expression=expr)
    except (ValueError, KeyError) as e:
        return CronValidation(is_valid=False, error=str(e), expression=expression)
    except Exception as e:
        return CronValidation(is_valid=False, error=f"Unexpected error: {e}", expression=expression)


def parse_cron(expression: str) -> CronInfo:
    """Parse a cron expression into its component fields.

    Returns CronInfo with individual field values and convenience flags.
    """
    parts = expression.strip().split()
    if len(parts) != 5:
        raise ValueError(f"Invalid cron expression: expected 5 fields, got {len(parts)}: '{expression}'")

    minute, hour, dom, month, dow = parts

    return CronInfo(
        expression=expression.strip(),
        minute=minute,
        hour=hour,
        day_of_month=dom,
        month=month,
        day_of_week=dow,
        is_every_minute=minute == "*",
        is_every_hour=hour == "*" and minute != "*",
        is_every_day=dom == "*",
        is_every_week=dow != "*",
        is_every_month=month == "*",
    )


def describe_cron(expression: str) -> str:
    """Convert a cron expression to a human-readable description.

    Examples::

        describe_cron("* * * * *")
        # => "Every minute"

        describe_cron("0 9 * * MON-FRI")
        # => "At 09:00 AM, Monday through Friday"

        describe_cron("*/15 * * * *")
        # => "Every 15 minutes"

        describe_cron("30 14 1 * *")
        # => "At 02:30 PM on day 1 of the month"
    """
    validation = validate_cron(expression)
    if not validation.is_valid:
        return f"Invalid expression: {validation.error}"

    try:
        info = parse_cron(expression)
    except ValueError as e:
        return str(e)

    parts: list[str] = []

    # ── Time component ────────────────────────────────────

    if info.is_every_minute and info.hour == "*":
        return "Every minute"

    # Handle minute patterns
    minute_desc = _describe_minute(info.minute, info.hour)
    hour_desc = _describe_hour(info.hour)

    if minute_desc and hour_desc:
        parts.append(f"{hour_desc}, {minute_desc.lower()}")
    elif minute_desc:
        parts.append(minute_desc)
    elif hour_desc:
        parts.append(hour_desc)

    # ── Day of month ──────────────────────────────────────

    if info.day_of_month != "*":
        dom_desc = _describe_day_of_month(info.day_of_month)
        if dom_desc:
            parts.append(dom_desc)

    # ── Month ─────────────────────────────────────────────

    if info.month != "*":
        month_desc = _describe_month(info.month)
        if month_desc:
            parts.append(month_desc)

    # ── Day of week ───────────────────────────────────────

    if info.day_of_week != "*":
        dow_desc = _describe_day_of_week(info.day_of_week)
        if dow_desc:
            parts.append(dow_desc)

    # If nothing matched, return the raw expression
    result = ", ".join(parts)
    if not result:
        return f"Cron: {expression}"
    return result


def _describe_minute(minute: str, hour: str) -> str:
    """Describe the minute field."""
    if minute == "*":
        if hour == "*":
            return ""
        return "every minute"

    # */N pattern
    if minute.startswith("*/"):
        n = minute[2:]
        return f"every {n} minutes"

    # Range like 0-30
    if "-" in minute and "," not in minute and "/" not in minute:
        start, end = minute.split("-", 1)
        return f"minutes {start} through {end}"

    # List
    if "," in minute:
        nums = [m.strip() for m in minute.split(",")]
        return f"at minutes {', '.join(nums)}"

    # Specific minute
    if hour != "*":
        # We have a specific hour and minute — format as time
        try:
            hour_int = int(hour)
            minute_int = int(minute)
            return _format_time(hour_int, minute_int)
        except ValueError:
            pass

    try:
        return f"at minute {int(minute)}"
    except ValueError:
        return f"at minute {minute}"


def _describe_hour(hour: str) -> str:
    """Describe the hour field."""
    if hour == "*":
        return ""

    if hour.startswith("*/"):
        n = hour[2:]
        return f"every {n} hours"

    if "-" in hour and "," not in hour and "/" not in hour:
        start, end = hour.split("-", 1)
        try:
            start_h = int(start)
            end_h = int(end)
            return f"between {_format_hour(start_h)} and {_format_hour(end_h)}"
        except ValueError:
            pass

    if "," in hour:
        nums = [h.strip() for h in hour.split(",")]
        hours_fmt = ", ".join(_format_hour(int(h)) if h.isdigit() else h for h in nums)
        return f"at {hours_fmt}"

    try:
        return f"at {_format_hour(int(hour))}"
    except ValueError:
        return ""


def _describe_day_of_month(dom: str) -> str:
    """Describe the day-of-month field."""
    if dom == "*":
        return ""

    if dom.startswith("*/"):
        n = dom[2:]
        return f"every {n} days"

    if "-" in dom and "," not in dom:
        start, end = dom.split("-", 1)
        return f"on days {start} through {end} of the month"

    if "," in dom:
        days = [d.strip() for d in dom.split(",")]
        # Add ordinal suffixes
        formatted = ", ".join(_ordinal(d) for d in days)
        return f"on the {formatted} of the month"

    try:
        return f"on day {int(dom)} of the month"
    except ValueError:
        return ""


def _describe_month(month: str) -> str:
    """Describe the month field."""
    if month == "*":
        return ""

    if month.startswith("*/"):
        n = month[2:]
        return f"every {n} months"

    if "-" in month and "," not in month:
        start, end = month.split("-", 1)
        try:
            start_m = int(start)
            end_m = int(end)
            return f"in {_MONTHS.get(start_m, start)} through {_MONTHS.get(end_m, end)}"
        except ValueError:
            pass

    if "," in month:
        months = [m.strip() for m in month.split(",")]
        names = []
        for m in months:
            try:
                names.append(_MONTHS.get(int(m), m))
            except ValueError:
                names.append(m)
        return f"in {' '.join(names)}"

    try:
        m = int(month)
        return f"in {_MONTHS.get(m, month)}"
    except ValueError:
        return ""


def _describe_day_of_week(dow: str) -> str:
    """Describe the day-of-week field."""
    if dow == "*":
        return ""

    # Handle named days like MON, TUE, MON-FRI
    upper = dow.upper()

    # Range with names or numbers
    if "-" in upper and "," not in upper:
        start, end = upper.split("-", 1)
        start_day = _DAY_NAME_TO_NUM.get(start.lower(), None)
        end_day = _DAY_NAME_TO_NUM.get(end.lower(), None)
        if start_day is not None and end_day is not None:
            day_range = _DAYS_OF_WEEK[start_day]
            end_day_name = _DAYS_OF_WEEK[end_day]
            return f"on {day_range} through {end_day_name}"
        # Numeric range
        try:
            start_n = int(start)
            end_n = int(end)
            days = [_DAYS_OF_WEEK.get(i, str(i)) for i in range(start_n, end_n + 1)]
            return f"on {' through '.join(days[:1])}" if len(days) == 1 else f"on {days[0]} through {days[-1]}"
        except ValueError:
            pass

    # List
    if "," in upper:
        parts = [p.strip() for p in upper.split(",")]
        days = []
        for p in parts:
            day_num = _DAY_NAME_TO_NUM.get(p.lower())
            if day_num is not None:
                days.append(_DAYS_OF_WEEK[day_num])
            else:
                try:
                    days.append(_DAYS_OF_WEEK.get(int(p), p))
                except ValueError:
                    days.append(p)
        return f"on {', '.join(days)}"

    # Single named day
    day_num = _DAY_NAME_TO_NUM.get(upper.lower())
    if day_num is not None:
        return f"on {_DAYS_OF_WEEK[day_num]}"

    # Single numeric day
    try:
        n = int(dow)
        return f"on {_DAYS_OF_WEEK.get(n, dow)}"
    except ValueError:
        return ""


def _format_hour(hour: int) -> str:
    """Format an hour as 12-hour time."""
    if hour == 0:
        return "12 AM"
    if hour < 12:
        return f"{hour} AM"
    if hour == 12:
        return "12 PM"
    return f"{hour - 12} PM"


def _format_time(hour: int, minute: int) -> str:
    """Format hour:minute as a readable time string."""
    period = "AM" if hour < 12 else "PM"
    display_hour = hour if hour <= 12 else hour - 12
    if display_hour == 0:
        display_hour = 12
    if minute == 0:
        return f"at {display_hour:02d}:00 {period}"
    return f"at {display_hour:02d}:{minute:02d} {period}"


def _ordinal(n_str: str) -> str:
    """Add ordinal suffix to a number string."""
    try:
        n = int(n_str)
    except ValueError:
        return n_str
    if 11 <= n % 100 <= 13:
        suffix = "th"
    else:
        suffixes = {1: "st", 2: "nd", 3: "rd"}
        suffix = suffixes.get(n % 10, "th")
    return f"{n}{suffix}"


def preview_runs(expression: str, n: int = 5, start: Optional[datetime] = None) -> list[datetime]:
    """Preview the next N run times for a cron expression.

    Args:
        expression: Valid cron expression
        n: Number of future run times to compute (default 5)
        start: Starting datetime (defaults to now in UTC)

    Returns:
        List of datetime objects representing future run times

    Raises:
        ValueError: If the expression is invalid
    """
    validation = validate_cron(expression)
    if not validation.is_valid:
        raise ValueError(f"Invalid cron expression: {validation.error}")

    if start is None:
        start = datetime.now(timezone.utc)

    from croniter import croniter

    cron = croniter(expression, start)
    return [cron.get_next(datetime) for _ in range(n)]


def suggest_cron(frequency: str, **kwargs) -> str:
    """Suggest a cron expression from a frequency description.

    Args:
        frequency: One of: "every-minute", "every-n-minutes", "hourly",
                   "every-n-hours", "daily", "weekly", "weekdays",
                   "weekends", "monthly"
        **kwargs: Additional parameters (hour, minute, day, n, interval)

    Returns:
        A cron expression string

    Raises:
        ValueError: If frequency is unknown or parameters are invalid

    Examples::

        suggest_cron("daily", hour=9, minute=30)
        # => "30 9 * * *"

        suggest_cron("every-n-minutes", n=15)
        # => "*/15 * * * *"

        suggest_cron("weekly", day="monday", hour=9)
        # => "0 9 * * 0"
    """
    builder = CronBuilder()

    if frequency == "every-minute":
        builder.every_minute()
    elif frequency == "every-n-minutes":
        n = kwargs.get("n", kwargs.get("interval", 5))
        builder.every_n_minutes(n)
    elif frequency == "hourly":
        minute = kwargs.get("minute", 0)
        builder.every_hour_at(minute)
    elif frequency == "every-n-hours":
        n = kwargs.get("n", kwargs.get("interval", 6))
        minute = kwargs.get("minute", 0)
        builder.every_n_hours(n, minute)
    elif frequency == "daily":
        hour = kwargs.get("hour", 0)
        minute = kwargs.get("minute", 0)
        builder.daily_at(hour, minute)
    elif frequency == "weekly":
        day = kwargs.get("day", "monday")
        hour = kwargs.get("hour", 0)
        minute = kwargs.get("minute", 0)
        builder.weekly_on(day, hour, minute)
    elif frequency == "weekdays":
        hour = kwargs.get("hour", 9)
        minute = kwargs.get("minute", 0)
        builder.weekdays(hour, minute)
    elif frequency == "weekends":
        hour = kwargs.get("hour", 9)
        minute = kwargs.get("minute", 0)
        builder.weekends(hour, minute)
    elif frequency == "monthly":
        day = kwargs.get("day", kwargs.get("day_of_month", 1))
        hour = kwargs.get("hour", 0)
        minute = kwargs.get("minute", 0)
        builder.monthly_on(day, hour, minute)
    else:
        raise ValueError(
            f"Unknown frequency: {frequency!r}. "
            f"Use: every-minute, every-n-minutes, hourly, every-n-hours, "
            f"daily, weekly, weekdays, weekends, monthly"
        )

    return builder.build_and_validate()
