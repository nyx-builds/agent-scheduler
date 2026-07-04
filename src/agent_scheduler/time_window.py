"""Time window constraints for job execution.

Allows restricting when a job is allowed to run — e.g., only during
business hours, only on weekdays, only in a specific timezone.

When a job is due but outside its time window, it is rescheduled to
the next moment the window opens rather than being executed immediately.
"""

from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, Field, field_validator


__all__ = [
    "TimeWindow",
    "is_within_window",
    "next_window_start",
]


class TimeWindow(BaseModel):
    """Defines when a job is allowed to execute.

    All time references are interpreted in ``timezone``.

    Example::

        # Only run on weekdays between 09:00 and 17:00 US/Eastern
        window = TimeWindow(
            start_time="09:00",
            end_time="17:00",
            days_of_week=[0, 1, 2, 3, 4],  # Mon-Fri
            timezone="US/Eastern",
        )
    """

    start_time: str = Field(
        default="00:00",
        description="Start of allowed window in HH:MM format (24h)",
    )
    end_time: str = Field(
        default="23:59",
        description="End of allowed window in HH:MM format (24h)",
    )
    days_of_week: list[int] = Field(
        default_factory=lambda: list(range(7)),
        description="Allowed days: 0=Mon .. 6=Sun. Empty = all days.",
    )
    timezone: str = Field(
        default="UTC",
        description="IANA timezone name (e.g. 'US/Eastern', 'Europe/London')",
    )

    @field_validator("start_time", "end_time")
    @classmethod
    def validate_time_format(cls, v: str) -> str:
        try:
            parts = v.strip().split(":")
            h, m = int(parts[0]), int(parts[1])
            if not (0 <= h <= 23 and 0 <= m <= 59):
                raise ValueError
        except (ValueError, IndexError):
            raise ValueError(f"Invalid time format '{v}'. Expected HH:MM (24-hour).")
        return v.strip()

    @field_validator("days_of_week")
    @classmethod
    def validate_days(cls, v: list[int]) -> list[int]:
        for d in v:
            if not (0 <= d <= 6):
                raise ValueError(f"Invalid day {d}. Use 0=Mon .. 6=Sun.")
        return sorted(set(v))

    def _get_tz(self) -> ZoneInfo:
        try:
            return ZoneInfo(self.timezone)
        except ZoneInfoNotFoundError:
            # Fall back to UTC if timezone is invalid
            return ZoneInfo("UTC")

    def _parse_time(self, t: str) -> time:
        parts = t.split(":")
        return time(hour=int(parts[0]), minute=int(parts[1]))

    @property
    def is_always_open(self) -> bool:
        """True if the window imposes no effective restriction."""
        return (
            self.start_time == "00:00"
            and self.end_time == "23:59"
            and len(self.days_of_week) == 7
        )

    def is_within_window(self, dt: Optional[datetime] = None) -> bool:
        """Check whether *dt* (default: now) falls within this window."""
        if self.is_always_open:
            return True

        if dt is None:
            dt = datetime.now(timezone.utc)

        # Convert to the window's timezone
        tz = self._get_tz()
        local_dt = dt.astimezone(tz)

        # Check day of week (Python weekday(): 0=Mon .. 6=Sun)
        if local_dt.weekday() not in self.days_of_week:
            return False

        # Check time range
        start = self._parse_time(self.start_time)
        end = self._parse_time(self.end_time)

        current_time = local_dt.time()

        if start <= end:
            # Normal range, e.g. 09:00 - 17:00
            return start <= current_time <= end
        else:
            # Overnight range, e.g. 22:00 - 06:00
            return current_time >= start or current_time <= end

    def next_window_start(self, dt: Optional[datetime] = None) -> datetime:
        """Compute the next datetime (in UTC) when the window opens.

        If *dt* is already inside the window, returns *dt* unchanged.
        """
        if self.is_always_open:
            return dt or datetime.now(timezone.utc)

        if dt is None:
            dt = datetime.now(timezone.utc)

        if self.is_within_window(dt):
            return dt

        tz = self._get_tz()
        local_dt = dt.astimezone(tz)
        start = self._parse_time(self.start_time)

        # Search day by day (up to 8 days to cover all weekday permutations)
        for day_offset in range(8):
            candidate_date = (local_dt + timedelta(days=day_offset)).date()
            candidate_local = datetime.combine(candidate_date, start, tzinfo=tz)

            if day_offset == 0:
                # Today: we might need to be after the current time
                # If start_time hasn't passed yet today
                if candidate_local > local_dt and candidate_local.weekday() in self.days_of_week:
                    return candidate_local.astimezone(timezone.utc)
            else:
                if candidate_local.weekday() in self.days_of_week:
                    return candidate_local.astimezone(timezone.utc)

        # Fallback (shouldn't reach here with valid config)
        return dt


# ── Standalone helpers ──────────────────────────────────────


def is_within_window(
    dt: datetime,
    start_time: str = "00:00",
    end_time: str = "23:59",
    days_of_week: Optional[list[int]] = None,
    timezone_name: str = "UTC",
) -> bool:
    """Convenience function to check if a datetime is within a window."""
    tw = TimeWindow(
        start_time=start_time,
        end_time=end_time,
        days_of_week=days_of_week or list(range(7)),
        timezone=timezone_name,
    )
    return tw.is_within_window(dt)


def next_window_start(
    dt: datetime,
    start_time: str = "00:00",
    end_time: str = "23:59",
    days_of_week: Optional[list[int]] = None,
    timezone_name: str = "UTC",
) -> datetime:
    """Convenience function to get the next window opening."""
    tw = TimeWindow(
        start_time=start_time,
        end_time=end_time,
        days_of_week=days_of_week or list(range(7)),
        timezone=timezone_name,
    )
    return tw.next_window_start(dt)
