"""Tests for Time Window constraints (v0.6.0)."""

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from agent_scheduler.time_window import (
    TimeWindow,
    is_within_window,
    next_window_start,
)


class TestTimeWindowDefaults:
    def test_always_open(self):
        tw = TimeWindow()
        assert tw.is_always_open is True
        assert tw.is_within_window() is True

    def test_default_days_all_week(self):
        tw = TimeWindow()
        assert tw.days_of_week == [0, 1, 2, 3, 4, 5, 6]


class TestTimeWindowValidation:
    def test_valid_time_format(self):
        tw = TimeWindow(start_time="09:30", end_time="17:00")
        assert tw.start_time == "09:30"

    def test_invalid_time_format(self):
        with pytest.raises(Exception):
            TimeWindow(start_time="25:00")

    def test_invalid_time_format_negative(self):
        with pytest.raises(Exception):
            TimeWindow(end_time="-1:30")

    def test_invalid_day(self):
        with pytest.raises(Exception):
            TimeWindow(days_of_week=[7])

    def test_days_deduplicated_and_sorted(self):
        tw = TimeWindow(days_of_week=[3, 1, 1, 5])
        assert tw.days_of_week == [1, 3, 5]

    def test_invalid_timezone_falls_back_to_utc(self):
        tw = TimeWindow(timezone="Invalid/Zone")
        # Should not raise, falls back to UTC
        assert tw.is_within_window(datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc))


class TestTimeWindowChecking:
    def test_within_normal_range(self):
        tw = TimeWindow(start_time="09:00", end_time="17:00")
        dt = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)  # Thursday noon
        assert tw.is_within_window(dt) is True

    def test_outside_normal_range(self):
        tw = TimeWindow(start_time="09:00", end_time="17:00")
        dt = datetime(2026, 1, 1, 18, 0, tzinfo=timezone.utc)
        assert tw.is_within_window(dt) is False

    def test_before_start(self):
        tw = TimeWindow(start_time="09:00", end_time="17:00")
        dt = datetime(2026, 1, 1, 8, 59, tzinfo=timezone.utc)
        assert tw.is_within_window(dt) is False

    def test_exactly_at_start(self):
        tw = TimeWindow(start_time="09:00", end_time="17:00")
        dt = datetime(2026, 1, 1, 9, 0, tzinfo=timezone.utc)
        assert tw.is_within_window(dt) is True

    def test_exactly_at_end(self):
        tw = TimeWindow(start_time="09:00", end_time="17:00")
        dt = datetime(2026, 1, 1, 17, 0, tzinfo=timezone.utc)
        assert tw.is_within_window(dt) is True

    def test_overnight_range(self):
        """22:00 - 06:00 should cover late night and early morning."""
        tw = TimeWindow(start_time="22:00", end_time="06:00")
        # 23:00 should be within
        assert tw.is_within_window(datetime(2026, 1, 1, 23, 0, tzinfo=timezone.utc)) is True
        # 03:00 should be within
        assert tw.is_within_window(datetime(2026, 1, 1, 3, 0, tzinfo=timezone.utc)) is True
        # 12:00 should NOT be within
        assert tw.is_within_window(datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)) is False

    def test_day_restriction(self):
        # Only weekdays
        tw = TimeWindow(days_of_week=[0, 1, 2, 3, 4])
        # Monday (0) at noon
        monday = datetime(2026, 1, 5, 12, 0, tzinfo=timezone.utc)  # Jan 5 2026 is Monday
        assert tw.is_within_window(monday) is True
        # Saturday (5)
        saturday = datetime(2026, 1, 10, 12, 0, tzinfo=timezone.utc)
        assert tw.is_within_window(saturday) is False

    def test_timezone_conversion(self):
        """A 09:00-17:00 US/Eastern window should be 14:00-22:00 UTC in winter."""
        tw = TimeWindow(
            start_time="09:00",
            end_time="17:00",
            timezone="US/Eastern",
        )
        # 14:00 UTC = 09:00 EST (winter, EST = UTC-5)
        dt = datetime(2026, 1, 15, 14, 0, tzinfo=timezone.utc)  # Thursday
        assert tw.is_within_window(dt) is True
        # 13:00 UTC = 08:00 EST → outside
        dt_early = datetime(2026, 1, 15, 13, 0, tzinfo=timezone.utc)
        assert tw.is_within_window(dt_early) is False


class TestNextWindowStart:
    def test_already_in_window(self):
        tw = TimeWindow(start_time="09:00", end_time="17:00")
        dt = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
        result = tw.next_window_start(dt)
        assert result == dt  # No change

    def test_before_window_today(self):
        tw = TimeWindow(start_time="09:00", end_time="17:00")
        dt = datetime(2026, 1, 1, 6, 0, tzinfo=timezone.utc)  # Thursday 6am
        result = tw.next_window_start(dt)
        # Should be today at 09:00 UTC
        assert result.hour == 9
        assert result.day == 1

    def test_after_window_today_skips_to_tomorrow(self):
        tw = TimeWindow(start_time="09:00", end_time="17:00")
        dt = datetime(2026, 1, 1, 18, 0, tzinfo=timezone.utc)  # Thursday 6pm
        result = tw.next_window_start(dt)
        # Tomorrow at 09:00
        assert result.hour == 9
        assert result.day == 2

    def test_weekend_skip(self):
        """If window is weekdays only, Friday evening should skip to Monday."""
        tw = TimeWindow(
            start_time="09:00",
            end_time="17:00",
            days_of_week=[0, 1, 2, 3, 4],  # Mon-Fri
        )
        # Friday Jan 2 2026 at 18:00
        friday_evening = datetime(2026, 1, 2, 18, 0, tzinfo=timezone.utc)
        result = tw.next_window_start(friday_evening)
        # Should be Monday Jan 5 at 09:00
        assert result.weekday() == 0  # Monday
        assert result.day == 5
        assert result.hour == 9

    def test_always_open_returns_input(self):
        tw = TimeWindow()
        dt = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
        result = tw.next_window_start(dt)
        assert result == dt


class TestStandaloneFunctions:
    def test_is_within_window_function(self):
        dt = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
        assert is_within_window(dt, "09:00", "17:00") is True
        assert is_within_window(dt, "13:00", "17:00") is False

    def test_next_window_start_function(self):
        dt = datetime(2026, 1, 1, 6, 0, tzinfo=timezone.utc)
        result = next_window_start(dt, "09:00", "17:00")
        assert result.hour == 9


class TestJobTimeWindowIntegration:
    def test_job_with_time_window_delays_next_run(self):
        """A job with a time window should have next_run_at pushed to window start."""
        from agent_scheduler.models import Job

        # Job with a business-hours window
        job = Job(
            name="biz-hours-job",
            handler="test",
            delay=0,  # Immediate
            time_window={
                "start_time": "09:00",
                "end_time": "17:00",
                "timezone": "UTC",
            },
        )

        # Compute next run at 6am UTC (outside window)
        early_dt = datetime(2026, 1, 1, 6, 0, tzinfo=timezone.utc)
        next_run = job.compute_next_run(early_dt)

        # Should be pushed to 09:00
        assert next_run is not None
        assert next_run.hour == 9

    def test_job_without_time_window_unaffected(self):
        from agent_scheduler.models import Job

        job = Job(name="test", handler="h", delay=0)
        now = datetime(2026, 1, 1, 3, 0, tzinfo=timezone.utc)
        next_run = job.compute_next_run(now)
        # No window — should be immediate (now)
        assert next_run is not None
