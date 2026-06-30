"""Tests for the cron_helper module."""

import pytest
from datetime import datetime, timezone

from agent_scheduler.cron_helper import (
    CronBuilder,
    CronInfo,
    CronValidation,
    validate_cron,
    parse_cron,
    describe_cron,
    preview_runs,
    suggest_cron,
    _format_hour,
    _format_time,
    _ordinal,
)


# ── validate_cron ──────────────────────────────────────────


class TestValidateCron:
    def test_valid_simple(self):
        result = validate_cron("0 9 * * *")
        assert result.is_valid is True
        assert result.error is None

    def test_valid_every_minute(self):
        result = validate_cron("* * * * *")
        assert result.is_valid is True

    def test_valid_named_days(self):
        result = validate_cron("0 9 * * MON-FRI")
        assert result.is_valid is True

    def test_valid_step(self):
        result = validate_cron("*/15 * * * *")
        assert result.is_valid is True

    def test_invalid_empty(self):
        result = validate_cron("")
        assert result.is_valid is False
        assert "Empty" in result.error

    def test_invalid_format(self):
        result = validate_cron("not a cron")
        assert result.is_valid is False

    def test_invalid_too_many_fields(self):
        result = validate_cron("0 9 * * * MON")
        assert result.is_valid is False

    def test_strips_whitespace(self):
        result = validate_cron("  0 9 * * *  ")
        assert result.is_valid is True
        assert result.expression == "0 9 * * *"


# ── parse_cron ─────────────────────────────────────────────


class TestParseCron:
    def test_basic(self):
        info = parse_cron("30 9 * * *")
        assert info.minute == "30"
        assert info.hour == "9"
        assert info.day_of_month == "*"
        assert info.month == "*"
        assert info.day_of_week == "*"
        assert info.is_every_day is True
        assert info.is_every_month is True

    def test_every_minute(self):
        info = parse_cron("* * * * *")
        assert info.is_every_minute is True

    def test_every_hour(self):
        info = parse_cron("0 * * * *")
        assert info.is_every_hour is True
        assert info.is_every_minute is False

    def test_weekly(self):
        info = parse_cron("0 9 * * 1")
        assert info.is_every_week is True

    def test_invalid_raises(self):
        with pytest.raises(ValueError, match="5 fields"):
            parse_cron("0 9 * *")


# ── describe_cron ──────────────────────────────────────────


class TestDescribeCron:
    def test_every_minute(self):
        assert describe_cron("* * * * *") == "Every minute"

    def test_every_n_minutes(self):
        desc = describe_cron("*/15 * * * *")
        assert "15 minutes" in desc

    def test_daily_at(self):
        desc = describe_cron("0 9 * * *")
        assert "09" in desc
        assert "AM" in desc

    def test_specific_minute_hour(self):
        desc = describe_cron("30 14 * * *")
        assert "02:30" in desc.lower() or "2:30" in desc.lower()
        assert "PM" in desc.upper()

    def test_day_of_month(self):
        desc = describe_cron("0 0 1 * *")
        assert "day 1" in desc.lower() or "1" in desc

    def test_named_days(self):
        desc = describe_cron("0 9 * * MON-FRI")
        assert "Monday" in desc or "Mon" in desc

    def test_invalid_returns_message(self):
        desc = describe_cron("invalid")
        assert "Invalid" in desc

    def test_month(self):
        desc = describe_cron("0 0 1 6 *")
        assert "June" in desc or "6" in desc


# ── preview_runs ───────────────────────────────────────────


class TestPreviewRuns:
    def test_basic(self):
        start = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        runs = preview_runs("0 9 * * *", n=3, start=start)
        assert len(runs) == 3
        # All should be at 9 AM
        for r in runs:
            assert r.hour == 9
            assert r.minute == 0

    def test_every_minute(self):
        start = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        runs = preview_runs("* * * * *", n=5, start=start)
        assert len(runs) == 5
        # Each run should be 1 minute apart
        for i in range(1, len(runs)):
            diff = (runs[i] - runs[i - 1]).total_seconds()
            assert diff == 60

    def test_invalid_raises(self):
        with pytest.raises(ValueError, match="Invalid cron"):
            preview_runs("not valid", n=3)

    def test_default_start(self):
        runs = preview_runs("0 9 * * *", n=1)
        assert len(runs) == 1


# ── CronBuilder ────────────────────────────────────────────


class TestCronBuilder:
    def test_every_minute(self):
        expr = CronBuilder().every_minute().build()
        assert expr == "* * * * *"

    def test_every_n_minutes(self):
        expr = CronBuilder().every_n_minutes(15).build()
        assert expr == "*/15 * * * *"

    def test_every_n_minutes_invalid(self):
        with pytest.raises(ValueError):
            CronBuilder().every_n_minutes(0)
        with pytest.raises(ValueError):
            CronBuilder().every_n_minutes(60)

    def test_every_hour_at(self):
        expr = CronBuilder().every_hour_at(30).build()
        assert expr == "30 * * * *"

    def test_every_n_hours(self):
        expr = CronBuilder().every_n_hours(6, minute=0).build()
        assert expr == "0 */6 * * *"

    def test_daily_at(self):
        expr = CronBuilder().daily_at(9, 30).build()
        assert expr == "30 9 * * *"

    def test_daily_at_invalid_hour(self):
        with pytest.raises(ValueError):
            CronBuilder().daily_at(24, 0)

    def test_weekly_on(self):
        expr = CronBuilder().weekly_on("monday", hour=9).build()
        assert expr == "0 9 * * 0"

    def test_weekly_on_invalid_day(self):
        with pytest.raises(ValueError):
            CronBuilder().weekly_on("funday")

    def test_monthly_on(self):
        expr = CronBuilder().monthly_on(15, hour=9, minute=30).build()
        assert expr == "30 9 15 * *"

    def test_weekdays(self):
        expr = CronBuilder().weekdays(hour=9, minute=0).build()
        assert expr == "0 9 * * 0-4"

    def test_weekends(self):
        expr = CronBuilder().weekends(hour=10).build()
        assert expr == "0 10 * * 5,6"

    def test_chaining(self):
        expr = CronBuilder().daily_at(9).build()
        assert expr == "0 9 * * *"

    def test_build_and_validate(self):
        expr = CronBuilder().daily_at(9, 30).build_and_validate()
        assert expr == "30 9 * * *"


# ── suggest_cron ───────────────────────────────────────────


class TestSuggestCron:
    def test_every_minute(self):
        assert suggest_cron("every-minute") == "* * * * *"

    def test_every_n_minutes(self):
        assert suggest_cron("every-n-minutes", n=15) == "*/15 * * * *"

    def test_hourly(self):
        assert suggest_cron("hourly", minute=30) == "30 * * * *"

    def test_every_n_hours(self):
        assert suggest_cron("every-n-hours", n=6) == "0 */6 * * *"

    def test_daily(self):
        assert suggest_cron("daily", hour=9, minute=30) == "30 9 * * *"

    def test_weekly(self):
        assert suggest_cron("weekly", day="monday", hour=9) == "0 9 * * 0"

    def test_weekdays(self):
        assert suggest_cron("weekdays", hour=9) == "0 9 * * 0-4"

    def test_weekends(self):
        assert suggest_cron("weekends", hour=10) == "0 10 * * 5,6"

    def test_monthly(self):
        assert suggest_cron("monthly", day=15, hour=9, minute=30) == "30 9 15 * *"

    def test_unknown_raises(self):
        with pytest.raises(ValueError, match="Unknown frequency"):
            suggest_cron("biweekly")

    def test_uses_interval_alias(self):
        assert suggest_cron("every-n-minutes", interval=10) == "*/10 * * * *"


# ── Helper functions ───────────────────────────────────────


class TestFormatHour:
    def test_midnight(self):
        assert _format_hour(0) == "12 AM"

    def test_morning(self):
        assert _format_hour(9) == "9 AM"

    def test_noon(self):
        assert _format_hour(12) == "12 PM"

    def test_afternoon(self):
        assert _format_hour(15) == "3 PM"

    def test_evening(self):
        assert _format_hour(23) == "11 PM"


class TestFormatTime:
    def test_on_hour(self):
        result = _format_time(9, 0)
        assert "09:00" in result
        assert "AM" in result

    def test_with_minutes(self):
        result = _format_time(14, 30)
        assert "02:30" in result
        assert "PM" in result

    def test_midnight(self):
        result = _format_time(0, 0)
        assert "12" in result
        assert "AM" in result


class TestOrdinal:
    def test_first(self):
        assert _ordinal("1") == "1st"

    def test_second(self):
        assert _ordinal("2") == "2nd"

    def test_third(self):
        assert _ordinal("3") == "3rd"

    def test_eleventh(self):
        assert _ordinal("11") == "11th"

    def test_twenty_first(self):
        assert _ordinal("21") == "21st"

    def test_non_number(self):
        assert _ordinal("abc") == "abc"
