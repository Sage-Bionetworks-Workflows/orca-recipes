"""Tests for dags/top-public-synapse-projects-from-snowflake.py."""

import importlib
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

# The module's filename has hyphens, so it can't be reached with a normal
# dotted import - load it by its exact filename instead.
dag_module = importlib.import_module("top-public-synapse-projects-from-snowflake")
raise_if_empty = dag_module.raise_if_empty


class TestRaiseIfEmpty:
    """Tests for raise_if_empty."""

    def test_empty_results_raise_value_error(self) -> None:
        """An empty result raises a ValueError naming what was being queried."""
        # When the results are empty
        with pytest.raises(ValueError) as exc_info:
            raise_if_empty([], "testing")

        # Then the error message names the description
        assert "testing" in str(exc_info.value)

    def test_non_empty_results_do_not_raise(self) -> None:
        """No error is raised when the results are non-empty."""
        raise_if_empty([1, 2, 3], "testing")

    def test_message_reports_queried_date_for_backfill(self) -> None:
        """The reported date is backfill_date minus hours_time_delta, not the raw inputs."""
        # When a backfill run for 2024-01-01 looks back 24 hours
        with pytest.raises(ValueError) as exc_info:
            raise_if_empty(
                [], "testing", backfill_date="2024-01-01", hours_time_delta="24"
            )

        # Then the message reports the date actually queried
        assert "2023-12-31" in str(exc_info.value)

    @patch("top-public-synapse-projects-from-snowflake.datetime")
    def test_message_reports_queried_date_without_backfill_date(
        self, mock_datetime: MagicMock
    ) -> None:
        """Without a backfill_date, the queried date is today's UTC date minus the delta."""
        mock_datetime.now.return_value = datetime(2024, 1, 1, tzinfo=timezone.utc)

        # When a scheduled run looks back 24 hours from today (2024-01-01)
        with pytest.raises(ValueError) as exc_info:
            raise_if_empty([], "testing", hours_time_delta="24")

        # Then the message reports the previous day, resolved in UTC
        assert "2023-12-31" in str(exc_info.value)
        mock_datetime.now.assert_called_once_with(timezone.utc)
