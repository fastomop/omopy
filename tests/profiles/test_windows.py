"""Tests for omopy.profiles._windows — window utilities."""

from __future__ import annotations

import math

import pytest

from omopy.profiles._windows import (
    _to_snake_case,
    format_name_style,
    validate_windows,
    window_name,
)

# ---------------------------------------------------------------------------
# validate_windows
# ---------------------------------------------------------------------------


class TestValidateWindows:
    def test_single_tuple(self):
        result = validate_windows((0, float("inf")))
        assert result == [(0.0, float("inf"))]

    def test_list_of_tuples(self):
        result = validate_windows([(-365, -1), (0, 0), (1, 365)])
        assert len(result) == 3
        assert result[0] == (-365.0, -1.0)
        assert result[1] == (0.0, 0.0)
        assert result[2] == (1.0, 365.0)

    def test_infinite_bounds(self):
        result = validate_windows((float("-inf"), float("inf")))
        assert math.isinf(result[0][0]) and result[0][0] < 0
        assert math.isinf(result[0][1]) and result[0][1] > 0

    def test_invalid_order_raises(self):
        with pytest.raises(ValueError, match=r"lower bound.*upper bound"):
            validate_windows((5, 3))

    def test_wrong_length_raises(self):
        with pytest.raises(ValueError, match="pair"):
            validate_windows([(1, 2, 3)])  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# window_name
# ---------------------------------------------------------------------------


class TestWindowName:
    def test_zero_to_inf(self):
        assert window_name((0, float("inf"))) == "0_to_inf"

    def test_minf_to_m1(self):
        assert window_name((float("-inf"), -1)) == "minf_to_m1"

    def test_m365_to_m1(self):
        assert window_name((-365, -1)) == "m365_to_m1"

    def test_zero_to_zero(self):
        assert window_name((0, 0)) == "0_to_0"

    def test_minf_to_inf(self):
        assert window_name((float("-inf"), float("inf"))) == "minf_to_inf"

    def test_positive_range(self):
        assert window_name((1, 365)) == "1_to_365"

    def test_negative_range(self):
        assert window_name((-30, -1)) == "m30_to_m1"

    def test_mixed_range(self):
        assert window_name((-30, 30)) == "m30_to_30"


# ---------------------------------------------------------------------------
# format_name_style
# ---------------------------------------------------------------------------


class TestFormatNameStyle:
    def test_basic_template(self):
        result = format_name_style(
            "{cohort_name}_{window_name}",
            cohort_name="my_cohort",
            window_name="0_to_inf",
        )
        assert result == "my_cohort_0_to_inf"

    def test_uppercase_to_snake(self):
        result = format_name_style(
            "{cohort_name}_{window_name}",
            cohort_name="My Cohort",
            window_name="0_to_inf",
        )
        assert result == "my_cohort_0_to_inf"

    def test_multiple_placeholders(self):
        result = format_name_style(
            "{table_name}_{field}_{window_name}",
            table_name="drug_exposure",
            field="drug_name",
            window_name="m365_to_m1",
        )
        assert result == "drug_exposure_drug_name_m365_to_m1"


# ---------------------------------------------------------------------------
# _to_snake_case
# ---------------------------------------------------------------------------


class TestToSnakeCase:
    def test_already_snake(self):
        assert _to_snake_case("hello_world") == "hello_world"

    def test_camel_case(self):
        assert _to_snake_case("helloWorld") == "hello_world"

    def test_pascal_case(self):
        assert _to_snake_case("HelloWorld") == "hello_world"

    def test_spaces(self):
        assert _to_snake_case("hello world") == "hello_world"

    def test_hyphens(self):
        assert _to_snake_case("hello-world") == "hello_world"

    def test_multiple_separators(self):
        assert _to_snake_case("hello--world__test") == "hello_world_test"

    def test_leading_trailing(self):
        assert _to_snake_case("_hello_") == "hello"
