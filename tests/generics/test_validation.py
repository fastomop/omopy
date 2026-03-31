"""Tests for omopy.generics._validation — assertion helpers."""

import datetime

import pytest

from omopy.generics._validation import (
    assert_character,
    assert_choice,
    assert_class,
    assert_date,
    assert_list,
    assert_logical,
    assert_numeric,
    assert_table_columns,
    assert_true,
)

# ---------------------------------------------------------------------------
# assert_character
# ---------------------------------------------------------------------------


class TestAssertCharacter:
    def test_single_string(self):
        assert_character("hello")

    def test_list_of_strings(self):
        assert_character(["a", "b", "c"])

    def test_none_allowed(self):
        assert_character(None, null_allowed=True)

    def test_none_not_allowed(self):
        with pytest.raises(TypeError, match="must not be None"):
            assert_character(None)

    def test_not_a_string(self):
        with pytest.raises(TypeError, match="must be a string"):
            assert_character(42)

    def test_list_with_non_string(self):
        with pytest.raises(TypeError, match="must be a string"):
            assert_character(["a", 42])

    def test_na_in_list_allowed(self):
        assert_character(["a", None, "c"], na_allowed=True)

    def test_na_in_list_not_allowed(self):
        with pytest.raises(ValueError, match="must not be None"):
            assert_character(["a", None], na_allowed=False)

    def test_min_length(self):
        with pytest.raises(ValueError, match="at least 2"):
            assert_character(["a"], min_length=2)

    def test_max_length(self):
        with pytest.raises(ValueError, match="at most 1"):
            assert_character(["a", "b"], max_length=1)

    def test_min_length_ok(self):
        assert_character(["a", "b"], min_length=2)


# ---------------------------------------------------------------------------
# assert_choice
# ---------------------------------------------------------------------------


class TestAssertChoice:
    def test_valid_choice(self):
        assert_choice("a", ["a", "b", "c"])

    def test_invalid_choice(self):
        with pytest.raises(ValueError, match="must be one of"):
            assert_choice("d", ["a", "b", "c"])

    def test_none_allowed(self):
        assert_choice(None, ["a"], null_allowed=True)

    def test_none_not_allowed(self):
        with pytest.raises(TypeError, match="must not be None"):
            assert_choice(None, ["a"])


# ---------------------------------------------------------------------------
# assert_class
# ---------------------------------------------------------------------------


class TestAssertClass:
    def test_valid_class(self):
        assert_class("hello", str)

    def test_invalid_class(self):
        with pytest.raises(TypeError, match="must be an instance of"):
            assert_class(42, str)

    def test_tuple_of_classes(self):
        assert_class(42, (str, int))

    def test_none_allowed(self):
        assert_class(None, str, null_allowed=True)

    def test_none_not_allowed(self):
        with pytest.raises(TypeError, match="must not be None"):
            assert_class(None, str)


# ---------------------------------------------------------------------------
# assert_date
# ---------------------------------------------------------------------------


class TestAssertDate:
    def test_valid_date(self):
        assert_date(datetime.date(2024, 1, 1))

    def test_valid_datetime(self):
        # datetime is a subclass of date
        assert_date(datetime.datetime(2024, 1, 1, 12, 0))

    def test_not_a_date(self):
        with pytest.raises(TypeError, match="must be a date"):
            assert_date("2024-01-01")

    def test_none_allowed(self):
        assert_date(None, null_allowed=True)

    def test_none_not_allowed(self):
        with pytest.raises(TypeError, match="must not be None"):
            assert_date(None)


# ---------------------------------------------------------------------------
# assert_list
# ---------------------------------------------------------------------------


class TestAssertList:
    def test_valid_list(self):
        assert_list([1, 2, 3])

    def test_valid_tuple(self):
        assert_list((1, 2, 3))

    def test_not_a_list(self):
        with pytest.raises(TypeError, match="must be a list or tuple"):
            assert_list("hello")

    def test_element_class(self):
        assert_list([1, 2, 3], element_class=int)

    def test_element_class_failure(self):
        with pytest.raises(TypeError, match="must be an instance of"):
            assert_list([1, "two"], element_class=int)

    def test_min_length(self):
        with pytest.raises(ValueError, match="at least 2"):
            assert_list([1], min_length=2)

    def test_none_allowed(self):
        assert_list(None, null_allowed=True)


# ---------------------------------------------------------------------------
# assert_logical
# ---------------------------------------------------------------------------


class TestAssertLogical:
    def test_true(self):
        assert_logical(True)

    def test_false(self):
        assert_logical(False)

    def test_not_bool(self):
        with pytest.raises(TypeError, match="must be a bool"):
            assert_logical(1)

    def test_none_allowed(self):
        assert_logical(None, null_allowed=True)


# ---------------------------------------------------------------------------
# assert_numeric
# ---------------------------------------------------------------------------


class TestAssertNumeric:
    def test_int(self):
        assert_numeric(42)

    def test_float(self):
        assert_numeric(3.14)

    def test_not_numeric(self):
        with pytest.raises(TypeError, match="must be numeric"):
            assert_numeric("42")

    def test_min_val(self):
        with pytest.raises(ValueError, match=">= 0"):
            assert_numeric(-1, min_val=0)

    def test_max_val(self):
        with pytest.raises(ValueError, match="<= 10"):
            assert_numeric(11, max_val=10)

    def test_range_ok(self):
        assert_numeric(5, min_val=0, max_val=10)

    def test_none_allowed(self):
        assert_numeric(None, null_allowed=True)


# ---------------------------------------------------------------------------
# assert_true
# ---------------------------------------------------------------------------


class TestAssertTrue:
    def test_true(self):
        assert_true(True)

    def test_false(self):
        with pytest.raises(ValueError, match="Assertion failed"):
            assert_true(False)

    def test_custom_message(self):
        with pytest.raises(ValueError, match="custom error"):
            assert_true(False, msg="custom error")


# ---------------------------------------------------------------------------
# assert_table_columns
# ---------------------------------------------------------------------------


class TestAssertTableColumns:
    def test_all_present(self):
        assert_table_columns(
            ["a", "b", "c"],
            required=["a", "b"],
        )

    def test_missing_columns(self):
        with pytest.raises(ValueError, match="missing required columns"):
            assert_table_columns(
                ["a"],
                required=["a", "b", "c"],
                table_name="test_table",
            )

    def test_extra_columns_ok(self):
        # Extra columns are fine — only required matter
        assert_table_columns(
            ["a", "b", "c", "d"],
            required=["a", "b"],
        )
