"""Tests for omopy.vis._style — style configuration and text formatting."""

import pytest

from omopy.vis._style import (
    PlotStyle,
    TableStyle,
    customise_text,
    default_plot_style,
    default_table_style,
)


# ── customise_text ────────────────────────────────────────────────────────


class TestCustomiseText:
    def test_snake_case_to_sentence(self):
        assert customise_text("cohort_name") == "Cohort name"

    def test_single_word(self):
        assert customise_text("age") == "Age"

    def test_list_input(self):
        result = customise_text(["age_group", "sex"])
        assert result == ["Age group", "Sex"]

    def test_custom_replacement(self):
        result = customise_text("cdm_name", custom={"cdm_name": "Database"})
        assert result == "Database"

    def test_keep_unchanged(self):
        result = customise_text("cdm_name", keep=["cdm_name"])
        assert result == "cdm_name"

    def test_custom_function(self):
        result = customise_text("hello_world", fun=str.upper)
        assert result == "HELLO_WORLD"

    def test_custom_overrides_fun(self):
        result = customise_text("cdm_name", fun=str.upper, custom={"cdm_name": "Database"})
        assert result == "Database"

    def test_keep_overrides_fun(self):
        result = customise_text("age", fun=str.upper, keep=["age"])
        assert result == "age"

    def test_empty_string(self):
        assert customise_text("") == ""

    def test_list_with_custom(self):
        result = customise_text(
            ["age_group", "cdm_name"],
            custom={"cdm_name": "Database"},
        )
        assert result == ["Age group", "Database"]


# ── TableStyle ────────────────────────────────────────────────────────────


class TestTableStyle:
    def test_default_construction(self):
        style = TableStyle()
        assert style.header_background == "#4361ee"
        assert style.stripe is True
        assert style.na_display == "\u2013"

    def test_frozen(self):
        style = TableStyle()
        with pytest.raises(AttributeError):
            style.header_background = "#000000"  # type: ignore[misc]

    def test_custom_values(self):
        style = TableStyle(
            header_background="#ff0000",
            stripe=False,
            font_size=16,
        )
        assert style.header_background == "#ff0000"
        assert style.stripe is False
        assert style.font_size == 16

    def test_default_table_style_factory(self):
        style = default_table_style()
        assert isinstance(style, TableStyle)


# ── PlotStyle ─────────────────────────────────────────────────────────────


class TestPlotStyle:
    def test_default_construction(self):
        style = PlotStyle()
        assert len(style.color_palette) == 10
        assert style.background_color == "#ffffff"

    def test_frozen(self):
        style = PlotStyle()
        with pytest.raises(AttributeError):
            style.font_size = 20  # type: ignore[misc]

    def test_custom_palette(self):
        palette = ["#ff0000", "#00ff00"]
        style = PlotStyle(color_palette=palette)
        assert style.color_palette == palette

    def test_default_plot_style_factory(self):
        style = default_plot_style()
        assert isinstance(style, PlotStyle)
