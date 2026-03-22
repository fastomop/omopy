"""Tests for omopy.vis._plot — plot rendering functions."""

import polars as pl
import pytest

from omopy.generics.summarised_result import SummarisedResult
from omopy.vis import mock_summarised_result
from omopy.vis._plot import (
    _prepare_plot_data,
    _with_opacity,
    bar_plot,
    box_plot,
    scatter_plot,
)
from omopy.vis._style import PlotStyle


@pytest.fixture()
def sr() -> SummarisedResult:
    return mock_summarised_result()


@pytest.fixture()
def tidy_df(sr: SummarisedResult) -> pl.DataFrame:
    return _prepare_plot_data(sr)


# ── scatter_plot ──────────────────────────────────────────────────────────


class TestScatterPlot:
    def test_returns_plotly_figure(self, tidy_df: pl.DataFrame):
        import plotly.graph_objects as go
        fig = scatter_plot(tidy_df, x="cohort_name", y="count")
        assert isinstance(fig, go.Figure)

    def test_from_summarised_result(self, sr: SummarisedResult):
        import plotly.graph_objects as go
        fig = scatter_plot(sr, x="cohort_name", y="count")
        assert isinstance(fig, go.Figure)

    def test_with_colour(self, tidy_df: pl.DataFrame):
        import plotly.graph_objects as go
        # Only use colour if the column exists and has data
        if "sex" in tidy_df.columns:
            fig = scatter_plot(tidy_df, x="cohort_name", y="count", colour="sex")
        else:
            fig = scatter_plot(tidy_df, x="cohort_name", y="count")
        assert isinstance(fig, go.Figure)

    def test_with_line(self, tidy_df: pl.DataFrame):
        import plotly.graph_objects as go
        fig = scatter_plot(tidy_df, x="cohort_name", y="count", line=True)
        assert isinstance(fig, go.Figure)

    def test_with_line_and_point(self, tidy_df: pl.DataFrame):
        import plotly.graph_objects as go
        fig = scatter_plot(tidy_df, x="cohort_name", y="count", line=True, point=True)
        assert isinstance(fig, go.Figure)

    def test_line_only(self, tidy_df: pl.DataFrame):
        import plotly.graph_objects as go
        fig = scatter_plot(tidy_df, x="cohort_name", y="count", line=True, point=False)
        assert isinstance(fig, go.Figure)

    def test_missing_column_raises(self, tidy_df: pl.DataFrame):
        with pytest.raises(ValueError, match="not in the data"):
            scatter_plot(tidy_df, x="nonexistent", y="count")

    def test_custom_style(self, tidy_df: pl.DataFrame):
        import plotly.graph_objects as go
        style = PlotStyle(background_color="#f0f0f0")
        fig = scatter_plot(tidy_df, x="cohort_name", y="count", style=style)
        assert isinstance(fig, go.Figure)

    def test_with_title(self, tidy_df: pl.DataFrame):
        import plotly.graph_objects as go
        fig = scatter_plot(tidy_df, x="cohort_name", y="count", title="My Plot")
        assert isinstance(fig, go.Figure)
        assert fig.layout.title.text == "My Plot"

    def test_with_axis_titles(self, tidy_df: pl.DataFrame):
        import plotly.graph_objects as go
        fig = scatter_plot(
            tidy_df, x="cohort_name", y="count",
            x_title="Cohort", y_title="Count"
        )
        assert fig.layout.xaxis.title.text == "Cohort"
        assert fig.layout.yaxis.title.text == "Count"


# ── bar_plot ──────────────────────────────────────────────────────────────


class TestBarPlot:
    def test_returns_plotly_figure(self, tidy_df: pl.DataFrame):
        import plotly.graph_objects as go
        fig = bar_plot(tidy_df, x="cohort_name", y="count")
        assert isinstance(fig, go.Figure)

    def test_from_summarised_result(self, sr: SummarisedResult):
        import plotly.graph_objects as go
        fig = bar_plot(sr, x="cohort_name", y="count")
        assert isinstance(fig, go.Figure)

    def test_stacked(self, tidy_df: pl.DataFrame):
        import plotly.graph_objects as go
        fig = bar_plot(tidy_df, x="cohort_name", y="count", position="stack")
        assert isinstance(fig, go.Figure)

    def test_dodged(self, tidy_df: pl.DataFrame):
        import plotly.graph_objects as go
        fig = bar_plot(tidy_df, x="cohort_name", y="count", position="dodge")
        assert isinstance(fig, go.Figure)

    def test_missing_column_raises(self, tidy_df: pl.DataFrame):
        with pytest.raises(ValueError, match="not in the data"):
            bar_plot(tidy_df, x="nonexistent", y="count")

    def test_with_title(self, tidy_df: pl.DataFrame):
        import plotly.graph_objects as go
        fig = bar_plot(tidy_df, x="cohort_name", y="count", title="Bar Chart")
        assert isinstance(fig, go.Figure)


# ── box_plot ──────────────────────────────────────────────────────────────


class TestBoxPlot:
    def test_returns_plotly_figure(self):
        import plotly.graph_objects as go
        df = pl.DataFrame({
            "category": ["A", "B"],
            "min": ["10", "20"],
            "q25": ["20", "30"],
            "median": ["30", "40"],
            "q75": ["40", "50"],
            "max": ["50", "60"],
        })
        fig = box_plot(df, x="category")
        assert isinstance(fig, go.Figure)

    def test_missing_columns_raises(self):
        df = pl.DataFrame({"category": ["A"], "value": [1]})
        with pytest.raises(ValueError, match="not found"):
            box_plot(df, x="category")

    def test_custom_statistic_columns(self):
        import plotly.graph_objects as go
        df = pl.DataFrame({
            "grp": ["A"],
            "lo": ["10"], "p25": ["20"], "med": ["30"],
            "p75": ["40"], "hi": ["50"],
        })
        fig = box_plot(
            df, x="grp",
            y_min="lo", lower="p25", middle="med", upper="p75", y_max="hi",
        )
        assert isinstance(fig, go.Figure)

    def test_with_colour(self):
        import plotly.graph_objects as go
        df = pl.DataFrame({
            "category": ["A", "A", "B", "B"],
            "group": ["x", "y", "x", "y"],
            "min": ["10", "15", "20", "25"],
            "q25": ["20", "25", "30", "35"],
            "median": ["30", "35", "40", "45"],
            "q75": ["40", "45", "50", "55"],
            "max": ["50", "55", "60", "65"],
        })
        fig = box_plot(df, x="category", colour="group")
        assert isinstance(fig, go.Figure)
        assert len(fig.data) == 2  # Two colour groups


# ── Internal helpers ──────────────────────────────────────────────────────


class TestPrepareData:
    def test_from_summarised_result(self, sr: SummarisedResult):
        df = _prepare_plot_data(sr)
        assert isinstance(df, pl.DataFrame)
        # Should have pivoted estimates as columns
        assert "count" in df.columns or "mean" in df.columns

    def test_from_dataframe(self):
        df = pl.DataFrame({"a": [1, 2], "b": [3, 4]})
        result = _prepare_plot_data(df)
        assert result.equals(df)


class TestWithOpacity:
    def test_hex_to_rgba(self):
        result = _with_opacity("#4361ee", 0.5)
        assert result == "rgba(67,97,238,0.5)"

    def test_black(self):
        result = _with_opacity("#000000", 1.0)
        assert result == "rgba(0,0,0,1.0)"

    def test_white(self):
        result = _with_opacity("#ffffff", 0.2)
        assert result == "rgba(255,255,255,0.2)"
