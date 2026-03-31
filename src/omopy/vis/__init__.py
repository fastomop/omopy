"""``omopy.vis`` — Visualization and formatting for OMOP CDM results.

This subpackage provides functions to format, tabulate, and plot
summarised results. It is the Python equivalent of the R
``visOmopResults`` package.

Table rendering uses ``great_tables`` (Python port of R's ``gt``);
plot rendering uses ``plotly``.

Core workflow::

    from omopy.vis import (
        format_estimate_value,
        format_estimate_name,
        vis_omop_table,
        scatter_plot,
        bar_plot,
    )

Format pipeline (composable)::

    result = format_estimate_value(result)
    result = format_estimate_name(
        result, estimate_name={"N (%)": "<count> (<percentage>%)"}
    )
    table = vis_omop_table(result, header=["cohort_name"])
"""

from omopy.vis._format import (
    format_estimate_name,
    format_estimate_value,
    format_header,
    format_min_cell_count,
    tidy_columns,
    tidy_result,
)
from omopy.vis._mock import mock_summarised_result
from omopy.vis._plot import (
    bar_plot,
    box_plot,
    scatter_plot,
)
from omopy.vis._style import (
    PlotStyle,
    TableStyle,
    customise_text,
    default_plot_style,
    default_table_style,
)
from omopy.vis._table import (
    format_table,
    vis_omop_table,
    vis_table,
)

__all__ = [
    # Style
    "PlotStyle",
    "TableStyle",
    # Plots
    "bar_plot",
    "box_plot",
    "customise_text",
    "default_plot_style",
    "default_table_style",
    # Format pipeline
    "format_estimate_name",
    "format_estimate_value",
    "format_header",
    "format_min_cell_count",
    "format_table",
    # Mock data
    "mock_summarised_result",
    "scatter_plot",
    # Tidy helpers
    "tidy_columns",
    "tidy_result",
    # High-level table
    "vis_omop_table",
    "vis_table",
]
