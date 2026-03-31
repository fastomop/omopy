"""Plot functions for treatment pathways.

Provides Sankey diagrams, sunburst charts, and event duration box plots
using plotly.
"""

from __future__ import annotations

from typing import Any

import polars as pl

from omopy.generics import SummarisedResult

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _filter_result_type(
    result: SummarisedResult,
    result_type: str,
) -> SummarisedResult:
    """Filter a SummarisedResult to a specific result_type."""
    matching_ids = result.settings.filter(pl.col("result_type") == result_type)[
        "result_id"
    ].to_list()

    if not matching_ids:
        return result

    # Cast matching_ids to match each column's dtype
    data_dtype = result.data["result_id"].dtype
    data_ids = [str(x) for x in matching_ids] if data_dtype == pl.Utf8 else matching_ids

    settings_dtype = result.settings["result_id"].dtype
    if settings_dtype == pl.Utf8:
        settings_ids = [str(x) for x in matching_ids]
    else:
        settings_ids = matching_ids

    data = result.data.filter(pl.col("result_id").is_in(data_ids))
    settings = result.settings.filter(pl.col("result_id").is_in(settings_ids))
    return SummarisedResult(data, settings=settings)


def _extract_pathway_counts(
    result: SummarisedResult,
    *,
    group_combinations: bool = False,
) -> pl.DataFrame:
    """Extract pathway -> freq mapping from a SummarisedResult.

    Returns DataFrame with columns: pathway, freq.
    """
    data = result.data.filter(
        (pl.col("variable_name") == "treatment_pathway")
        & (pl.col("estimate_name") == "count")
    )

    if data.height == 0:
        return pl.DataFrame(schema={"pathway": pl.Utf8, "freq": pl.Int64})

    pathways = data.select(
        pl.col("variable_level").alias("pathway"),
        pl.col("estimate_value").cast(pl.Int64).alias("freq"),
    )

    if group_combinations:
        pathways = pathways.with_columns(
            pl.col("pathway")
            .str.replace_all(r"[^-]+([\+][^-]+)+", "Combination")
            .alias("pathway")
        )
        # Re-aggregate after combination grouping
        pathways = pathways.group_by("pathway").agg(pl.col("freq").sum())

    return pathways.sort("freq", descending=True)


# ---------------------------------------------------------------------------
# Public API: plot_sankey
# ---------------------------------------------------------------------------


def plot_sankey(
    result: SummarisedResult,
    *,
    group_combinations: bool = False,
    colors: dict[str, str] | list[str] | None = None,
    max_paths: int = 20,
    title: str = "Treatment Pathways",
) -> Any:
    """Create a Sankey diagram of treatment pathways.

    Each treatment line is represented as a column of nodes. Links flow
    from one treatment step to the next, with width proportional to
    patient count.

    Parameters
    ----------
    result
        A ``SummarisedResult`` with
        ``result_type="summarise_treatment_pathways"``.
    group_combinations
        If ``True``, replace combination treatments (e.g. ``"A+B"``)
        with a generic ``"Combination"`` label.
    colors
        Optional color mapping. Either a ``dict`` mapping treatment names
        to hex colors, or a ``list`` of hex colors to cycle through.
    max_paths
        Maximum number of pathways to display (top N by frequency).
    title
        Chart title.

    Returns
    -------
    plotly.graph_objects.Figure
        Sankey diagram figure.
    """
    import plotly.graph_objects as go

    result = _filter_result_type(result, "summarise_treatment_pathways")
    pathways = _extract_pathway_counts(result, group_combinations=group_combinations)

    if pathways.height == 0:
        fig = go.Figure()
        fig.update_layout(title=title)
        return fig

    # Limit to top N
    pathways = pathways.head(max_paths)

    # Parse pathways into links
    # Each pathway is "A-B-C", meaning step 1=A, step 2=B, step 3=C
    nodes: dict[str, int] = {}
    links_source: list[int] = []
    links_target: list[int] = []
    links_value: list[int] = []

    def _get_node(name: str, step: int) -> int:
        key = f"{step + 1}. {name}"
        if key not in nodes:
            nodes[key] = len(nodes)
        return nodes[key]

    for row in pathways.iter_rows(named=True):
        steps = row["pathway"].split("-")
        freq = row["freq"]
        for i in range(len(steps) - 1):
            src = _get_node(steps[i], i)
            tgt = _get_node(steps[i + 1], i + 1)
            links_source.append(src)
            links_target.append(tgt)
            links_value.append(freq)

        # If single-step pathway, add a "Stopped" node
        if len(steps) == 1:
            src = _get_node(steps[0], 0)
            tgt = _get_node("Stopped", 1)
            links_source.append(src)
            links_target.append(tgt)
            links_value.append(freq)

    # Build node colors
    node_labels = list(nodes.keys())
    node_colors = _assign_colors(node_labels, colors)

    # Aggregate duplicate links
    link_map: dict[tuple[int, int], int] = {}
    for s, t, v in zip(links_source, links_target, links_value, strict=False):
        key = (s, t)
        link_map[key] = link_map.get(key, 0) + v

    agg_source = [k[0] for k in link_map]
    agg_target = [k[1] for k in link_map]
    agg_value = list(link_map.values())

    fig = go.Figure(
        data=[
            go.Sankey(
                node=dict(
                    pad=15,
                    thickness=20,
                    line=dict(color="black", width=0.5),
                    label=node_labels,
                    color=node_colors,
                ),
                link=dict(
                    source=agg_source,
                    target=agg_target,
                    value=agg_value,
                ),
            )
        ]
    )

    fig.update_layout(title_text=title, font_size=12)
    return fig


def _assign_colors(
    labels: list[str],
    colors: dict[str, str] | list[str] | None,
) -> list[str]:
    """Assign colors to Sankey nodes."""
    # Default plotly category20 palette
    default_palette = [
        "#1f77b4",
        "#ff7f0e",
        "#2ca02c",
        "#d62728",
        "#9467bd",
        "#8c564b",
        "#e377c2",
        "#7f7f7f",
        "#bcbd22",
        "#17becf",
        "#aec7e8",
        "#ffbb78",
        "#98df8a",
        "#ff9896",
        "#c5b0d5",
        "#c49c94",
        "#f7b6d2",
        "#c7c7c7",
        "#dbdb8d",
        "#9edae5",
    ]

    if isinstance(colors, dict):
        return [
            colors.get(
                label.split(". ", 1)[-1], default_palette[i % len(default_palette)]
            )
            for i, label in enumerate(labels)
        ]
    elif isinstance(colors, list):
        return [colors[i % len(colors)] for i in range(len(labels))]
    else:
        return [default_palette[i % len(default_palette)] for i in range(len(labels))]


# ---------------------------------------------------------------------------
# Public API: plot_sunburst
# ---------------------------------------------------------------------------


def plot_sunburst(
    result: SummarisedResult,
    *,
    group_combinations: bool = False,
    colors: dict[str, str] | list[str] | None = None,
    max_paths: int = 30,
    title: str = "Treatment Pathways",
    unit: str = "percent",
) -> Any:
    """Create a sunburst chart of treatment pathways.

    Inner ring represents first-line treatment; outer rings represent
    subsequent treatment lines.

    Parameters
    ----------
    result
        A ``SummarisedResult`` with
        ``result_type="summarise_treatment_pathways"``.
    group_combinations
        If ``True``, replace combination treatments with ``"Combination"``.
    colors
        Optional color mapping for treatments.
    max_paths
        Maximum number of pathways to display.
    title
        Chart title.
    unit
        ``"percent"`` or ``"count"`` for hover labels.

    Returns
    -------
    plotly.graph_objects.Figure
        Sunburst chart figure.
    """
    import plotly.graph_objects as go

    result = _filter_result_type(result, "summarise_treatment_pathways")
    pathways = _extract_pathway_counts(result, group_combinations=group_combinations)

    if pathways.height == 0:
        fig = go.Figure()
        fig.update_layout(title=title)
        return fig

    pathways = pathways.head(max_paths)

    # Build sunburst data
    # IDs: hierarchical path like "A", "A-B", "A-B-C"
    ids: list[str] = []
    labels: list[str] = []
    parents: list[str] = []
    values: list[int] = []

    pathways["freq"].sum()

    for row in pathways.iter_rows(named=True):
        steps = row["pathway"].split("-")
        freq = row["freq"]

        for i, step in enumerate(steps):
            # Build hierarchical ID
            path_id = "-".join(steps[: i + 1])
            parent_id = "-".join(steps[:i]) if i > 0 else ""

            if path_id not in ids:
                ids.append(path_id)
                labels.append(step)
                parents.append(parent_id)
                values.append(0)

            # Add value only to leaf nodes
            if i == len(steps) - 1:
                idx = ids.index(path_id)
                values[idx] += freq

    # Build color mapping
    unique_labels = sorted(set(labels))
    if isinstance(colors, dict):
        color_map = colors
    else:
        palette = [
            "#1f77b4",
            "#ff7f0e",
            "#2ca02c",
            "#d62728",
            "#9467bd",
            "#8c564b",
            "#e377c2",
            "#7f7f7f",
            "#bcbd22",
            "#17becf",
        ]
        if isinstance(colors, list):
            palette = colors
        color_map = {
            name: palette[i % len(palette)] for i, name in enumerate(unique_labels)
        }

    marker_colors = [color_map.get(label, "#ccc") for label in labels]

    hover_template = (
        "%{label}<br>%{percentRoot:.1%}"
        if unit == "percent"
        else "%{label}<br>%{value}"
    )

    fig = go.Figure(
        go.Sunburst(
            ids=ids,
            labels=labels,
            parents=parents,
            values=values,
            branchvalues="total",
            marker=dict(colors=marker_colors),
            hovertemplate=hover_template,
        )
    )

    fig.update_layout(title_text=title, margin=dict(t=40, l=0, r=0, b=0))
    return fig


# ---------------------------------------------------------------------------
# Public API: plot_event_duration
# ---------------------------------------------------------------------------


def plot_event_duration(
    result: SummarisedResult,
    *,
    min_cell_count: int = 0,
    treatment_groups: str = "both",
    event_lines: list[int] | None = None,
    include_overall: bool = True,
    title: str = "Event Duration",
) -> Any:
    """Create box plots of treatment event durations.

    Parameters
    ----------
    result
        A ``SummarisedResult`` with
        ``result_type="summarise_event_duration"``.
    min_cell_count
        Filter events with count below this threshold.
    treatment_groups
        ``"both"`` (mono + combination + individual), ``"group"``
        (mono/combination only), ``"individual"`` (per-drug only).
    event_lines
        Pathway positions to include (``None`` = all).
    include_overall
        Include the ``"overall"`` line aggregation.
    title
        Chart title.

    Returns
    -------
    plotly.graph_objects.Figure
        Box plot figure.
    """
    import plotly.graph_objects as go

    result = _filter_result_type(result, "summarise_event_duration")
    data = result.data

    if data.height == 0:
        fig = go.Figure()
        fig.update_layout(title=title)
        return fig

    # Filter by line
    if event_lines is not None:
        line_strs = [str(x) for x in event_lines]
        if include_overall:
            line_strs.append("overall")
        data = data.filter(pl.col("additional_level").is_in(line_strs))
    elif not include_overall:
        data = data.filter(pl.col("additional_level") != "overall")

    # Filter by treatment group
    if treatment_groups == "group":
        data = data.filter(
            pl.col("variable_name").is_in(["mono-event", "combination-event"])
        )
    elif treatment_groups == "individual":
        data = data.filter(
            ~pl.col("variable_name").is_in(["mono-event", "combination-event"])
        )

    # Filter by min_cell_count
    if min_cell_count > 0:
        count_data = data.filter(pl.col("estimate_name") == "count")
        valid_events = (
            count_data.filter(pl.col("estimate_value").cast(pl.Int64) >= min_cell_count)
            .select("variable_name", "additional_level")
            .unique()
        )
        data = data.join(
            valid_events, on=["variable_name", "additional_level"], how="inner"
        )

    if data.height == 0:
        fig = go.Figure()
        fig.update_layout(title=title)
        return fig

    # Pivot to get stats per event
    stats_rows = []
    for (var_name, line), group in data.group_by(["variable_name", "additional_level"]):
        stats: dict[str, Any] = {"event_name": var_name, "line": line}
        for row in group.iter_rows(named=True):
            est = row["estimate_name"]
            val = row["estimate_value"]
            if val != "NA":
                try:
                    stats[est] = float(val)
                except ValueError, TypeError:
                    stats[est] = None
            else:
                stats[est] = None
        stats_rows.append(stats)

    if not stats_rows:
        fig = go.Figure()
        fig.update_layout(title=title)
        return fig

    # Create box plots using precomputed statistics
    fig = go.Figure()

    # Group by line for faceting via subplot traces
    lines = sorted(set(s["line"] for s in stats_rows))
    for line in lines:
        line_stats = [s for s in stats_rows if s["line"] == line]
        for s in line_stats:
            fig.add_trace(
                go.Box(
                    name=f"{s['event_name']}",
                    legendgroup=s["event_name"],
                    showlegend=(line == lines[0]),
                    q1=[s.get("q25")],
                    median=[s.get("median")],
                    q3=[s.get("q75")],
                    lowerfence=[s.get("min")],
                    upperfence=[s.get("max")],
                    mean=[s.get("mean")],
                    x=[f"Line {line}"],
                    boxpoints=False,
                )
            )

    fig.update_layout(
        title=title,
        xaxis_title="Treatment Line",
        yaxis_title="Duration (days)",
        boxmode="group",
    )

    return fig
