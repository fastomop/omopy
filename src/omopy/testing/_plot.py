"""Cohort timeline visualization using Plotly."""

from __future__ import annotations

from typing import Any

import polars as pl

__all__ = ["graph_cohort"]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def graph_cohort(
    subject_id: int,
    cohorts: dict[str, pl.DataFrame],
    *,
    style: Any | None = None,
) -> Any:
    """Plot cohort timelines for a single subject.

    Each cohort is a named DataFrame with columns ``cohort_definition_id``,
    ``subject_id``, ``cohort_start_date``, ``cohort_end_date``.  This
    function draws a horizontal segment for each cohort entry for the
    given ``subject_id``.

    Args:
        subject_id: The subject to visualize.
        cohorts: Mapping of cohort name to cohort DataFrame.
        style: Optional Plotly layout overrides (dict or ``None``).

    Returns:
        A ``plotly.graph_objects.Figure``.

    Raises:
        ValueError: If no cohort records found for the subject, or if
            required columns are missing.
    """
    import plotly.graph_objects as go

    required_cols = {"subject_id", "cohort_start_date", "cohort_end_date"}

    # Validate inputs
    for name, df in cohorts.items():
        missing = required_cols - set(df.columns)
        if missing:
            msg = f"Cohort '{name}' is missing required columns: {sorted(missing)}"
            raise ValueError(msg)

    # Collect segments for this subject
    segments: list[dict[str, Any]] = []
    for cohort_name, df in cohorts.items():
        subject_df = df.filter(pl.col("subject_id") == subject_id)
        if isinstance(subject_df, pl.LazyFrame):
            subject_df = subject_df.collect()
        for row in subject_df.iter_rows(named=True):
            segments.append({
                "cohort_name": cohort_name,
                "start": row["cohort_start_date"],
                "end": row["cohort_end_date"],
                "cohort_id": row.get("cohort_definition_id", 0),
            })

    if not segments:
        msg = f"No cohort records found for subject_id={subject_id}"
        raise ValueError(msg)

    # Build figure
    fig = go.Figure()

    # Assign a y-position per cohort name
    cohort_names = list(dict.fromkeys(s["cohort_name"] for s in segments))
    y_map = {name: i for i, name in enumerate(cohort_names)}

    # Default color palette
    colors = [
        "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
        "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
    ]

    for seg in segments:
        cname = seg["cohort_name"]
        y_val = y_map[cname]
        color = colors[y_val % len(colors)]
        start_str = str(seg["start"])
        end_str = str(seg["end"])

        fig.add_trace(go.Scatter(
            x=[start_str, end_str],
            y=[cname, cname],
            mode="lines+markers",
            line={"color": color, "width": 6},
            marker={"size": 8, "color": color},
            name=cname,
            showlegend=False,
            hovertemplate=(
                f"<b>{cname}</b><br>"
                f"Start: {start_str}<br>"
                f"End: {end_str}<br>"
                f"<extra></extra>"
            ),
        ))

    # Layout
    layout_kwargs: dict[str, Any] = {
        "title": f"Cohort Timeline — Subject {subject_id}",
        "xaxis_title": "Date",
        "yaxis_title": "",
        "yaxis": {
            "categoryorder": "array",
            "categoryarray": cohort_names,
        },
        "showlegend": False,
        "height": max(300, 80 * len(cohort_names) + 100),
    }
    if style is not None and isinstance(style, dict):
        layout_kwargs.update(style)

    fig.update_layout(**layout_kwargs)

    return fig
