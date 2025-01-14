import datetime
import pickle
from pathlib import Path
from typing import Any, Optional

import networkx as nx
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pypalettes
import pytz
from plotly.graph_objs import Figure

from ampere.common import get_frontend_db_con, timeit
from ampere.get_repo_metrics import read_repos
from ampere.styling import AmperePalette, ScreenWidth


@timeit
def generate_repo_palette() -> dict[str, str]:
    with get_frontend_db_con() as con:
        repos = sorted(
            read_repos(con),
            key=lambda x: x.stargazers_count,
            reverse=True,
        )

    n_colors = 10
    n_repos = len(repos)
    repeats = (n_repos // n_colors) + 1

    colors = list(
        pypalettes.load_cmap("Tableau_10", cmap_type="discrete", repeat=repeats).rgb  # type: ignore
    )[0:n_repos]

    output = {}
    for i, repo in enumerate(repos):
        rgb_string = ", ".join(str(x) for x in colors[i])
        output[repo.repo_name] = f"rgb({rgb_string})"

    return output


def format_plot_name_list(
    names: list[str] | float | None, max_names: int = 5
) -> Optional[str]:
    if names is None or isinstance(names, float) or isinstance(names, int):
        return None

    names_clean = names[0 : min(max_names, len(names))]

    names_clean_str = ", ".join(names_clean)
    if len(names) > max_names:
        names_clean_str += "..."

    return names_clean_str


def read_pickle(pkl_name: str) -> Any:
    out_dir = Path(__file__).parents[1] / "data" / "viz"
    out_path = out_dir / f"{pkl_name}.pkl"
    with out_path.open("rb") as f:
        obj = pickle.load(f)
    return obj


def read_network_graph_pickle(pkl_name: str) -> nx.Graph:
    return read_pickle(pkl_name)


@timeit
def read_plotly_fig_pickle(pkl_name: str) -> Figure:
    return read_pickle(pkl_name)


def viz_summary(
    df: pd.DataFrame,
    metric_type: str,
    date_range: Optional[list[int]] = None,
    show_fig: bool = False,
    screen_width: ScreenWidth = ScreenWidth.lg,
    dark_mode: bool = False,
) -> Figure:
    df_filtered = df.query(f"metric_type == '{metric_type}'").sort_values("metric_date")
    if date_range is not None:
        filter_date_min = datetime.datetime.fromtimestamp(
            date_range[0], tz=pytz.timezone("America/New_York")
        )
        filter_date_max = datetime.datetime.fromtimestamp(
            date_range[1], tz=pytz.timezone("America/New_York")
        )

        df_filtered = df_filtered.query(f"metric_date >= '{filter_date_min}'").query(
            f"metric_date <= '{filter_date_max}'"
        )

    if dark_mode:
        font_color = "white"
        bg_color = AmperePalette.PAGE_BACKGROUND_COLOR_DARK
        template = "plotly_dark"
    else:
        font_color = "black"
        bg_color = AmperePalette.PAGE_BACKGROUND_COLOR_LIGHT
        template = "plotly_white"

    repo_palette = generate_repo_palette()
    fig = px.area(
        df_filtered,
        x="metric_date",
        y="metric_count",
        color="repo_name",
        template=template,
        hover_name="repo_name",
        color_discrete_map=repo_palette,
        height=500,
        category_orders={"repo_name": repo_palette.keys()},
        facet_col="metric_type",  # single var facet col for plot title
    )
    fig.update_layout(plot_bgcolor=bg_color, paper_bgcolor=bg_color)
    fig.for_each_annotation(
        lambda a: a.update(
            text="<b>" + a.text.split("=")[-1] + "</b>",
            font_size=18,
            bgcolor=AmperePalette.PAGE_ACCENT_COLOR2,
            font_color="white",
            borderpad=5,
            y=1.02,
        )
    )
    fig.update_yaxes(matches=None, showticklabels=True, showgrid=False)
    fig.update_xaxes(showgrid=False)
    fig.update_traces(hovertemplate="<b>%{x}</b><br>n=%{y}")

    fig_legend_y = {ScreenWidth.xs: 1.04, ScreenWidth.sm: 1.02}
    if screen_width in [ScreenWidth.xs, ScreenWidth.sm]:
        fig.update_layout(
            legend=dict(
                title=None,
                itemsizing="constant",
                font=dict(size=14),
                orientation="h",
                yanchor="top",
                y=fig_legend_y[screen_width],
                xanchor="center",
                x=0.5,
            )
        )
    else:
        fig.update_layout(
            legend=dict(title=None, itemsizing="constant", font=dict(size=14))
        )

    fig.for_each_annotation(
        lambda a: a.update(
            text="<b>" + a.text.split("=")[-1] + "</b>",
            font_size=18,
            bgcolor=AmperePalette.PAGE_ACCENT_COLOR2,
            font_color="white",
            borderpad=5,
        )
    )

    fig.for_each_yaxis(
        lambda y: y.update(
            title="",
            showline=True,
            linewidth=1,
            linecolor=font_color,
            mirror=True,
            tickfont_size=14,
        )
    )
    fig.for_each_xaxis(
        lambda x: x.update(
            title="",
            showline=True,
            linewidth=1,
            linecolor=font_color,
            mirror=True,
            showticklabels=True,
            tickfont_size=14,
        )
    )

    fig.update_layout(margin=dict(l=0, r=0))
    if show_fig:
        fig.show()

    return fig


def get_summary_data() -> pd.DataFrame:
    with get_frontend_db_con() as con:
        df = con.sql(
            """
        select
            repo_name,
            metric_type,
            metric_date,
            metric_count,
        from main.mart_repo_summary
        order by metric_date
    """,
        ).to_df()

    return df


NETWORK_LAYOUT = go.Layout(
    showlegend=True,
    hovermode="closest",
    margin=dict(b=20, l=0, r=0, t=55),
    xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
    yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
    template="none",
    legend=dict(
        title=None,
        itemsizing="constant",
        font=dict(size=14),
        orientation="h",
        yanchor="top",
        y=1.04,
        xanchor="center",
        x=0.5,
    ),
)
