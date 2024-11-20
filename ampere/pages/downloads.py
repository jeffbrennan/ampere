import dash
import pandas as pd
import plotly.express as px
from dash import Input, Output, callback, dcc, html
from plotly.graph_objects import Figure

from ampere.common import get_db_con

dash.register_page(__name__, name="downloads", top_nav=True, order=1)


def create_downloads_summary() -> pd.DataFrame:
    con = get_db_con()
    return con.sql(
        """
            select
            repo,
            download_date,
            group_name,
            group_value,
            download_count
            from mart_downloads_summary
            where group_name <> 'system_name'
            order by download_date, download_count
            """
    ).to_df()


def viz_line(df: pd.DataFrame, group_name: str) -> Figure:
    df_filtered = df.query(f"group_name=='{group_name}'")
    print(df_filtered.shape)
    fig = px.line(
        df_filtered,
        x="download_date",
        y="download_count",
        color="group_value",
        title=group_name,
        template="simple_white",
    )
    fig.for_each_yaxis(
        lambda y: y.update(
            title="",
            showline=True,
            linewidth=1,
            linecolor="black",
            mirror=True,
            tickfont_size=14,
        )
    )
    fig.for_each_xaxis(
        lambda x: x.update(
            title="",
            showline=True,
            linewidth=1,
            linecolor="black",
            mirror=True,
            showticklabels=True,
            tickfont_size=14,
        )
    )
    fig.update_yaxes(matches=None, showticklabels=True)

    fig.update_layout(margin=dict(l=0, r=0))
    return fig


def viz_area(df: pd.DataFrame, repo_name: str, group_name: str) -> Figure:
    df_filtered = df.query(f"group_name=='{group_name}'").query(f"repo=='{repo_name}'")
    max_date = df_filtered["download_date"].max()
    categories = (
        df_filtered[(df_filtered["download_date"] == max_date)]
        .sort_values("download_count", ascending=False)["group_value"]
        .tolist()
    )

    fig = px.area(
        df_filtered,
        x="download_date",
        y="download_count",
        color="group_value",
        title=f"{repo_name} - {group_name}",
        template="simple_white",
        category_orders={"group_value": categories},
    )
    fig.for_each_yaxis(
        lambda y: y.update(
            title="",
            showline=True,
            linewidth=1,
            linecolor="black",
            mirror=True,
            tickfont_size=14,
        )
    )
    fig.for_each_xaxis(
        lambda x: x.update(
            title="",
            showline=True,
            linewidth=1,
            linecolor="black",
            mirror=True,
            showticklabels=True,
            tickfont_size=14,
        )
    )
    fig.update_yaxes(matches=None, showticklabels=True)
    fig.update_layout(margin=dict(l=0, r=0))

    return fig


def get_valid_repos() -> list[str]:
    con = get_db_con()
    result = con.sql("select distinct repo from mart_downloads_summary").to_df()
    return result.squeeze().tolist()


@callback(
    Output("downloads-overall", "figure"),
    [
        Input("repo-selection", "value"),
        Input("breakpoints", "widthBreakpoint"),
    ],
)
def viz_downloads_overall(repo_name: str, breakpoint_name: str) -> Figure:
    df = create_downloads_summary()
    fig = viz_area(df, repo_name, "overall")
    return fig


@callback(
    Output("downloads-cloud", "figure"),
    [
        Input("repo-selection", "value"),
        Input("breakpoints", "widthBreakpoint"),
    ],
)
def viz_downloads_by_cloud_provider(repo_name: str, breakpoint_name: str) -> Figure:
    df = create_downloads_summary()
    fig = viz_area(df, repo_name, "system_release")
    return fig


@callback(
    Output("downloads-python-version", "figure"),
    [
        Input("repo-selection", "value"),
        Input("breakpoints", "widthBreakpoint"),
    ],
)
def viz_downloads_by_python_version(repo_name: str, breakpoint_name: str) -> Figure:
    df = create_downloads_summary()
    fig = viz_area(df, repo_name, "python_version")
    return fig


@callback(
    Output("downloads-package-version", "figure"),
    [
        Input("repo-selection", "value"),
        Input("breakpoints", "widthBreakpoint"),
    ],
)
def viz_downloads_by_package_version(repo_name: str, breakpoint_name: str) -> Figure:
    df = create_downloads_summary()
    fig = viz_area(df, repo_name, "package_version")
    return fig


layout = [
    html.Br(),
    dcc.Dropdown(
        get_valid_repos(),
        placeholder="quinn",
        value="quinn",
        id="repo-selection",
    ),
    dcc.Loading(
        id="loading-graph",
        type="default",
        children=[
            dcc.Graph("downloads-overall"),
            dcc.Graph("downloads-package-version"),
            dcc.Graph("downloads-python-version"),
            dcc.Graph("downloads-cloud"),
        ],
    ),
]
