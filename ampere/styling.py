from dataclasses import asdict, dataclass
from enum import StrEnum, auto
from typing import Any

import colorlover
import pandas as pd


class AmperePalette(StrEnum):
    PAGE_ACCENT_COLOR = "#304FFE"
    PAGE_ACCENT_COLOR2 = "#5560fa"
    PAGE_LIGHT_GRAY = "#EEEEEE"
    BRAND_TEXT_COLOR = "#FFFFFF"
    BRAND_TEXT_COLOR_MUTED = "#E3E7FA"
    PAGE_BACKGROUND_COLOR_LIGHT = "rgb(240, 240, 240)"
    PAGE_BACKGROUND_COLOR_DARK = "rgb(30, 30, 30)"


class ScreenWidth(StrEnum):
    xs = auto()
    sm = auto()
    md = auto()
    lg = auto()
    xl = auto()


@dataclass
class DTStyle:
    sort_action: str
    sort_mode: str
    column_selectable: str
    row_selectable: bool
    row_deletable: bool
    fixed_rows: dict[str, bool]
    filter_options: dict[str, str]
    page_size: int
    style_header: dict[str, str]
    filter_action: str
    style_filter: dict[str, str]
    style_cell: dict[str, Any]
    style_cell_conditional: list[dict[str, Any]]
    style_data_conditional: list[dict[str, Any]]
    style_data: dict[str, str]
    style_table: dict[str, Any]
    css: list[dict[str, Any]]


@dataclass
class ColumnInfo:
    name: str
    ascending: bool
    palette: str


def style_dt_background_colors_by_rank(
    df: pd.DataFrame, n_bins: int, cols: list[ColumnInfo]
) -> list[dict]:
    # https://dash.plotly.com/datatable/conditional-formatting
    styles = []

    for col in cols:
        colors = colorlover.scales[str(n_bins)]["seq"][col.palette]
        ranks = (
            (df[col.name].rank(ascending=col.ascending, method="max").astype("int") - 1)
            .squeeze()
            .tolist()  # type: ignore
        )
        for row in range(n_bins):
            row_val = df[col.name].iloc[row]
            rank_val = ranks[row]
            styles.append(
                {
                    "if": {
                        "filter_query": f"{{{col.name}}} = {row_val}",
                        "column_id": col.name,
                    },
                    "backgroundColor": colors[rank_val],
                    "color": "white" if rank_val > 4 else "inherit",
                }
            )

    return styles


AmpereDTStyle = asdict(
    DTStyle(
        sort_action="native",
        sort_mode="multi",
        column_selectable="single",
        row_selectable=False,
        row_deletable=False,
        fixed_rows={"headers": True},
        filter_action="native",
        filter_options={"case": "insensitive", "placeholder_text": ""},
        page_size=100,
        style_header={
            "backgroundColor": AmperePalette.PAGE_ACCENT_COLOR,
            "color": AmperePalette.BRAND_TEXT_COLOR,
            # account for filters
            "paddingRight": "12px",
            "margin": "0",
            "fontWeight": "bold",
        },
        style_filter={
            "borderTop": "0",
            "borderBottom": "2px solid black",
            "backgroundColor": AmperePalette.BRAND_TEXT_COLOR,
        },
        style_cell={
            "textAlign": "center",
            "minWidth": 100,
            "maxWidth": 170,
            "font_size": "1em",
            "whiteSpace": "normal",
            "height": "auto",
            "font-family": "sans-serif",
            "borderTop": "0",
            "borderBottom": "0",
            "paddingRight": "5px",
            "paddingLeft": "5px",
            "borderLeft": "2px solid black",
            "borderRight": "2px solid black",
        },
        style_cell_conditional=[],
        style_data_conditional=[
            {
                "if": {"row_index": "odd"},
                "backgroundColor": AmperePalette.PAGE_LIGHT_GRAY,
            },
        ],
        style_data={"color": "black", "backgroundColor": "white"},
        css=[dict(selector="p", rule="margin-bottom: 0; text-align: right;")],
        style_table={
            "height": "85vh",
            "maxHeight": "85vh",
            "overflowY": "scroll",
            "overflowX": "scroll",
            "margin": {"b": 100},
            "borderBottom": "2px solid black",
            "borderTop": "2px solid black",
            "borderLeft": "1px solid black",
        },
    )
)

table_title_style = {
    "color": AmperePalette.BRAND_TEXT_COLOR,
    "backgroundColor": AmperePalette.PAGE_ACCENT_COLOR,
    "paddingBottom": "0",
    "paddingLeft": "10px",
    "paddingRight": "10px",
    "fontSize": "1.5rem",
    "fontWeight": "bold",
    "marginBottom": "0",
    "border": "none",
}
