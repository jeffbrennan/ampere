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
    TABLE_EVEN_ROW_COLOR_DARK = "rgb(30, 30, 30)"
    TABLE_ODD_ROW_COLOR_DARK = "rgb(50, 50, 50)"
    TABLE_EVEN_ROW_COLOR_LIGHT = "rgb(245, 245, 245)"
    TABLE_ODD_ROW_COLOR_LIGHT = "rgb(220, 220, 220)"


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


def get_ampere_dt_style(dark_mode: bool = False) -> dict:
    if dark_mode:
        color = "white"
        even_row_color = AmperePalette.TABLE_EVEN_ROW_COLOR_DARK
        odd_row_color = AmperePalette.TABLE_ODD_ROW_COLOR_DARK
        background_color = AmperePalette.PAGE_BACKGROUND_COLOR_DARK

    else:
        color = "black"
        even_row_color = AmperePalette.TABLE_EVEN_ROW_COLOR_LIGHT
        odd_row_color = AmperePalette.TABLE_ODD_ROW_COLOR_LIGHT
        background_color = AmperePalette.PAGE_BACKGROUND_COLOR_LIGHT

    dt_style = asdict(
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
                "backgroundColor": AmperePalette.PAGE_ACCENT_COLOR2,
                "color": AmperePalette.BRAND_TEXT_COLOR,
                "paddingRight": "12px",
                "margin": "0",
                "fontWeight": "bold",
                "borderLeft": f"2px solid {color}",
                "borderRight": f"2px solid {color}",
            },
            style_filter={
                "backgroundColor": background_color,
                "borderTop": "0",
                "borderBottom": f"2px solid {color}",
                "borderLeft": f"2px solid {color}",
                "borderRight": f"2px solid {color}",
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
                "borderLeft": f"2px solid {color}",
                "borderRight": f"2px solid {color}",
            },
            style_cell_conditional=[],
            style_data_conditional=[
                {
                    "if": {"row_index": "even"},
                    "backgroundColor": even_row_color,
                },
                {
                    "if": {"row_index": "odd"},
                    "backgroundColor": odd_row_color,
                },
            ],
            style_data={"color": color, "backgroundColor": background_color},
            css=[
                dict(
                    selector="p",
                    rule="""
                        margin-bottom: 0;
                        padding-bottom: 15px;
                        padding-top: 15px;
                        padding-left: 5px;
                        padding-right: 5px;
                        text-align: left;
                    """,
                ),
                dict(
                    selector=".first-page, .previous-page, .next-page, .current-page, .current-page, .page-number, .last-page",
                    rule=f"background-color: {background_color}; color: {color} !important;",
                ),
                dict(
                    selector="input.current-page",
                    rule=f"background-color: {AmperePalette.PAGE_ACCENT_COLOR2}; border-bottom: 0 !important;",
                ),
                dict(
                    selector='.dash-table-container .dash-spreadsheet-container .dash-spreadsheet-inner .dash-header > div input[type="text"], .dash-table-container .dash-spreadsheet-container .dash-spreadsheet-inner .dash-filter > div input[type="text"]',
                    rule=f"color: {color} !important;",
                ),
            ],
            style_table={
                "height": "85vh",
                "maxHeight": "85vh",
                "overflowY": "scroll",
                "overflowX": "scroll",
                "margin": {"b": 100},
                "borderBottom": f"2px solid {color}",
                "borderTop": f"2px solid {color}",
                "borderLeft": f"1px solid {color}",
            },
        )
    )
    return dt_style


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
