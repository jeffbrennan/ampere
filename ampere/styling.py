from dataclasses import asdict, dataclass
from enum import StrEnum, auto
from typing import Any

import pandas as pd

from ampere.common import timeit


class AmperePalette(StrEnum):
    PAGE_ACCENT_COLOR = "rgb(247, 111, 83)"
    PAGE_BACKGROUND_COLOR_LIGHT = "rgb(242, 240, 227)"
    TABLE_EVEN_ROW_COLOR_LIGHT = "rgb(242, 240, 227)"
    TABLE_ODD_ROW_COLOR_LIGHT = "rgb(239, 230, 210)"
    BRAND_TEXT_COLOR_LIGHT = "rgb(33, 33, 33)"

    PAGE_BACKGROUND_COLOR_DARK = "rgb(33, 33, 33)"
    TABLE_EVEN_ROW_COLOR_DARK = "rgb(33, 33, 33)"
    TABLE_ODD_ROW_COLOR_DARK = "rgb(50, 50, 50)"
    BRAND_TEXT_COLOR_DARK = "rgb(242, 240, 227)"


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


def adjust_rgb_for_dark_mode(rgb: str) -> str:
    rgb_vals = rgb.split("rgb(")[1].split(")")[0].split(",")
    adjusted_rgb = [str(255 - int(i)) for i in rgb_vals]
    return f'rgb({",".join(adjusted_rgb)})'


def generate_heatmap_palette(dark_mode: bool, palette_name: str) -> list[str]:
    if palette_name not in ["oranges", "greens"]:
        raise ValueError("Invalid palette name")

    palettes = {
        "oranges": {
            "light": ["#FFC6BE", "#FF9182", "#FF4B33"],
            "dark": ["#660C00", "#881000", "#CC1800"],
        },
        "greens": {
            "light": [
                "#C8F6B2",
                "#8DEC60",
                "#57D61A",
            ],
            "dark": ["#112A05", "#22530A", "#337D0F"],
        },
    }

    return palettes[palette_name]["dark" if dark_mode else "light"]


@timeit
def style_dt_background_colors_by_rank(
    df: pd.DataFrame, n_bins: int, cols: list[ColumnInfo], dark_mode: bool
) -> list[dict]:
    # https://dash.plotly.com/datatable/conditional-formatting
    styles = []
    rows_to_update = 3
    if dark_mode:
        text_color = "white"
    else:
        text_color = "black"

    for col in cols:
        colors = generate_heatmap_palette(dark_mode, col.palette)
        ranks = (
            (df[col.name].rank(ascending=col.ascending, method="max").astype("int") - 1)
            .squeeze()
            .tolist()  # type: ignore
        )
        max_ranks = sorted(set(ranks))[-rows_to_update:]
        for row in range(n_bins):
            row_val = df[col.name].iloc[row]
            rank_val = ranks[row]
            if rank_val not in max_ranks:
                continue

            rank_val_idx = max_ranks.index(rank_val)
            styles.append(
                {
                    "if": {
                        "filter_query": f"{{{col.name}}} = {row_val}",
                        "column_id": col.name,
                    },
                    "backgroundColor": colors[rank_val_idx],
                    "color": text_color,
                    "borderLeft": f"2px solid {text_color}",
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
                # "backgroundColor": AmperePalette.PAGE_ACCENT_COLOR2,
                # "color": AmperePalette.BRAND_TEXT_COLOR,
                "paddingRight": "12px",
                "margin": "0",
                "fontWeight": "bold",
                "borderLeft": "none",
                # "borderLeft": f"2px solid {color}",
                "borderRight": f"2px solid {color}",
            },
            style_filter={
                "backgroundColor": background_color,
                "borderTop": "0",
                "borderBottom": f"2px solid {color}",
                "borderLeft": "none",
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
                "borderLeft": "none",
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
                    rule=f"background-color: {AmperePalette.PAGE_ACCENT_COLOR}; border-bottom: 0 !important;",
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
                "border": "none",
                "borderBottom": f"2px solid {color}",
                "borderTop": f"2px solid {color}",
                "borderLeft": f"2px solid {color}",
            },
        )
    )
    return dt_style


table_title_style = {
    # "color": AmperePalette.BRAND_TEXT_COLOR,
    "backgroundColor": AmperePalette.PAGE_ACCENT_COLOR,
    "paddingBottom": "0",
    "paddingLeft": "10px",
    "paddingRight": "10px",
    "fontSize": "1.5rem",
    "fontWeight": "bold",
    "marginBottom": "0",
    "border": "none",
}
