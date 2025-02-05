from dataclasses import asdict, dataclass
from enum import StrEnum, auto
from typing import Any

import pandas as pd

from ampere.common import timeit


class AmperePalette(StrEnum):
    PAGE_BACKGROUND_COLOR_LIGHT = "rgb(242, 240, 227)"
    BRAND_TEXT_COLOR_LIGHT = "rgb(33, 33, 33)"
    PAGE_BACKGROUND_COLOR_DARK = "rgb(33, 33, 33)"
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
    _, color = get_ampere_colors(dark_mode, contrast=False)

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
                    "color": color,
                    "borderLeft": f"2px solid {color}",
                }
            )

    return styles


def get_ampere_dt_style(dark_mode: bool = False) -> dict:
    background, color = get_ampere_colors(dark_mode, contrast=False)
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
            style_header={},
            style_filter={},
            style_cell={
                "textAlign": "center",
                "minWidth": 100,
                "maxWidth": 170,
                "whiteSpace": "normal",
                "wordBreak": "break-word",
            },
            style_cell_conditional=[],
            style_data_conditional=[],
            style_data={
                "color": color,
                "backgroundColor": background,
            },
            css=[],
            style_table={
                "height": "85vh",
                "maxHeight": "85vh",
                "overflowY": "scroll",
                "overflowX": "scroll",
            },
        )
    )
    return dt_style


def get_ampere_colors(dark_mode: bool, contrast: bool) -> tuple[str, str]:
    colors = {
        "dark": {
            "background": AmperePalette.PAGE_BACKGROUND_COLOR_DARK,
            "text": AmperePalette.BRAND_TEXT_COLOR_DARK,
        },
        "light": {
            "background": AmperePalette.PAGE_BACKGROUND_COLOR_LIGHT,
            "text": AmperePalette.BRAND_TEXT_COLOR_LIGHT,
        },
    }

    # invert the selector if contrast is True
    selector = not dark_mode if contrast else dark_mode
    selector_str = "dark" if selector else "light"

    return colors[selector_str]["background"], colors[selector_str]["text"]


def get_table_title_style(dark_mode: bool) -> dict:
    background, color = get_ampere_colors(dark_mode, contrast=True)

    return {
        "color": color,
        "backgroundColor": background,
        "paddingBottom": "0",
        "paddingLeft": "10px",
        "paddingRight": "10px",
        "fontSize": "1.5rem",
        "fontWeight": "bold",
        "marginBottom": "0",
        "border": "none",
    }
