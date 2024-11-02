from dataclasses import asdict, dataclass
from enum import StrEnum, auto
from typing import Any


class AmperePalette(StrEnum):
    PAGE_ACCENT_COLOR = "#304FFE"
    PAGE_LIGHT_GRAY = "#EEEEEE"
    BRAND_TEXT_COLOR = "#FFFFFF"
    BRAND_TEXT_COLOR_MUTED = "#e3e7fa"


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
    filter_action: str
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
