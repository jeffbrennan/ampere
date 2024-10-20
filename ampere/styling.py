from dataclasses import dataclass, asdict
from typing import Any
from enum import StrEnum


class AmperePalette(StrEnum):
    PAGE_ACCENT_COLOR = "#3F6DF9"
    PAGE_LIGHT_GRAY = "#EEEEEE"
    BRAND_TEXT_COLOR = "#FFFFFF"
    BRAND_TEXT_COLOR_MUTED = "#CEE5F2"


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
    style_header_conditional: list[dict[str, Any]]
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
        filter_options={"case": "insensitive"},
        page_size=100,
        style_header={
            "backgroundColor": "#3F6DF9",
            "padding": "10px",
            "color": "#FFFFFF",
            "fontWeight": "bold",
            "border": f"1px solid {AmperePalette.PAGE_ACCENT_COLOR}",
        },
        style_filter={
            "borderTop": "0",
            "borderBottom": "2px solid black",
            "borderLeft": "0",
            "borderRight": "0",
            "backgroundColor": AmperePalette.BRAND_TEXT_COLOR,
        },
        style_cell={
            "textAlign": "right",
            "minWidth": 95,
            "maxWidth": 95,
            "width": 95,
            "font_size": "1em",
            "whiteSpace": "normal",
            "height": "auto",
            "font-family": "sans-serif",
            "borderTop": "0",
            "borderBottom": "0",
            "borderLeft": "2px solid black",
            "borderRight": "2px solid black",
        },
        style_header_conditional=[{"if": {"column_id": "name"}, "textAlign": "center"}],
        style_data_conditional=[
            {"if": {"column_id": "name"}, "textAlign": "center"},
            {
                "if": {"row_index": "odd"},
                "backgroundColor": AmperePalette.PAGE_LIGHT_GRAY,
            },
        ],
        style_data={"color": "black", "backgroundColor": "white"},
        css=[dict(selector="p", rule="margin-bottom: 0; text-align: right;")],
        style_table={
            "height": "50%",
            "overflowY": "scroll",
            "overflowX": "scroll",
            "margin": {"b": 100},
        },
    )
)
