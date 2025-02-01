from typing import Any

from flask_caching import Cache

from ampere.styling import ScreenWidth, get_ampere_colors

# fixes circular import error when attempting to import cache from app.py
cache = Cache(
    config={
        "CACHE_TYPE": "simple",
        "CACHE_DEFAULT_TIMEOUT": 60,
    }
)


def update_tooltip(
    min_date_seconds: int,
    max_date_seconds: int,
    date_range: list[int],
    breakpoint_name: str,
    dark_mode: bool,
) -> dict[Any, Any]:
    always_visible = (
        date_range[0] == min_date_seconds and date_range[1] == max_date_seconds
    )
    if breakpoint_name == ScreenWidth.xs:
        tooltip_font_size = "12px"
    else:
        tooltip_font_size = "16px"

    background, color = get_ampere_colors(dark_mode, False)

    return {
        "placement": "bottom",
        "always_visible": always_visible,
        "transform": "secondsToYMD",
        "style": {
            "background": background,
            "color": color,
            "fontSize": tooltip_font_size,
            "paddingLeft": "4px",
            "paddingRight": "4px",
            "borderRadius": "10px",
        },
    }
