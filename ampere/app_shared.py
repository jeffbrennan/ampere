from typing import Any

from flask_caching import Cache

from ampere.styling import ScreenWidth

# fixes circular import error when attempting to import cache from app.py
cache = Cache(
    config={
        "CACHE_TYPE": "simple",
        "CACHE_DEFAULT_TIMEOUT": 60,
    }
)


def update_tooltip(breakpoint_name: str) -> dict[Any, Any]:
    if breakpoint_name == ScreenWidth.xs:
        tooltip_font_size = "12px"
    else:
        tooltip_font_size = "16px"

    return {
        "placement": "bottom",
        "always_visible": True,
        "transform": "secondsToYMD",
        "style": {"fontSize": tooltip_font_size},
    }
