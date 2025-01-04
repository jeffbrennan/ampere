from flask_caching import Cache

# fixes circular import error when attempting to import cache from app.py
cache = Cache(
    config={
        "CACHE_TYPE": "simple",
        "CACHE_DEFAULT_TIMEOUT": 60,
    }
)
