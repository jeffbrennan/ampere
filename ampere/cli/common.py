from enum import StrEnum, auto


class CLIEnvironment(StrEnum):
    prod = auto()
    dev = auto()


def get_api_url(env: CLIEnvironment) -> str:
    url_lookup = {
        CLIEnvironment.prod: "https://api-ampere.jeffbrennan.dev",
        CLIEnvironment.dev: "http://127.0.0.1:8000",
    }
    return url_lookup[env]


def get_flag_emoji(country_code: str) -> str:
    """Converts a two-letter country code to a flag emoji."""
    code_points = [ord(c) + 127397 for c in country_code.upper()]
    return "".join(chr(i) for i in code_points)


def get_pct_change(last_period: int, this_period: int) -> float:
    if last_period == 0:
        return 100.0
    return (this_period - last_period) / last_period * 100
