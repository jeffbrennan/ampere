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
