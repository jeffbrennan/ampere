from enum import StrEnum, auto
from functools import partial

import typer

from ampere.cli.common import CLIEnvironment
from ampere.models import create_repo_enum


class CLIOutputFormat(StrEnum):
    json = auto()
    table = auto()


def repo_option_callback(
    value: str | None, ctx: typer.Context, with_downloads: bool
) -> str | None:
    if value is None:
        return None
    env: CLIEnvironment = ctx.obj.get("env", CLIEnvironment.prod)
    RepoEnum = create_repo_enum(env, with_downloads)

    try:
        return RepoEnum(value)  # type: ignore
    except ValueError:
        raise typer.BadParameter(f"Invalid repo: {value}")


def repo_callback_without_downloads(value: str | None, ctx: typer.Context) -> str | None:
    return repo_option_callback(value, ctx, with_downloads=False)


def repo_callback_with_downloads(value: str | None, ctx: typer.Context) -> str | None:
    return repo_option_callback(value, ctx, with_downloads=True)
