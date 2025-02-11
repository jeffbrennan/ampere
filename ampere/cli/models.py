from enum import StrEnum, auto

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
        return RepoEnum(value.lower())  # type: ignore
    except ValueError:
        valid_repos = [repo.name for repo in RepoEnum]  # type: ignore
        valid_repos_str = ""
        for i, repo in enumerate(valid_repos, 1):
            valid_repos_str += "\n" + f"{i}. {repo}"

        title = "Valid Repos"
        if with_downloads:
            title += " with Downloads"

        raise typer.BadParameter(f"\n{'='*20} {title} {'='*20}{valid_repos_str}")


def repo_callback_without_downloads(value: str | None, ctx: typer.Context) -> str | None:
    return repo_option_callback(value, ctx, with_downloads=False)


def repo_callback_with_downloads(value: str | None, ctx: typer.Context) -> str | None:
    return repo_option_callback(value, ctx, with_downloads=True)
