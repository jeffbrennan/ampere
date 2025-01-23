from typing import Annotated
import requests
import typer
from rich.console import Console
from rich.table import Table

from ampere.api.models import DownloadsPublic
from ampere.api.routes.downloads import (
    DownloadsGranularity,
    DownloadsPublicGroup,
    GetDownloadsPublicConfig,
    RepoEnum,
)
from ampere.cli.common import get_api_url
from ampere.cli.models import CLIOutputFormat
from ampere.cli.state import State
from ampere.common import timeit

console = Console()

downloads_app = typer.Typer()


def format_downloads_list_output(response: DownloadsPublic):
    table = Table("Repo", "Timestamp", "Group Name", "Group Value", "Download Count")
    for item in response.data:
        table.add_row(
            item.repo,
            item.download_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            item.group_name,
            item.group_value,
            str(item.download_count),
        )

    return table


def get_downloads_response(config: GetDownloadsPublicConfig) -> DownloadsPublic:
    base_url = get_api_url(State.env)
    url = f"{base_url}/downloads/{config.granularity}"
    response = requests.get(
        url,
        params={
            "repo": config.repo,
            "group": config.group,
            "n_days": config.n_days,
            "limit": config.limit,
            "descending": config.descending,
        },
    )
    return DownloadsPublic.model_validate(response.json())


@downloads_app.command("list")
@timeit
def list_downloads(
    granularity: Annotated[DownloadsGranularity, typer.Option(prompt=True)],
    repo: Annotated[RepoEnum, typer.Option(prompt=True)],  # type: ignore
    group: DownloadsPublicGroup = DownloadsPublicGroup.overall,
    n_days: int = 30,
    limit: int = 50,
    descending: bool = True,
    output: CLIOutputFormat = CLIOutputFormat.table,
):
    response = get_downloads_response(
        GetDownloadsPublicConfig(
            granularity=granularity,
            repo=repo,
            group=group,
            n_days=n_days,
            limit=limit,
            descending=descending,
        )
    )
    if output == CLIOutputFormat.json:
        console.print_json(response.model_dump_json())
        return

    table = format_downloads_list_output(response)
    console.print(table)


@downloads_app.command("summary")
def summarize_downloads():
    print("Summary of downloads")
