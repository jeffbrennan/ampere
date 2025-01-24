from enum import StrEnum, auto
from typing import Annotated, Optional

import requests
import typer
from pydantic import BaseModel
from rich.console import Console
from rich.table import Table

from ampere.api.models import DownloadsPublic
from ampere.api.routes.downloads import (
    DownloadsGranularity,
    DownloadsPublicGroup,
    DownloadsSummaryGranularity,
    GetDownloadsPublicConfig,
    RepoEnum,
)
from ampere.cli.common import get_api_url
from ampere.cli.models import CLIOutputFormat
from ampere.cli.state import State
from ampere.common import timeit

console = Console()

downloads_app = typer.Typer()


class DownloadsSummary(BaseModel):
    granularity: DownloadsSummaryGranularity
    repo: RepoEnum  # type: ignore
    last_period: int
    this_period: int
    pct_change: float


def format_downloads_summary_output(
    records: list[DownloadsSummary],
    granularity: DownloadsSummaryGranularity,
    descending: bool,
) -> Table:
    period = granularity.name.capitalize().removesuffix("ly")
    table = Table("Repo", f"Last {period}", f"This {period}", "% Change")
    records = sorted(records, key=lambda x: x.pct_change, reverse=descending)
    for record in records:
        table.add_row(
            record.repo,
            f"{record.last_period:,}",
            f"{record.this_period:,}",
            str(round(record.pct_change, 2)),
        )

    return table


def format_downloads_list_output(response: DownloadsPublic) -> Table:
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


@timeit
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
    assert response.status_code == 200, print(response.json())
    return DownloadsPublic.model_validate(response.json())


@downloads_app.command("list")
@timeit
def list_downloads(
    granularity: Annotated[
        DownloadsGranularity,
        typer.Option(
            "--granularity",
            "-g",
            prompt=True,
        ),
    ],
    repo: Annotated[
        Optional[RepoEnum],  # type: ignore
        typer.Option("--repo", "-r", prompt=True),
    ],
    group: Annotated[
        DownloadsPublicGroup, typer.Option("--group", "-gr")
    ] = DownloadsPublicGroup.overall,
    n_days: Annotated[int, typer.Option("--n-days", "-n")] = 180,
    limit: Annotated[int, typer.Option("--limit", "-l")] = 30,
    descending: Annotated[bool, typer.Option("--desc/--asc", "-d/-a")] = True,
    output: Annotated[
        CLIOutputFormat, typer.Option("--output", "-o")
    ] = CLIOutputFormat.table,
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


def create_downloads_summary(
    records: list[DownloadsPublic], granularity: DownloadsSummaryGranularity
) -> list[DownloadsSummary]:
    summary = []
    for record in records:
        last_period = record.data[-2].download_count
        this_period = record.data[-1].download_count
        pct_change = (this_period - last_period) / last_period * 100
        summary.append(
            DownloadsSummary(
                granularity=granularity,
                repo=record.data[-1].repo,
                last_period=last_period,
                this_period=this_period,
                pct_change=pct_change,
            )
        )

    return summary


@downloads_app.command("summary")
@timeit
def summarize_downloads(
    granularity: Annotated[
        DownloadsSummaryGranularity,
        typer.Option(
            "--granularity",
            "-g",
            prompt=True,
        ),
    ],
    repo: Annotated[
        Optional[RepoEnum],  # type: ignore
        typer.Option("--repo", "-r"),
    ] = None,
    group: Annotated[
        DownloadsPublicGroup, typer.Option("--group", "-gr")
    ] = DownloadsPublicGroup.overall,
    descending: Annotated[bool, typer.Option("--desc/--asc", "-d/-a")] = True,
    output: Annotated[
        CLIOutputFormat, typer.Option("--output", "-o")
    ] = CLIOutputFormat.table,
):
    filters = {
        granularity.monthly: {"limit": 2, "n_days": 7 * 4 * 3},
        granularity.weekly: {"limit": 2, "n_days": 7 * 3},
    }

    if repo is None:
        repos = [i for i in RepoEnum]
    else:
        repos = [repo]

    all_records = []
    for repo in repos:
        records = get_downloads_response(
            GetDownloadsPublicConfig(
                granularity=granularity,
                repo=repo,
                group=group,
                n_days=filters[granularity]["n_days"],
                limit=filters[granularity]["limit"],
                descending=False,
            )
        )

        all_records.append(records)

    summary = create_downloads_summary(all_records, granularity)
    if output == CLIOutputFormat.json:
        for model in summary:
            console.print_json(model.model_dump_json())
        return

    table = format_downloads_summary_output(summary, granularity, descending)
    console.print(table)
