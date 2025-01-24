import datetime
import time
from concurrent.futures import ThreadPoolExecutor
from enum import StrEnum, auto
from typing import Annotated, Optional

import requests
import typer
from pydantic import BaseModel
from rich import box
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
from ampere.cli.common import CLIEnvironment, get_api_url, get_flag_emoji
from ampere.cli.models import CLIOutputFormat
from ampere.cli.state import State
from ampere.common import timeit

console = Console()

downloads_app = typer.Typer()


class DownloadsSummary(BaseModel):
    granularity: DownloadsSummaryGranularity
    repo: RepoEnum  # type: ignore
    min_date: datetime.datetime
    max_date: datetime.datetime
    last_period: int
    this_period: int
    pct_change: float


def format_downloads_summary_output(
    records: list[DownloadsSummary],
    granularity: DownloadsSummaryGranularity,
    descending: bool,
) -> Table:
    period = granularity.name.removesuffix("ly")
    min_date = min([record.min_date for record in records])
    max_date = max([record.max_date for record in records])

    title = f"Downloads Summary"
    subtitle = f"{min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}"
    title_full = f"{title}\n{subtitle}"

    table = Table(
        title=title_full, title_justify="left", title_style="bold", box=box.ROUNDED
    )
    table.add_column("repo", justify="left")
    table.add_column(f"last {period}", justify="right")
    table.add_column(f"this {period}", justify="right")
    table.add_column("% change", justify="right")

    records = sorted(records, key=lambda x: x.pct_change, reverse=descending)
    for record in records:
        table.add_row(
            record.repo,
            f"{record.last_period:,}",
            f"{record.this_period:,}",
            str(round(record.pct_change, 2)),
        )

    return table


def format_downloads_list_output(response: DownloadsPublic, descending: bool) -> Table:
    repo = response.data[0].repo
    group = response.data[0].group_name

    title = f"{repo} {group} downloads"
    table = Table(
        title=title,
        title_justify="left",
        title_style="bold",
        box=box.ROUNDED,
    )
    table.add_column("timestamp", justify="center")
    table.add_column("group value", justify="left")
    table.add_column("downloads", justify="right")
    table.add_column("%", justify="right")

    totals_by_timestamp = {}
    for item in response.data:
        if item.download_timestamp not in totals_by_timestamp:
            totals_by_timestamp[item.download_timestamp] = 0
        totals_by_timestamp[item.download_timestamp] += item.download_count

    prev_timestamp = response.data[0].download_timestamp
    for item in response.data:
        if item.download_timestamp != prev_timestamp:
            table.add_section()
        prev_timestamp = item.download_timestamp

        pct = item.download_count / totals_by_timestamp[item.download_timestamp] * 100

        if group == "country_code":
            group_emoji = get_flag_emoji(item.group_value)
            group_value = f"{group_emoji} {item.group_value}"
        else:
            group_value = item.group_value

        table.add_row(
            item.download_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            group_value,
            f"{item.download_count:,}",
            f"{pct:.2f}%",
        )
    return table


def get_downloads_response(config: GetDownloadsPublicConfig) -> DownloadsPublic:
    if State.env == CLIEnvironment.dev:
        time.sleep(0.4)

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

    table = format_downloads_list_output(response, descending)
    console.print(table)


def create_downloads_summary(
    records: list[DownloadsPublic], granularity: DownloadsSummaryGranularity
) -> list[DownloadsSummary]:
    summary = []
    for record in records:
        min_date = min([item.download_timestamp for item in record.data])
        max_date = max([item.download_timestamp for item in record.data])
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
                min_date=min_date,
                max_date=max_date,
            )
        )

    return summary


@downloads_app.command("summary")
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

    configs = []
    for repo in repos:
        configs.append(
            GetDownloadsPublicConfig(
                granularity=granularity,
                repo=repo,
                group=group,
                n_days=filters[granularity]["n_days"],
                limit=filters[granularity]["limit"],
                descending=False,
            )
        )

    with ThreadPoolExecutor(max_workers=len(repos)) as executor:
        all_records = list(executor.map(get_downloads_response, configs))

    summary = create_downloads_summary(all_records, granularity)
    if output == CLIOutputFormat.json:
        for model in summary:
            console.print_json(model.model_dump_json())
        return

    table = format_downloads_summary_output(summary, granularity, descending)
    console.print(table)
