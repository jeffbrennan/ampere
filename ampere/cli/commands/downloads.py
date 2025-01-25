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
    group: DownloadsPublicGroup
    repo: RepoEnum  # type: ignore
    group_value: str
    min_date: datetime.datetime
    max_date: datetime.datetime
    last_period: int
    this_period: int
    pct_change: float
    pct_total: float


def format_downloads_summary_output(
    records: list[DownloadsSummary], granularity: DownloadsSummaryGranularity
) -> Table:
    period = granularity.name.removesuffix("ly")
    min_date = min([record.min_date for record in records])
    max_date = max([record.max_date for record in records])

    group = records[0].group
    title = f"{group} downloads summary"
    subtitle = f"{min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}"
    title_full = f"{title}\n{subtitle}"

    table = Table(
        title=title_full, title_justify="left", title_style="bold", box=box.ROUNDED
    )
    table.add_column("repo", justify="left")
    table.add_column("group", justify="left")
    table.add_column(f"last {period}", justify="right")
    table.add_column(f"this {period}", justify="right")
    table.add_column("% change", justify="right")

    if group != DownloadsPublicGroup.overall:
        table.add_column("% total", justify="right")

    prev_repo = records[0].repo
    for record in records:
        if record.repo != prev_repo and group != DownloadsPublicGroup.overall:
            table.add_section()

        prev_repo = record.repo
        if record.group == "country_code" and len(record.group_value) == 2:
            group_emoji = get_flag_emoji(record.group_value)
            group_value = f"{group_emoji} {record.group_value}"
        else:
            group_value = record.group_value

        row_contents = [
            record.repo,
            group_value,
            f"{record.last_period:,}",
            f"{record.this_period:,}",
            str(round(record.pct_change, 2)),
        ]

        if group != DownloadsPublicGroup.overall:
            row_contents.append(str(round(record.pct_total, 2)))

        table.add_row(*row_contents)

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


@timeit
def create_downloads_summary(
    records: list[DownloadsPublic],
    group: DownloadsPublicGroup,
    granularity: DownloadsSummaryGranularity,
    descending: bool,
    min_pct_of_total: float,
) -> list[DownloadsSummary]:
    others: dict[RepoEnum, DownloadsSummary] = {}  # type: ignore
    summary = []

    for repo_data in records:
        group_val_lookup = {}
        repo = repo_data.data[0].repo
        for item in repo_data.data:
            if descending:
                if item.group_value not in group_val_lookup:
                    group_val_lookup[item.group_value] = {
                        "min_date": item.download_timestamp,
                        "max_date": item.download_timestamp,
                        "this_period": item.download_count,
                        "last_period": 0,
                    }
                else:
                    group_val_lookup[item.group_value].update(
                        {
                            "min_date": item.download_timestamp,
                            "last_period": item.download_count,
                        }
                    )
                continue

            if item.group_value not in group_val_lookup:
                group_val_lookup[item.group_value] = {
                    "min_date": item.download_timestamp,
                    "max_date": item.download_timestamp,
                    "this_period": 0,
                    "last_period": item.download_count,
                }
            else:
                group_val_lookup[item.group_value].update(
                    {
                        "max_date": item.download_timestamp,
                        "this_period": item.download_count,
                    }
                )

        total_this_period = sum([i["this_period"] for i in group_val_lookup.values()])
        for k, v in group_val_lookup.items():
            last_period = v["last_period"]
            this_period = v["this_period"]
            if last_period == 0:
                pct_change = 100
            else:
                pct_change = (this_period - last_period) / last_period * 100

            pct_total = this_period / total_this_period * 100

            record_parsed = DownloadsSummary(
                granularity=granularity,
                group=group,
                group_value=k,
                repo=repo,
                last_period=last_period,
                this_period=this_period,
                min_date=v["min_date"],
                max_date=v["max_date"],
                pct_change=pct_change,
                pct_total=pct_total,
            )
            if pct_total < min_pct_of_total:
                record_parsed.group_value = "other"
                if repo not in others:
                    others[repo] = record_parsed
                    continue
                others[repo].this_period += this_period
                others[repo].last_period += last_period
                others[repo].pct_total += pct_total

            else:
                summary.append(record_parsed)

    if len(others) > 0:
        for k, v in others.items():
            if v.last_period == 0:
                continue
            v.pct_change = (v.this_period - v.last_period) / v.last_period * 100
            summary.append(v)

    summary = sorted(summary, key=lambda x: (x.repo, x.this_period), reverse=descending)
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
    min_group_pct_of_total: Annotated[
        float,
        typer.Option(
            "--min-pct",
            "-m",
            help="""
            The minimum download count % of total in the most recent period for the specified repo and group.
            Group values with a % total lower than this threshold will be collapsed into an 'other' category.
            """,
        ),
    ] = 0.1,
    output: Annotated[
        CLIOutputFormat, typer.Option("--output", "-o")
    ] = CLIOutputFormat.table,
):
    filters = {
        granularity.monthly: {"limit": 10000, "n_days": 7 * 4 * 3},
        granularity.weekly: {"limit": 10000, "n_days": 7 * 3},
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
                descending=True,
            )
        )

    with ThreadPoolExecutor(max_workers=len(repos)) as executor:
        all_records = list(executor.map(get_downloads_response, configs))

    summary = create_downloads_summary(
        all_records,
        group,
        granularity,
        descending,
        min_group_pct_of_total,
    )
    if output == CLIOutputFormat.json:
        for model in summary:
            console.print_json(model.model_dump_json())
        return

    table = format_downloads_summary_output(summary, granularity)
    console.print(table)
