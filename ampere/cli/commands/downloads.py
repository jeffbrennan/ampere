import datetime
import time
from concurrent.futures import ThreadPoolExecutor
from itertools import repeat
from typing import Annotated

import requests
import typer
from pydantic import BaseModel
from rich import box
from rich.console import Console
from rich.table import Table

from ampere.cli.common import CLIEnvironment, get_api_url, get_flag_emoji, get_pct_change
from ampere.cli.models import CLIOutputFormat, repo_callback_with_downloads
from ampere.models import (
    DownloadsGranularity,
    DownloadsPublic,
    DownloadsPublicGroup,
    DownloadsSummaryGranularity,
    GetDownloadsPublicConfig,
    create_repo_enum,
)

console = Console()

downloads_app = typer.Typer()


class DownloadsSummary(BaseModel):
    granularity: DownloadsSummaryGranularity
    group: DownloadsPublicGroup
    repo: str
    group_value: str
    min_date: datetime.datetime
    max_date: datetime.datetime
    last_period: int
    this_period: int
    pct_change: float
    pct_total: float


class DownloadsSummaryOutput(BaseModel):
    records: dict[str, list[DownloadsSummary]]
    group: DownloadsPublicGroup
    min_date: datetime.datetime
    max_date: datetime.datetime
    grand_total_this_period: int
    grand_total_last_period: int
    grand_total_pct_change: float


def format_downloads_summary_output(
    config: DownloadsSummaryOutput,
    granularity: DownloadsSummaryGranularity,
    show_grand_total: bool,
) -> Table:
    period = granularity.name.removesuffix("ly")

    title = f"{config.group} downloads summary"
    subtitle = f"{config.min_date.strftime('%Y-%m-%d')} to {config.max_date.strftime('%Y-%m-%d')}"
    title_full = f"{title}\n{subtitle}"

    table = Table(
        title=title_full, title_justify="left", title_style="bold", box=box.ROUNDED
    )
    table.add_column("repo", justify="left")
    table.add_column("group", justify="left")
    table.add_column(f"last {period}", justify="right")
    table.add_column(f"this {period}", justify="right")
    table.add_column("% change", justify="right")

    if config.group != DownloadsPublicGroup.overall:
        table.add_column("% total", justify="right")

    for repo_records in config.records.values():
        if config.group != DownloadsPublicGroup.overall:
            table.add_section()
        for record in repo_records:
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
                f"{record.pct_change:.2f}%",
            ]
            style = "bold" if record.group_value == "subtotal" else None

            if (
                config.group != DownloadsPublicGroup.overall
                and record.group_value != "subtotal"
            ):
                row_contents.append(f"{record.pct_total:.2f}%")

            table.add_row(*row_contents, style=style)

    if show_grand_total:
        table.add_section()
        table.add_row(
            "total",
            "",
            f"{config.grand_total_last_period:,}",
            f"{config.grand_total_this_period:,}",
            f"{config.grand_total_pct_change:.2f}%",
            style="bold",
        )
    return table


def format_downloads_list_output(response: DownloadsPublic) -> Table:
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
    if group != DownloadsPublicGroup.overall:
        table.add_column("%", justify="right")

    totals_by_timestamp = {}
    for item in response.data:
        if item.download_timestamp not in totals_by_timestamp:
            totals_by_timestamp[item.download_timestamp] = 0
        totals_by_timestamp[item.download_timestamp] += item.download_count

    prev_timestamp = response.data[0].download_timestamp
    for item in response.data:
        if (
            item.download_timestamp != prev_timestamp
            and group != DownloadsPublicGroup.overall
        ):
            table.add_section()
        prev_timestamp = item.download_timestamp

        pct = item.download_count / totals_by_timestamp[item.download_timestamp] * 100

        if group == "country_code":
            group_emoji = get_flag_emoji(item.group_value)
            group_value = f"{group_emoji} {item.group_value}"
        else:
            group_value = item.group_value

        row_contents = [
            item.download_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            group_value,
            f"{item.download_count:,}",
        ]
        if group != DownloadsPublicGroup.overall:
            row_contents.append(str(round(pct, 2)))

        table.add_row(*row_contents)

    return table


def get_downloads_response(
    config: GetDownloadsPublicConfig, ctx: typer.Context
) -> DownloadsPublic:
    env = ctx.obj["env"]
    if env == CLIEnvironment.dev:
        time.sleep(0.4)

    base_url = get_api_url(env)
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
    ctx: typer.Context,
    granularity: Annotated[
        DownloadsGranularity,
        typer.Option(
            "--granularity",
            "-g",
            prompt=True,
        ),
    ],
    repo: Annotated[
        str,
        typer.Option("--repo", "-r", prompt=True, callback=repo_callback_with_downloads),
    ],
    group: Annotated[
        DownloadsPublicGroup, typer.Option("--group", "-gr")
    ] = DownloadsPublicGroup.overall,
    n_days: Annotated[int, typer.Option("--n-days", "-n")] = 180,
    limit: Annotated[int, typer.Option("--limit", "-l")] = 10_000,
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
        ),
        ctx=ctx,
    )
    if output == CLIOutputFormat.json:
        console.print_json(response.model_dump_json())
        return

    table = format_downloads_list_output(response)
    console.print(table)


def create_downloads_summary(
    records: list[DownloadsPublic],
    group: DownloadsPublicGroup,
    granularity: DownloadsSummaryGranularity,
    descending: bool,
    min_pct_of_total: float,
    show_subtotal: bool,
    ctx: typer.Context,
) -> DownloadsSummaryOutput:
    env = ctx.obj["env"]
    others: dict[create_repo_enum(env, True), DownloadsSummary] = {}  # type: ignore
    repo_summaries: dict[create_repo_enum(env, True), list[DownloadsSummary]] = {}  # type: ignore

    grand_total_this_period = 0
    grand_total_last_period = 0
    min_dates = []
    max_dates = []

    for repo_data in records:
        summary = []
        group_val_lookup = {}
        repo = repo_data.data[0].repo
        for item in repo_data.data:
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

        total_this_period = 0
        total_last_period = 0
        total_min_date = None
        total_max_date = None
        for val in group_val_lookup.values():
            total_this_period += val["this_period"]
            total_last_period += val["last_period"]

            if total_min_date is None or val["min_date"] < total_min_date:
                total_min_date = val["min_date"]
            if total_max_date is None or val["max_date"] > total_max_date:
                total_max_date = val["max_date"]

        assert all([total_min_date, total_max_date])
        grand_total_last_period += total_last_period
        grand_total_this_period += total_this_period
        min_dates.append(total_min_date)
        max_dates.append(total_max_date)

        subtotal_record = DownloadsSummary(
            granularity=granularity,
            group=group,
            group_value="subtotal",
            repo=repo,
            last_period=total_last_period,
            this_period=total_this_period,
            min_date=min([i["min_date"] for i in group_val_lookup.values()]),
            max_date=max([i["max_date"] for i in group_val_lookup.values()]),
            pct_change=get_pct_change(total_last_period, total_this_period),
            pct_total=100,
        )

        for k, v in group_val_lookup.items():
            last_period = v["last_period"]
            this_period = v["this_period"]

            pct_change = get_pct_change(last_period, this_period)
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

            else:
                summary.append(record_parsed)

        if descending:
            summary.insert(0, subtotal_record)
        else:
            summary.append(subtotal_record)

        repo_summaries[repo] = summary

    for k, v in others.items():
        pct_change = get_pct_change(v.last_period, v.this_period)
        v.pct_change = pct_change

    summary_totals = []
    for k, v in repo_summaries.items():
        subtotal_index = 0 if descending else -1
        summary_totals.append((k, v[subtotal_index].this_period))

        if not show_subtotal:
            _ = v.pop(subtotal_index)

        # final group level sort to account for new "other" category
        if k in others:
            v.append(others[k])
            v.sort(key=lambda x: x.this_period, reverse=descending)

    # top level repo level sort by total downloads
    summary_totals.sort(key=lambda x: x[1], reverse=descending)

    sorted_repo_summaries = {}
    for i in summary_totals:
        sorted_repo_summaries[i[0]] = repo_summaries[i[0]]

    return DownloadsSummaryOutput(
        records=sorted_repo_summaries,
        group=group,
        min_date=min(min_dates),
        max_date=max(max_dates),
        grand_total_last_period=grand_total_last_period,
        grand_total_this_period=grand_total_this_period,
        grand_total_pct_change=get_pct_change(
            grand_total_last_period, grand_total_this_period
        ),
    )


@downloads_app.command("summary")
def summarize_downloads(
    ctx: typer.Context,
    granularity: Annotated[
        DownloadsSummaryGranularity,
        typer.Option(
            "--granularity",
            "-g",
            prompt=True,
        ),
    ],
    repo: Annotated[
        str | None,
        typer.Option("--repo", "-r", callback=repo_callback_with_downloads),
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
    show_subtotal: Annotated[bool, typer.Option("--show-subtotal", "-st")] = False,
    show_grand_total: Annotated[bool, typer.Option("--show-grand-total", "-gt")] = False,
    output: Annotated[
        CLIOutputFormat, typer.Option("--output", "-o")
    ] = CLIOutputFormat.table,
):
    filters = {
        granularity.monthly: {"limit": 10000, "n_days": 7 * 4 * 3},
        granularity.weekly: {"limit": 10000, "n_days": 7 * 3},
    }

    env = ctx.obj["env"]
    if repo is None:
        repos = [i for i in create_repo_enum(env, True)]
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
                descending=True,  # get the most recent data regardless of the sort order
            )
        )

    with ThreadPoolExecutor(max_workers=len(repos)) as executor:
        all_records = list(executor.map(get_downloads_response, configs, repeat(ctx)))

    summary: DownloadsSummaryOutput = create_downloads_summary(
        records=all_records,
        group=group,
        granularity=granularity,
        descending=descending,
        min_pct_of_total=min_group_pct_of_total,
        show_subtotal=show_subtotal,
        ctx=ctx,
    )
    if output == CLIOutputFormat.json:
        for _, record_list in summary.records.items():
            for record in record_list:
                console.print_json(record.model_dump_json())
        return

    table = format_downloads_summary_output(
        config=summary, granularity=granularity, show_grand_total=show_grand_total
    )
    console.print(table)
