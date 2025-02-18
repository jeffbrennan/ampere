import copy
import datetime
from dataclasses import dataclass
from enum import StrEnum, auto
from typing import Annotated

import requests
import typer
from pydantic import BaseModel
from rich import box
from rich.console import Console
from rich.table import Table

from ampere.cli.common import get_api_url
from ampere.cli.models import CLIOutputFormat, repo_callback_without_downloads
from ampere.models import FeedPublic, FeedPublicAction, FeedPublicEvent

console = Console()
feed_app = typer.Typer()


@dataclass
class ParsedDates:
    date_format: str
    dates: list[datetime.datetime]
    trunc_dates: list[datetime.datetime]
    formatted_dates: list[str]


class FeedSummary(BaseModel):
    date: str
    stars: str
    forks: str
    issues: str
    prs: str
    commits: str
    total: str


class FeedGranularity(StrEnum):
    daily = auto()
    weekly = auto()
    monthly = auto()


class FeedSummaryOutput(BaseModel):
    records: list[FeedSummary]
    min_date: datetime.datetime
    max_date: datetime.datetime
    grand_total: FeedSummary
    granularity: FeedGranularity
    repo: str | None
    event: FeedPublicEvent | None
    action: FeedPublicAction | None
    username: str | None
    n_periods: int | None


class FeedSummaryConfig(BaseModel):
    granularity: FeedGranularity
    repo: str | None
    event: FeedPublicEvent | None
    action: FeedPublicAction | None
    username: str | None
    n_periods: int | None
    descending: bool


def date_trunc(dt: datetime.datetime, granularity: FeedGranularity) -> datetime.datetime:
    if granularity == granularity.daily:
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    elif granularity == granularity.weekly:
        start_of_week = dt - datetime.timedelta(days=dt.weekday())
        return start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
    elif granularity == granularity.monthly:
        return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def get_feed_list_response(
    repo: str | None,
    event: FeedPublicEvent | None,
    action: FeedPublicAction | None,
    username: str | None,
    n_days: int | None,
    min_date: str | None,
    max_date: str | None,
    limit: int,
    descending: bool,
    ctx: typer.Context,
) -> FeedPublic:
    env = ctx.obj["env"]
    base_url = get_api_url(env)

    url = f"{base_url}/feed/list"
    username = username.lower() if username is not None else None
    params = {
        "repo": repo,
        "event": event,
        "action": action,
        "username": username,
        "n_days": n_days,
        "min_date": min_date,
        "max_date": max_date,
        "limit": limit,
        "descending": descending,
    }
    response = requests.get(url, params=params)
    assert response.status_code == 200, response.json()
    model = FeedPublic.model_validate(response.json())
    return model


def format_feed_output(model: FeedPublic) -> Table:
    title = "Feed Events"
    table = Table(title=title, title_justify="left", title_style="bold", box=box.ROUNDED)

    cols = ["timestamp", "repo", "user", "event", "action", "data", "link"]
    for col in cols:
        table.add_column(col)

    max_data_len = 25
    for item in model.data:
        if item.event_data is None:
            event_data = ""
        elif len(item.event_data) > max_data_len:
            event_data = item.event_data[0:max_data_len] + "..."
        else:
            event_data = item.event_data

        table.add_row(
            item.event_timestamp.strftime("%Y-%m-%dT%H:%M:%S"),
            item.repo_name,
            item.user_name,
            item.event_type,
            item.event_action,
            event_data,
            item.event_link,
        )
        table.add_section()

    return table


def format_feed_summary(model: FeedSummaryOutput) -> Table:
    title = "Feed Summary"
    subtitle = (
        f"{model.min_date.strftime('%Y-%m-%d')} to {model.max_date.strftime('%Y-%m-%d')}"
    )
    title_full = f"{title}\n{subtitle}"
    table = Table(
        title=title_full,
        title_justify="left",
        title_style="bold",
        box=box.ROUNDED,
    )

    date_col_lookup = {
        FeedGranularity.daily: "date",
        FeedGranularity.weekly: "week",
        FeedGranularity.monthly: "month",
    }

    cols = [
        date_col_lookup[model.granularity],
        "stars",
        "forks",
        "issues",
        "prs",
        "commits",
    ]

    for col in cols:
        table.add_column(col)

    table.add_column("total", justify="right")

    prev_date_divider = model.records[0].date.split("-")[-2]
    for i, record in enumerate(model.records):
        date_divider = record.date.split("-")[-2]
        if prev_date_divider != date_divider:
            table.add_section()
        prev_date_divider = date_divider

        table.add_row(
            record.date,
            record.stars,
            record.forks,
            record.issues,
            record.prs,
            record.commits,
            record.total,
        )

    table.add_section()
    table.add_row(
        model.grand_total.date,
        model.grand_total.stars,
        model.grand_total.forks,
        model.grand_total.issues,
        model.grand_total.prs,
        model.grand_total.commits,
        model.grand_total.total,
    )

    return table


def parse_summary_dates(
    model: FeedPublic,
    granularity: FeedGranularity,
    descending: bool,
    n_periods: int | None,
) -> ParsedDates:
    dates = sorted(set(i.event_timestamp for i in model.data))
    max_date = max(dates)
    min_date = min(dates)
    n_days_lookup = {
        FeedGranularity.daily: 1,
        FeedGranularity.weekly: 7,
        FeedGranularity.monthly: 30,
    }
    date_format = "%Y-%m" if granularity == FeedGranularity.monthly else "%Y-%m-%d"

    if n_periods is None:
        min_date = date_trunc(min_date, granularity)
        max_date = date_trunc(max_date, granularity)
        trunc_dates = [
            max_date - datetime.timedelta(days=n_days_lookup[granularity] * i)
            for i in range(1, 1 + (max_date - min_date).days)
        ]

        return ParsedDates(
            date_format=date_format,
            dates=dates,
            trunc_dates=trunc_dates,
            formatted_dates=[i.strftime(date_format) for i in trunc_dates],
        )

    trunc_dates = []
    for i in range(n_periods):
        expected_date = max_date - datetime.timedelta(days=n_days_lookup[granularity] * i)
        trunc_dates.append(date_trunc(expected_date, granularity))

    min_trunc_date = min(trunc_dates)
    trunc_dates = sorted(trunc_dates, reverse=descending)
    formatted_dates = [i.strftime(date_format) for i in trunc_dates]
    dates = [i for i in dates if i >= min_trunc_date]
    if min_trunc_date not in dates:
        dates.insert(0, min_trunc_date)

    return ParsedDates(
        date_format=date_format,
        dates=dates,
        trunc_dates=trunc_dates,
        formatted_dates=formatted_dates,
    )


def create_feed_summary(
    model: FeedPublic,
    config: FeedSummaryConfig,
) -> FeedSummaryOutput:
    counts = {
        "star": 0,
        "fork": 0,
        "issue": {
            "created": 0,
            "updated": 0,
            "closed": 0,
        },
        "pull request": {
            "created": 0,
            "updated": 0,
            "merged": 0,
            "closed": 0,
        },
        "commit": 0,
        "total": 0,
    }

    parsed_dates = parse_summary_dates(
        model,
        config.granularity,
        config.descending,
        config.n_periods,
    )

    counts_by_date = {
        date: copy.deepcopy(counts) for date in parsed_dates.formatted_dates
    }
    counts_by_date["grand_total"] = counts.copy()
    for record in model.data:
        record_date = date_trunc(record.event_timestamp, config.granularity).strftime(
            parsed_dates.date_format
        )
        if record_date not in counts_by_date:
            continue

        if record.event_type in ["star", "fork", "commit"]:
            counts_by_date[record_date][record.event_type] += 1
            counts_by_date[record_date]["total"] += 1

            counts_by_date["grand_total"][record.event_type] += 1
            counts_by_date["grand_total"]["total"] += 1
            continue

        counts_by_date[record_date][record.event_type][record.event_action] += 1
        counts_by_date[record_date]["total"] += 1

        counts_by_date["grand_total"][record.event_type][record.event_action] += 1
        counts_by_date["grand_total"]["total"] += 1

    summary_records = []
    grand_total_records = []
    for k, v in counts_by_date.items():
        pr = v["pull request"]
        prs = f"+{pr['created']} ~{pr['updated'] + pr['merged']} -{pr['closed']}"

        issues = (
            f"+{v['issue']['created']} ~{v['issue']['updated']} -{v['issue']['closed']}"
        )

        if k == "grand_total":
            date = "GRAND TOTAL"
        else:
            date = k

        summary = FeedSummary(
            date=date,
            stars=f'+{v["star"]}',
            forks=f'+{v["fork"]}',
            issues=issues,
            prs=prs,
            commits=f'+{v["commit"]}',
            total=f"{v['total']:,}",
        )

        if k == "grand_total":
            grand_total_records.append(summary)
        else:
            summary_records.append(summary)

    return FeedSummaryOutput(
        records=summary_records,
        min_date=min(parsed_dates.dates),
        max_date=max(parsed_dates.dates),
        grand_total=grand_total_records[0],
        repo=config.repo,
        event=config.event,
        action=config.action,
        username=config.username,
        n_periods=config.n_periods,
        granularity=config.granularity,
    )


@feed_app.command("list")
def list_feed(
    ctx: typer.Context,
    repo: Annotated[
        str | None, typer.Option("--repo", "-r", callback=repo_callback_without_downloads)
    ] = None,
    event: Annotated[FeedPublicEvent | None, typer.Option("--event", "-e")] = None,
    action: Annotated[FeedPublicAction | None, typer.Option("--action", "-a")] = None,
    username: Annotated[str | None, typer.Option("--user", "-u")] = None,
    n_days: Annotated[int | None, typer.Option("--n-days", "-n")] = None,
    min_date: Annotated[str | None, typer.Option("--min-date", "-mind")] = None,
    max_date: Annotated[str | None, typer.Option("--max-date", "-maxd")] = None,
    limit: Annotated[int, typer.Option("--limit", "-l")] = 50,
    descending: Annotated[bool, typer.Option("--desc/--asc", "-d/-a")] = True,
    output: Annotated[
        CLIOutputFormat, typer.Option("--output", "-o")
    ] = CLIOutputFormat.table,
) -> None:
    # remove default limit if min_date is set
    if min_date is not None and limit == 50:
        limit = 10_000

    model = get_feed_list_response(
        repo,
        event,
        action,
        username,
        n_days,
        min_date,
        max_date,
        limit,
        descending,
        ctx,
    )
    if len(model.data) == 0:
        console.print("No feed events found.")
        return

    if output == CLIOutputFormat.json:
        console.print_json(model.model_dump_json())
        return

    table = format_feed_output(model)
    console.print(table)


@feed_app.command("summary")
def summarize_feed(
    ctx: typer.Context,
    granularity: Annotated[
        FeedGranularity,
        typer.Option(
            "--granularity",
            "-g",
            prompt=True,
        ),
    ] = FeedGranularity.daily,
    repo: Annotated[
        str | None, typer.Option("--repo", "-r", callback=repo_callback_without_downloads)
    ] = None,
    event: Annotated[FeedPublicEvent | None, typer.Option("--event", "-e")] = None,
    action: Annotated[FeedPublicAction | None, typer.Option("--action", "-a")] = None,
    username: Annotated[str | None, typer.Option("--user", "-u")] = None,
    n_periods: Annotated[int | None, typer.Option("--n-periods", "-n")] = 30,
    min_date: Annotated[str | None, typer.Option("--min-date", "-mind")] = None,
    max_date: Annotated[str | None, typer.Option("--max-date", "-maxd")] = None,
    descending: Annotated[bool, typer.Option("--desc/--asc", "-d/-a")] = True,
    output: Annotated[
        CLIOutputFormat, typer.Option("--output", "-o")
    ] = CLIOutputFormat.table,
) -> None:
    day_lookup = {
        FeedGranularity.daily: 1,
        FeedGranularity.weekly: 7,
        FeedGranularity.monthly: 30,
    }
    if n_periods is None:
        n_days = None
    else:
        n_days = day_lookup[granularity] * n_periods

    model = get_feed_list_response(
        repo,
        event,
        action,
        username,
        n_days,
        min_date,
        max_date,
        10_000,
        descending,
        ctx,
    )
    if len(model.data) == 0:
        console.print("No feed events found.")
        return

    summary = create_feed_summary(model, FeedSummaryConfig(**locals()))

    if output == CLIOutputFormat.json:
        console.print_json(summary.model_dump_json())
        return

    table = format_feed_summary(summary)
    console.print(table)
