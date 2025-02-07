import copy
import datetime
from enum import StrEnum, auto
from typing import Annotated

import requests
import typer
from pydantic import BaseModel
from rich import box
from rich.console import Console
from rich.table import Table

from ampere.cli.common import get_api_url
from ampere.cli.models import CLIOutputFormat
from ampere.cli.state import State
from ampere.common import timeit
from ampere.models import FeedPublic, FeedPublicAction, FeedPublicEvent, create_repo_enum

console = Console()
feed_app = typer.Typer()

RepoEnum = create_repo_enum(State.env)


class FeedSummary(BaseModel):
    date: str
    stars: str
    forks: str
    issues: str
    prs: str
    commits: str


class FeedSummaryOutput(BaseModel):
    records: list[FeedSummary]
    min_date: datetime.datetime
    max_date: datetime.datetime
    grand_total: FeedSummary


class FeedGranularity(StrEnum):
    daily = auto()
    weekly = auto()
    monthly = auto()


def date_trunc(dt: datetime.datetime, granularity: FeedGranularity) -> datetime.datetime:
    if granularity == granularity.daily:
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    elif granularity == granularity.weekly:
        start_of_week = dt - datetime.timedelta(days=dt.weekday())
        return start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
    elif granularity == granularity.monthly:
        return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


@timeit
def get_feed_list_response(
    repo: RepoEnum | None,
    event: FeedPublicEvent,
    action: FeedPublicAction,
    n_days: int,
    limit: int,
    descending: bool,
) -> FeedPublic:
    base_url = get_api_url(State.env)
    url = f"{base_url}/feed/list"
    params = {
        "repo": repo,
        "event": event,
        "action": action,
        "n_days": n_days,
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


@timeit
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
    cols = ["date", "stars", "forks", "issues", "prs", "commits"]
    for col in cols:
        table.add_column(col)

    for record in model.records:
        table.add_row(
            record.date,
            record.stars,
            record.forks,
            record.issues,
            record.prs,
            record.commits,
        )

    table.add_section()
    table.add_row(
        model.grand_total.date,
        model.grand_total.stars,
        model.grand_total.forks,
        model.grand_total.issues,
        model.grand_total.prs,
        model.grand_total.commits,
    )

    return table


@timeit
def create_feed_summary(
    model: FeedPublic,
    granularity: FeedGranularity,
    descending: bool,
    n_periods: int,
) -> FeedSummaryOutput:
    dates = set(i.event_timestamp for i in model.data)
    max_date = max(dates)
    min_date = min(dates)
    n_days_lookup = {
        FeedGranularity.daily: 1,
        FeedGranularity.weekly: 7,
        FeedGranularity.monthly: 30,
    }

    expected_min_date = max_date - datetime.timedelta(
        days=n_days_lookup[granularity] * n_periods
    )

    trunc_dates = []
    for i in range(n_periods):
        expected_date = max_date - datetime.timedelta(days=n_days_lookup[granularity] * i)
        if expected_date < min_date:
            print(f"""
            expected min date: {expected_min_date.strftime('%Y-%m-%d')}
            stopping at: {min_date.strftime('%Y-%m-%d')}
            increase `n_days` or decrease `n_periods`.
            """)
            break

        trunc_dates.append(date_trunc(expected_date, granularity))

    assert min(trunc_dates) >= date_trunc(
        min_date, granularity
    ), f"specified min date {min(trunc_dates)} older than earliest record {min_date}"

    assert max(trunc_dates) <= date_trunc(
        max_date, granularity
    ), f"specified max date {max(trunc_dates)} newer than latest record {max_date}"

    trunc_dates = sorted(trunc_dates, reverse=descending)
    formatted_dates = [i.strftime("%Y-%m-%d") for i in trunc_dates]

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
    }
    counts_by_date = {date: copy.deepcopy(counts) for date in formatted_dates}
    counts_by_date["grand_total"] = counts.copy()
    for record in model.data:
        record_date = date_trunc(record.event_timestamp, granularity).strftime("%Y-%m-%d")
        if record_date not in counts_by_date:
            continue

        if record.event_type in ["star", "fork", "commit"]:
            counts_by_date[record_date][record.event_type] += 1
            counts_by_date["grand_total"][record.event_type] += 1
            continue

        counts_by_date[record_date][record.event_type][record.event_action] += 1
        counts_by_date["grand_total"][record.event_type][record.event_action] += 1

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
        )

        if k == "grand_total":
            grand_total_records.append(summary)
        else:
            summary_records.append(summary)

    return FeedSummaryOutput(
        records=summary_records,
        min_date=min(dates),
        max_date=max(dates),
        grand_total=grand_total_records[0],
    )


@feed_app.command("summary")
@timeit
def summarize_feed(
    granularity: Annotated[
        FeedGranularity,
        typer.Option(
            "--granularity",
            "-g",
            prompt=True,
        ),
    ] = FeedGranularity.daily,
    repo: Annotated[RepoEnum | None, typer.Option("--repo", "-r")] = None,
    event: Annotated[FeedPublicEvent | None, typer.Option("--event", "-e")] = None,
    action: Annotated[FeedPublicAction | None, typer.Option("--action", "-a")] = None,
    n_periods: Annotated[int, typer.Option("--n-periods", "-n")] = 30,
    descending: Annotated[bool, typer.Option("--desc/--asc", "-d/-a")] = True,
    output: Annotated[
        CLIOutputFormat, typer.Option("--output", "-o")
    ] = CLIOutputFormat.table,
) -> None:
    model = get_feed_list_response(repo, event, action, 365, 10_000, descending)
    summary = create_feed_summary(model, granularity, descending, n_periods)

    if output == CLIOutputFormat.json:
        console.print_json(summary.model_dump_json())
        return

    table = format_feed_summary(summary)
    console.print(table)


@feed_app.command("list")
def list_feed(
    repo: Annotated[RepoEnum | None, typer.Option("--repo", "-r")] = None,
    event: Annotated[FeedPublicEvent | None, typer.Option("--event", "-e")] = None,
    action: Annotated[FeedPublicAction | None, typer.Option("--action", "-a")] = None,
    n_days: Annotated[int, typer.Option("--n-days", "-n")] = 60,
    limit: Annotated[int, typer.Option("--limit", "-l")] = 10_000,
    descending: Annotated[bool, typer.Option("--desc/--asc", "-d/-a")] = True,
    output: Annotated[
        CLIOutputFormat, typer.Option("--output", "-o")
    ] = CLIOutputFormat.table,
) -> None:
    model = get_feed_list_response(repo, event, action, n_days, limit, descending)
    if output == CLIOutputFormat.json:
        console.print_json(model.model_dump_json())
        return

    table = format_feed_output(model)
    console.print(table)
