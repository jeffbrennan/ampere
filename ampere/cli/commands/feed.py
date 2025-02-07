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


def create_feed_summary(
    model: FeedPublic, granularity: FeedGranularity, descending: bool = True
) -> FeedSummaryOutput:
    dates = set(i.event_timestamp for i in model.data)
    trunc_dates = sorted([date_trunc(i, granularity) for i in dates], reverse=descending)
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
        min_date=min(trunc_dates),
        max_date=max(trunc_dates),
        grand_total=grand_total_records[0],
    )


@feed_app.command("summary")
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
    n_days: Annotated[int, typer.Option("--n-days", "-n")] = 60,
    limit: Annotated[int, typer.Option("--limit", "-l")] = 10_000,
    descending: Annotated[bool, typer.Option("--desc/--asc", "-d/-a")] = True,
    output: Annotated[
        CLIOutputFormat, typer.Option("--output", "-o")
    ] = CLIOutputFormat.table,
) -> None:
    model = get_feed_list_response(repo, event, action, n_days, limit, descending)
    summary = create_feed_summary(model, granularity)

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
