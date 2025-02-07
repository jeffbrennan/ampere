from typing import Annotated

import requests
import typer
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


@feed_app.command("summary")
def summarize_feeds():
    print("Summary of feeds")


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

    if output == CLIOutputFormat.json:
        console.print_json(model.model_dump_json())
        return

    table = format_feed_output(model)
    console.print(table)
