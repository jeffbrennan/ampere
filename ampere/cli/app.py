from typing import Annotated

import typer

from ampere.cli.commands.downloads import downloads_app
from ampere.cli.commands.feed import feed_app
from ampere.cli.commands.repos import repos_app
from ampere.cli.common import CLIEnvironment

app = typer.Typer(pretty_exceptions_enable=False)
app.add_typer(downloads_app, name="downloads")
app.add_typer(feed_app, name="feed")
app.add_typer(repos_app, name="repos")


@app.callback()
def set_env(
    ctx: typer.Context,
    env: Annotated[CLIEnvironment, typer.Option("--env", "-e")] = CLIEnvironment.prod,
) -> None:
    ctx.obj = {"env": env}


@app.command()
def welcome():
    typer.echo("Welcome to Ampere CLI")
    typer.echo("Use --help to see available commands")


if __name__ == "__main__":
    app()
