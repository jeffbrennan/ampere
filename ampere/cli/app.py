import typer

from ampere.cli.commands.downloads import downloads_app
from ampere.cli.commands.feed import feed_app
from ampere.cli.common import CLIEnvironment
from ampere.cli.state import State

app = typer.Typer(pretty_exceptions_enable=False)
app.add_typer(downloads_app, name="downloads")
app.add_typer(feed_app, name="feed")


@app.callback()
def main(env: CLIEnvironment = CLIEnvironment.prod):
    State.env = env


if __name__ == "__main__":
    app()
