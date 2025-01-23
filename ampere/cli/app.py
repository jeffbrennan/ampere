import typer

from ampere.cli.commands.downloads import downloads_app
from ampere.cli.commands.feed import feed_app

app = typer.Typer()
app.add_typer(downloads_app, name="downloads")
app.add_typer(feed_app, name="feed")


if __name__ == "__main__":
    app()
