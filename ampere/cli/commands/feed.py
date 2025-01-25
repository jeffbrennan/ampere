import typer

feed_app = typer.Typer()


@feed_app.command("list")
def list_feeds():
    print("List of feeds")


@feed_app.command("summary")
def summarize_feeds():
    print("Summary of feeds")
