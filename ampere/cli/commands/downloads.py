import typer

downloads_app = typer.Typer()


@downloads_app.command("list")
def list_downloads():
    print("List of downloads")


@downloads_app.command("summary")
def summarize_downloads():
    print("Summary of downloads")
