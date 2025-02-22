from pathlib import Path
import typer
from rich.console import Console


console = Console()
scaffold_app = typer.Typer(help="manage dashboard resources")


@scaffold_app.command(help="build resources")
def up(ctx: typer.Context) -> None:
    console.print("setting up resources for a new project")

    root_dir = Path(__file__).parents[3]
    dot_env_location = root_dir / ".env"
    if not dot_env_location.exists():
        console.print("creating .env file...")
        with open(dot_env_location, "w") as f:
            f.write("")
        console.print("please populate the .env file using .env.example as a template")

    # create a new project directory
    data_dir = root_dir / "data"
    if not data_dir.exists():
        console.print("creating data directory...")
        data_dir.mkdir()

    # add bronze parent dir
    if not (data_dir / "bronze").exists():
        console.print("creating bronze directory...")
        (data_dir / "bronze").mkdir()

    # add bronze directories
    bronze_directories = [
        "commits",
        "followers",
        "forks",
        "issues",
        "pull_requests",
        "pypi_download_queries",
        "pypi_downloads",
        "releases",
        "repos",
        "stargazers",
        "users",
    ]

    for directory in bronze_directories:
        if not (data_dir / "bronze" / directory).exists():
            console.print(f"creating bronze directory: {directory}")
            (data_dir / "bronze" / directory).mkdir()

    # create frontend and backend databases
    frontend_db = data_dir / "frontend.duckdb"
    if not frontend_db.exists():
        console.print("creating frontend database...")
        with open(frontend_db, "w") as f:
            f.write("")

    backend_db = data_dir / "backend.duckdb"
    if not backend_db.exists():
        console.print("creating backend database...")
        with open(backend_db, "w") as f:
            f.write("")

    # run dagster init

    # run dbt init and compile

    # run duckdb view creation

    # tests
    # verify github api can be queried
    # verify pypi api can be queried
    # verify views exist in duckdb


@scaffold_app.command(help="destroy resources")
def down(help="destroy resources"):
    console.print("removing resources for a new project")
    root_dir = Path(__file__).parents[3]
    # remove .env file
    dot_env_location = root_dir / ".env"
    if dot_env_location.exists():
        console.print("removing .env file...")
        dot_env_location.unlink()

    # remove bronze directories
    data_dir = root_dir / "data" / "bronze"
    if data_dir.exists():
        console.print("removing data directory...")
        for directory in data_dir.iterdir():
            directory.rmdir()
        data_dir.rmdir()

    # remove frontend and backend databases
    frontend_db = root_dir / "data" / "frontend.duckdb"
    if frontend_db.exists():
        console.print("removing frontend database...")
        frontend_db.unlink()

    backend_db = root_dir / "data" / "backend.duckdb"
    if backend_db.exists():
        console.print("removing backend database...")
        backend_db.unlink()

    # add tests to ensure workspace is setup correctly
    console.print("resources removed")
