from dataclasses import asdict, dataclass
from enum import StrEnum, auto
import json
from pathlib import Path

import typer
from rich.console import Console

console = Console()
scaffold_app = typer.Typer(help="manage dashboard resources")


class ValidationType(StrEnum):
    up = auto()
    down = auto()


@dataclass
class ScaffoldTestExists:
    dotenv: bool
    bronze: bool
    frontend_db: bool
    backend_db: bool


@dataclass
class ScaffoldTests:
    def pretty_print(self):
        console.print_json(json.dumps(asdict(self)))

    exists: ScaffoldTestExists


def test_resources_exist(verbose: bool = False) -> bool:
    return test(ValidationType.up, verbose)


def test_resources_do_not_exist(verbose: bool = False) -> bool:
    return test(ValidationType.down, verbose)


@scaffold_app.command(help="run tests")
def test(validation_type: ValidationType, verbose: bool = False) -> bool:
    root_dir = Path(__file__).parents[3]
    env_exists = (root_dir / ".env").exists()
    bronze_exists = (root_dir / "data" / "bronze").exists()
    frontend_db_exists = (root_dir / "data" / "frontend.duckdb").exists()
    backend_db_exists = (root_dir / "data" / "backend.duckdb").exists()

    exists = ScaffoldTestExists(
        dotenv=env_exists,
        bronze=bronze_exists,
        frontend_db=frontend_db_exists,
        backend_db=backend_db_exists,
    )
    tests = ScaffoldTests(exists=exists)

    if verbose:
        tests.pretty_print()

    if validation_type == ValidationType.up:
        return all([env_exists, bronze_exists, frontend_db_exists, backend_db_exists])

    return not any([env_exists, bronze_exists, frontend_db_exists, backend_db_exists])


@scaffold_app.command(help="build resources")
def up(ctx: typer.Context) -> None:
    resources_missing = test_resources_do_not_exist(verbose=True)
    if not resources_missing:
        console.print("resources already exist - exiting early")
        return

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
    deletion_required = test_resources_exist(verbose=True)
    if not deletion_required:
        console.print("resources do not exist - exiting early")
        return

    typer.confirm("Are you sure you want to delete all resources?", abort=True)
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
