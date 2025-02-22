from dataclasses import asdict, dataclass
from enum import StrEnum, auto
import json
from pathlib import Path

import typer
from rich.console import Console

from ampere.common import timeit

console = Console()
scaffold_app = typer.Typer(help="manage dashboard resources")


class ValidationType(StrEnum):
    up = auto()
    down = auto()


@dataclass
class ScaffoldTestExists:
    def passes(self, validation_type: ValidationType):
        results = all([self.dotenv, self.bronze, self.frontend_db, self.backend_db])
        if validation_type == ValidationType.up:
            return results
        return not results

    dotenv: bool
    bronze: bool
    frontend_db: bool
    backend_db: bool


@dataclass
class ScaffoldTestValid:
    def passes(self):
        return all([self.dotenv])

    dotenv: bool


@dataclass
class ScaffoldTests:
    def pretty_print(self):
        console.print_json(json.dumps(asdict(self)))

    def passes(self, validation_type: ValidationType):
        return all([self.exists.passes(validation_type), self.valid.passes()])

    exists: ScaffoldTestExists
    valid: ScaffoldTestValid


def test_resources_exist(verbose: bool = False) -> bool:
    return test(verbose).exists.passes(ValidationType.up)


def test_resources_do_not_exist(verbose: bool = False) -> bool:
    return test(verbose).exists.passes(ValidationType.down)


def validate_dotenv(dotenv: Path) -> bool:
    required_vars = [
        "GITHUB_TOKEN",
        "AMPERE_HOST_PATH",
        "GCLOUD_PROJECT",
        "AMPERE_BACKEND",
        "BACKEND_ADMIN",
        "GOOGLE_APPLICATION_CREDENTIALS",
        "AMPERE_BACKEND_EMAIL_FROM",
        "AMPERE_BACKEND_EMAIL_PW",
        "AMPERE_BACKEND_EMAIL_LIST",
    ]

    if not dotenv.exists():
        return False

    with open(dotenv, "r") as f:
        dotenv_vars = f.readlines()

    dotenv_vars = [var.split("=")[0] for var in dotenv_vars]
    missing_vars = [var for var in required_vars if var not in dotenv_vars]
    if missing_vars:
        console.print(f"missing required variables: {missing_vars}")
        return False

    return True


@scaffold_app.command(help="run tests")
def test(verbose: bool = False) -> ScaffoldTests:
    root_dir = Path(__file__).parents[3]
    dotenv_exists = (root_dir / ".env").exists()
    bronze_exists = (root_dir / "data" / "bronze").exists()
    frontend_db_exists = (root_dir / "data" / "frontend.duckdb").exists()
    backend_db_exists = (root_dir / "data" / "backend.duckdb").exists()

    dotenv_valid = validate_dotenv(root_dir / ".env")

    exists = ScaffoldTestExists(
        dotenv=dotenv_exists,
        bronze=bronze_exists,
        frontend_db=frontend_db_exists,
        backend_db=backend_db_exists,
    )
    valid = ScaffoldTestValid(dotenv=dotenv_valid)
    tests = ScaffoldTests(exists, valid)

    if verbose:
        tests.pretty_print()

    return tests


def create_dotenv(root_dir: Path) -> None:
    dot_env_location = root_dir / ".env"
    if dot_env_location.exists():
        return
    console.print("creating .env file...")
    with open(dot_env_location, "w") as f:
        f.write("")
    console.print("please populate the .env file using .env.example as a template")


def create_bronze(root_dir: Path) -> None:
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


def create_databases(root_dir: Path) -> None:
    db_names = ["frontend", "backend"]
    for db_name in db_names:
        db = root_dir / "data" / f"{db_name}.duckdb"
        if not db.exists():
            console.print(f"creating {db_name} database...")
            with open(db, "w") as f:
                f.write("")


def create_all_resources(root_dir: Path) -> None:
    resources_missing = test_resources_do_not_exist(verbose=True)
    if not resources_missing:
        console.print("resources already exist - exiting early")
        return

    console.print("setting up resources for a new project")

    create_dotenv(root_dir)
    create_bronze(root_dir)
    create_databases(root_dir)


@scaffold_app.command(help="build resources")
@timeit
def up(ctx: typer.Context) -> None:
    root_dir = Path(__file__).parents[3]
    create_all_resources(root_dir)
    # run dagster init

    # run dbt init and compile

    # run duckdb view creation

    # tests
    # verify .env contains all required variables
    # verify github api can be queried
    # verify pypi api can be queried
    # verify views exist in duckdb


@scaffold_app.command(help="destroy resources")
@timeit
def down(ctx: typer.Context) -> None:
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
