import json
import os
from dataclasses import asdict, dataclass
from enum import StrEnum, auto
from pathlib import Path

import requests
import typer
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery
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
class Validation:
    name: str
    passes: bool = False
    error: str | None = "uncaught error"


@dataclass
class ScaffoldTestValid:
    def passes(self):
        validations = self.dotenv + self.api_access
        return all([validation.passes for validation in validations])

    dotenv: list[Validation]
    api_access: list[Validation]


@dataclass
class ScaffoldTests:
    def pretty_print(self):
        dict_repr = asdict(self)

        errors_to_remove: list[tuple[int, str]] = []
        for key, validations in dict_repr["valid"].items():
            for i, validation in enumerate(validations):
                if validation["error"] is None:
                    errors_to_remove.append((i, key))

        for i, key in errors_to_remove:
            del dict_repr["valid"][key][i]["error"]

        console.print_json(json.dumps(dict_repr))

    def passes(self, validation_type: ValidationType):
        return all([self.exists.passes(validation_type), self.valid.passes()])

    exists: ScaffoldTestExists
    valid: ScaffoldTestValid


def test_resources_exist(verbose: bool = False) -> bool:
    return test(verbose).exists.passes(ValidationType.up)


def test_resources_do_not_exist(verbose: bool = False) -> bool:
    return test(verbose).exists.passes(ValidationType.down)


def validate_dotenv_host_path(
    dotenv_keys: list[str], dotenv_values: list[tuple]
) -> Validation:
    host_path_index = dotenv_keys.index("AMPERE_HOST_PATH")
    host_path = dotenv_values[host_path_index][1]
    host_path = Path(host_path).expanduser().resolve()

    validation = Validation("host_path")

    if '"' in host_path.as_posix():
        validation.error = "host path contains illegal quote character"
        return validation

    if not host_path.exists():
        validation.error = "host path does not exist"
        return validation

    if not host_path.is_dir():
        validation.error = "host path is not a directory"
        return validation

    validation.passes = True
    validation.error = None
    return validation


def validate_dotenv_gcloud(
    dotenv_keys: list[str], dotenv_values: list[tuple]
) -> Validation:
    google_credentials_index = dotenv_keys.index("GOOGLE_APPLICATION_CREDENTIALS")
    google_credentials = dotenv_values[google_credentials_index][1]
    google_credentials_path = Path(google_credentials).expanduser().resolve()

    validation = Validation("gcloud")

    if not google_credentials_path.exists():
        validation.error = "google credentials file does not exist"
        return validation

    if (
        not google_credentials_path.parent
        == Path("~/.config/gcloud").expanduser().resolve()
    ):
        validation.error = "google credentials file is not in ~/.config/gcloud"
        return validation

    if not google_credentials_path.suffix == ".json":
        validation.error = "google credentials file is not a json file"
        return validation

    if not google_credentials_path.stem == "application_default_credentials":
        validation.error = (
            "google credentials file is not named application_default_credentials"
        )
        return validation

    validation.passes = True
    validation.error = None
    return validation


def validate_dotenv_github(
    dotenv_keys: list[str], dotenv_values: list[tuple]
) -> Validation:
    github_token_index = dotenv_keys.index("GITHUB_TOKEN")
    github_token = dotenv_values[github_token_index][1]
    github_token_valid = github_token.startswith("ghp_")

    validation = Validation("github")

    if not github_token_valid:
        validation.error = "github token is not valid"
        return validation

    validation.passes = True
    validation.error = None
    return validation


def validate_dotenv_file(dotenv: Path) -> tuple[Validation, list[str], list[tuple]]:
    validation = Validation("dotenv_file")
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
        validation.error = ".env file does not exist"
        return validation, [], []

    with open(dotenv, "r") as f:
        dotenv_raw = f.readlines()

    dotenv_values = []
    for line in dotenv_raw:
        if line.startswith("#"):
            continue
        dotenv_values.append(line.strip().split("="))

    dotenv_keys = [var[0] for var in dotenv_values]
    missing_vars = [var for var in required_vars if var not in dotenv_keys]
    if missing_vars:
        validation.error = "missing required variables"
        return validation, dotenv_keys, dotenv_values

    validation.passes = True
    validation.error = None
    return validation, dotenv_keys, dotenv_values


def validate_dotenv(dotenv: Path) -> tuple[list[Validation], list[str], list[tuple]]:
    validations: list[Validation] = []

    dotenv_file_validation, dotenv_keys, dotenv_values = validate_dotenv_file(dotenv)
    validations.append(dotenv_file_validation)
    if not dotenv_file_validation.passes:
        return validations, dotenv_keys, dotenv_values

    host_path_validation = validate_dotenv_host_path(dotenv_keys, dotenv_values)
    validations.append(host_path_validation)
    if not host_path_validation.passes:
        return validations, dotenv_keys, dotenv_values

    github_validation = validate_dotenv_github(dotenv_keys, dotenv_values)
    validations.append(github_validation)
    if not github_validation.passes:
        return validations, dotenv_keys, dotenv_values

    gcloud_validation = validate_dotenv_gcloud(dotenv_keys, dotenv_values)
    validations.append(gcloud_validation)
    if not gcloud_validation.passes:
        return validations, dotenv_keys, dotenv_values

    return validations, dotenv_keys, dotenv_values


def validate_github_api_access(api_key: str) -> Validation:
    validation = Validation("github")
    response = requests.get(
        "https://api.github.com/rate_limit",
        headers={"Authorization": f"Bearer {api_key}"},
        params={
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )

    if response.status_code != 200:
        validation.error = f"github api key is not valid: {response.text}"
        return validation

    validation.passes = True
    validation.error = None
    return validation


def validate_bigquery_access(gcloud_auth_path: Path) -> Validation:
    validation = Validation("bigquery")

    query = """
        select project
        from `bigquery-public-data.pypi.file_downloads`
        where project = 'ampere-meter'
            and TIMESTAMP_TRUNC(timestamp, hour) = timestamp '2025-02-17 19:00:00'
        limit 1 
    """

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
        gcloud_auth_path.expanduser().resolve().as_posix()
    )

    try:
        client = bigquery.Client()
    except OSError as e:
        validation.error = str(e)
        return validation

    with client:
        try:
            query_job = client.query_and_wait(query)
        except BadRequest as e:
            validation.error = str(e)
            return validation

    results = query_job.to_dataframe()

    if results is None:
        validation.error = "bigquery access is not valid. query did not return results"
        return validation

    if results.shape[0] != 1:
        validation.error = (
            "bigquery access is not valid. query did not return expected results"
        )
        return validation

    validation.passes = True
    validation.error = None
    return validation


def validate_api_access(api_key: str, gcloud_auth_path: Path) -> list[Validation]:
    return [
        validate_github_api_access(api_key),
        validate_bigquery_access(gcloud_auth_path),
    ]


@scaffold_app.command(help="run tests")
def test(
    raise_error: bool = False,
    verbose: bool = False,
    validation_type: ValidationType = ValidationType.up,
) -> ScaffoldTests:
    root_dir = Path(__file__).parents[3]
    dotenv_exists = (root_dir / ".env").exists()
    bronze_exists = (root_dir / "data" / "bronze").exists()
    frontend_db_exists = (root_dir / "data" / "frontend.duckdb").exists()
    backend_db_exists = (root_dir / "data" / "backend.duckdb").exists()

    dotenv_valid, dotenv_keys, dotenv_values = validate_dotenv(root_dir / ".env")

    github_api_key = dotenv_values[dotenv_keys.index("GITHUB_TOKEN")][1]
    gcloud_auth_path = Path(
        dotenv_values[dotenv_keys.index("GOOGLE_APPLICATION_CREDENTIALS")][1]
    )

    api_access_valid = validate_api_access(github_api_key, gcloud_auth_path)

    exists = ScaffoldTestExists(
        dotenv=dotenv_exists,
        bronze=bronze_exists,
        frontend_db=frontend_db_exists,
        backend_db=backend_db_exists,
    )
    valid = ScaffoldTestValid(dotenv=dotenv_valid, api_access=api_access_valid)
    tests = ScaffoldTests(exists, valid)

    passed = tests.passes(validation_type)
    if passed:
        tests.pretty_print() if verbose else console.print("✔︎")
        return tests

    tests.pretty_print()
    if not raise_error:
        return tests

    raise Exception("resources are not setup correctly")


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
