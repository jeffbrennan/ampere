import json
import os
import shutil
from dataclasses import asdict, dataclass, field
from enum import StrEnum, auto
from pathlib import Path

import duckdb
import pyarrow as pa
import requests
import typer
from deltalake import DeltaTable
from deltalake._internal import TableNotFoundError
from duckdb import IOException
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
class Validation:
    name: str
    passes: bool = False
    error: str | None = "uncaught error"


@dataclass
class ExistsInfo:
    name: str
    exists: bool = False
    location: str | None = None


@dataclass
class ScaffoldTestExists:
    dotenv: ExistsInfo = field(default_factory=lambda: ExistsInfo("dotenv"))
    bronze: ExistsInfo = field(default_factory=lambda: ExistsInfo("bronze"))
    frontend_db: ExistsInfo = field(default_factory=lambda: ExistsInfo("frontend_db"))
    backend_db: ExistsInfo = field(default_factory=lambda: ExistsInfo("backend_db"))

    def pretty_print(self) -> None:
        dict_repr = asdict(self)
        for _, v in dict_repr.items():
            del v["name"]

        console.print_json(json.dumps(dict_repr))

    def passes(self, validation_type: ValidationType):
        results = [self.dotenv, self.bronze, self.frontend_db, self.backend_db]
        if validation_type == ValidationType.up:
            return all(results)
        return not any(results)


@dataclass
class ScaffoldTestValidDotenv:
    dotenv_file: Validation = field(default_factory=lambda: Validation("dotenv_file"))
    host_path: Validation = field(default_factory=lambda: Validation("host_path"))
    github: Validation = field(default_factory=lambda: Validation("github"))
    gcloud: Validation = field(default_factory=lambda: Validation("gcloud"))


@dataclass
class ScaffoldTestValidAPI:
    github: Validation = field(default_factory=lambda: Validation("github"))
    bigquery: Validation = field(default_factory=lambda: Validation("bigquery"))


@dataclass
class ScaffoldTestValidBackend:
    views: Validation = field(default_factory=lambda: Validation("views"))


@dataclass
class ScaffoldTestValid:
    dotenv: ScaffoldTestValidDotenv = field(default_factory=ScaffoldTestValidDotenv)
    api_access: ScaffoldTestValidAPI = field(default_factory=ScaffoldTestValidAPI)
    backend: ScaffoldTestValidBackend = field(default_factory=ScaffoldTestValidBackend)

    def get_all_validations(self) -> dict[str, list[Validation]]:
        return {
            "dotenv": [
                self.dotenv.dotenv_file,
                self.dotenv.host_path,
                self.dotenv.github,
                self.dotenv.gcloud,
            ],
            "api_access": [
                self.api_access.github,
                self.api_access.bigquery,
                self.backend.views,
            ],
        }

    def passes(self) -> bool:
        results = self.get_all_validations()
        dotenv_passes = (i.passes for i in results["dotenv"])
        api_access_passes = (i.passes for i in results["api_access"])
        return all(dotenv_passes) and all(api_access_passes)

    def list_fails(self) -> dict[str, list[Validation]]:
        results = self.get_all_validations()
        output = {"dotenv": [], "api_access": []}
        for key, validations in results.items():
            for validation in validations:
                if not validation.passes:
                    output[key].append(validation)
        return output


@dataclass
class ScaffoldTests:
    exists: ScaffoldTestExists
    valid: ScaffoldTestValid

    def pretty_print(self):
        dict_repr = asdict(self)

        errors_to_remove: list[tuple[int, str] | str] = []
        for key, validations in dict_repr["valid"].items():
            if isinstance(validations, dict):
                for k, v in validations.items():
                    if v["error"] is None:
                        errors_to_remove.append((k, key))
            else:
                if validations["error"] is None:
                    errors_to_remove.append((-1, key))

        for i, key in errors_to_remove:
            # non-list validations
            if i == -1:
                del dict_repr["valid"][key]["error"]
                continue

            del dict_repr["valid"][key][i]["error"]

        console.print_json(json.dumps(dict_repr))

    def passes(self, validation_type: ValidationType):
        return all([self.exists.passes(validation_type), self.valid.passes()])


def test_up_required(verbose: bool) -> bool:
    test_results = test(verbose=verbose)
    resources_exist = test_results.exists.passes(ValidationType.up)
    resources_valid = test_results.valid.passes()
    return not resources_exist or not resources_valid


def test_down_required(verbose: bool) -> bool:
    test_result = test(verbose=verbose, validation_type=ValidationType.down)
    test_result.exists.pretty_print()

    return not test_result.exists.passes(ValidationType.down)


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
    required_vars = get_required_dotenv_vars()
    missing_vars = [var for var in required_vars if var not in dotenv_keys]
    if missing_vars:
        validation.error = "missing required variables"
        return validation, dotenv_keys, dotenv_values

    validation.passes = True
    validation.error = None
    return validation, dotenv_keys, dotenv_values


def validate_dotenv(
    dotenv: Path,
) -> tuple[ScaffoldTestValidDotenv, list[str], list[tuple]]:
    validations = ScaffoldTestValidDotenv()

    validations.dotenv_file, dotenv_keys, dotenv_values = validate_dotenv_file(dotenv)
    if not validations.dotenv_file.passes:
        return validations, dotenv_keys, dotenv_values

    validations.host_path = validate_dotenv_host_path(dotenv_keys, dotenv_values)
    validations.github = validate_dotenv_github(dotenv_keys, dotenv_values)
    validations.gcloud = validate_dotenv_gcloud(dotenv_keys, dotenv_values)
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
    validation.passes = True
    validation.error = None
    return validation

    # this will consume 8mb of the free 1tb monthly quota
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


def validate_api_access(api_key: str, gcloud_auth_path: Path) -> ScaffoldTestValidAPI:
    return ScaffoldTestValidAPI(
        github=validate_github_api_access(api_key),
        bigquery=validate_bigquery_access(gcloud_auth_path),
    )


def validate_backend_db_views(backend_db: Path) -> ScaffoldTestValidBackend:
    validation = ScaffoldTestValidBackend()
    try:
        con = duckdb.connect(backend_db)
    except IOException as e:
        validation.views.error = str(e).replace('"', "'")
        return validation

    views = con.execute(
        "select sql from duckdb_views() where sql like '%delta_scan%';"
    ).fetchall()

    views_flat = [view[0] for view in views]
    bronze_directories = get_bronze_directories()

    errors = []
    for directory in bronze_directories:
        substr_to_check = f'delta_scan("data/bronze/{directory}")'
        if not any(substr_to_check in view for view in views_flat):
            errors.append(directory)

    if errors:
        validation.views.error = f"missing views for bronze delta tables: {errors}"
        return validation

    validation.views.passes = True
    validation.views.error = None
    return validation


def get_required_dotenv_vars() -> list[str]:
    dotenv_example_path = Path(__file__).parents[3] / ".env.example"
    with dotenv_example_path.open("r") as f:
        dotenv_example = f.readlines()

    keys = [line.split("=")[0] for line in dotenv_example if "=" in line]
    return keys


def get_bronze_directories() -> list[str]:
    return [
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


@scaffold_app.command(help="run tests")
def test(
    raise_error: bool = False,
    verbose: bool = False,
    validation_type: ValidationType = ValidationType.up,
) -> ScaffoldTests:
    root_dir = Path(__file__).parents[3]
    dotenv_path = root_dir / ".env"
    bronze_path = root_dir / "data" / "bronze"
    frontend_path = root_dir / "data" / "frontend.duckdb"
    backend_path = root_dir / "data" / "backend.duckdb"
    exists = ScaffoldTestExists()

    exists.dotenv.exists = dotenv_path.exists()
    exists.dotenv.location = dotenv_path.as_posix()

    exists.bronze.exists = bronze_path.exists()
    exists.bronze.location = bronze_path.as_posix()

    exists.frontend_db.exists = frontend_path.exists()
    exists.frontend_db.location = frontend_path.as_posix()

    exists.backend_db.exists = backend_path.exists()
    exists.backend_db.location = backend_path.as_posix()

    dotenv_valid, dotenv_keys, dotenv_values = validate_dotenv(root_dir / ".env")

    api_access_valid = ScaffoldTestValidAPI()
    if dotenv_valid.github.passes and dotenv_valid.gcloud.passes:
        github_api_key = dotenv_values[dotenv_keys.index("GITHUB_TOKEN")][1]
        gcloud_auth_path = Path(
            dotenv_values[dotenv_keys.index("GOOGLE_APPLICATION_CREDENTIALS")][1]
        )
        api_access_valid = validate_api_access(github_api_key, gcloud_auth_path)

    backend_valid = ScaffoldTestValidBackend()
    if backend_path.exists():
        backend_valid = validate_backend_db_views(backend_path)

    valid = ScaffoldTestValid(
        dotenv=dotenv_valid, api_access=api_access_valid, backend=backend_valid
    )

    tests = ScaffoldTests(exists, valid)

    passed = tests.passes(validation_type)
    if verbose:
        tests.pretty_print()

    if passed or not raise_error:
        return tests

    raise Exception("resources are not setup correctly")


def create_dotenv(root_dir: Path) -> None:
    dot_env_location = root_dir / ".env"
    dot_env_example_location = root_dir / ".env.example"
    if dot_env_location.exists():
        return
    console.print("creating .env file...")

    with open(dot_env_example_location, "r") as f:
        dotenv_example = f.readlines()

    with open(dot_env_location, "w") as f:
        f.writelines(dotenv_example)
    console.print("please populate the .env file using .env.example as a template")


def create_bronze_table(bronze_path: Path) -> None:
    schema_lookup = {
        "commits": pa.schema(
            [
                ("repo_id", pa.int64()),
                ("commit_id", pa.string()),
                ("author_id", pa.float64()),
                ("comment_count", pa.int64()),
                ("message", pa.string()),
                (
                    "stats",
                    pa.list_(
                        pa.struct(
                            [
                                ("additions", pa.int64()),
                                ("changes", pa.int64()),
                                ("deletions", pa.int64()),
                                ("filename", pa.string()),
                                ("status", pa.string()),
                            ]
                        )
                    ),
                ),
                ("committed_at", pa.timestamp("us", tz="UTC")),
                ("retrieved_at", pa.timestamp("us", tz="UTC")),
            ]
        ),
        "followers": pa.schema(
            [
                ("user_id", pa.int64()),
                ("follower_id", pa.int64()),
                ("retrieved_at", pa.timestamp("us", tz="UTC")),
            ]
        ),
        "forks": pa.schema(
            [
                ("repo_id", pa.int64()),
                ("fork_id", pa.int64()),
                ("owner_id", pa.int64()),
                ("created_at", pa.timestamp("us", tz="UTC")),
                ("retrieved_at", pa.timestamp("us", tz="UTC")),
            ]
        ),
        "issues": pa.schema(
            [
                ("repo_id", pa.int64()),
                ("issue_id", pa.int64()),
                ("issue_number", pa.int64()),
                ("issue_title", pa.string()),
                ("issue_body", pa.string()),
                ("author_id", pa.int64()),
                ("state", pa.string()),
                ("state_reason", pa.string()),
                ("comments_count", pa.int64()),
                ("created_at", pa.timestamp("us", tz="UTC")),
                ("updated_at", pa.timestamp("us", tz="UTC")),
                ("closed_at", pa.timestamp("us", tz="UTC")),
                ("retrieved_at", pa.timestamp("us", tz="UTC")),
            ]
        ),
        "pull_requests": pa.schema(
            [
                ("repo_id", pa.int64()),
                ("pr_id", pa.int64()),
                ("pr_number", pa.int64()),
                ("pr_title", pa.string()),
                ("pr_state", pa.string()),
                ("pr_body", pa.string()),
                ("author_id", pa.int64()),
                ("created_at", pa.timestamp("us", tz="UTC")),
                ("updated_at", pa.timestamp("us", tz="UTC")),
                ("closed_at", pa.timestamp("us", tz="UTC")),
                ("merged_at", pa.timestamp("us", tz="UTC")),
                ("retrieved_at", pa.timestamp("us", tz="UTC")),
            ]
        ),
        "pypi_download_queries": pa.schema(
            [
                ("repo", pa.string()),
                ("retrieved_at", pa.timestamp("us", tz="UTC")),
                ("min_date", pa.string()),
                ("max_date", pa.string()),
            ]
        ),
        "pypi_downloads": pa.schema(
            [
                ("project", pa.string()),
                ("timestamp", pa.timestamp("us", tz="UTC")),
                ("country_code", pa.string()),
                ("package_version", pa.string()),
                ("python_version", pa.string()),
                ("system_distro_name", pa.string()),
                ("system_distro_version", pa.string()),
                ("system_name", pa.string()),
                ("system_release", pa.string()),
                ("download_count", pa.int64()),
                ("retrieved_at", pa.timestamp("us", tz="UTC")),
            ]
        ),
        "releases": pa.schema(
            [
                ("repo_id", pa.int64()),
                ("release_id", pa.int64()),
                ("release_name", pa.string()),
                ("tag_name", pa.string()),
                ("release_body", pa.string()),
                ("created_at", pa.timestamp("us", tz="UTC")),
                ("published_at", pa.timestamp("us", tz="UTC")),
                ("retrieved_at", pa.timestamp("us", tz="UTC")),
            ]
        ),
        "repos": pa.schema(
            [
                ("repo_id", pa.int64()),
                ("repo_name", pa.string()),
                ("license", pa.string()),
                ("topics", pa.list_(pa.string())),
                (
                    "language",
                    pa.list_(
                        pa.struct([("name", pa.string()), ("size_bytes", pa.int64())])
                    ),
                ),
                ("repo_size", pa.int64()),
                ("forks_count", pa.int64()),
                ("stargazers_count", pa.int64()),
                ("open_issues_count", pa.int64()),
                ("pushed_at", pa.timestamp("us", tz="UTC")),
                ("created_at", pa.timestamp("us", tz="UTC")),
                ("updated_at", pa.timestamp("us", tz="UTC")),
                ("retrieved_at", pa.timestamp("us", tz="UTC")),
            ]
        ),
        "stargazers": pa.schema(
            [
                ("repo_id", pa.int64()),
                ("user_id", pa.int64()),
                ("starred_at", pa.timestamp("us", tz="UTC")),
                ("retrieved_at", pa.timestamp("us", tz="UTC")),
            ]
        ),
        "users": pa.schema(
            [
                ("user_id", pa.int64()),
                ("user_name", pa.string()),
                ("full_name", pa.string()),
                ("company", pa.string()),
                ("avatar_url", pa.string()),
                ("repos_count", pa.int64()),
                ("followers_count", pa.int64()),
                ("following_count", pa.int64()),
                ("created_at", pa.timestamp("us", tz="UTC")),
                ("updated_at", pa.timestamp("us", tz="UTC")),
                ("retrieved_at", pa.timestamp("us", tz="UTC")),
            ]
        ),
    }

    directory = bronze_path.stem
    if directory not in schema_lookup:
        console.print(f"no schema found for {directory}")
        return

    bronze_path.mkdir(exist_ok=True)

    schema = schema_lookup[directory]
    dt = DeltaTable.create(bronze_path, schema)
    dt.schema()


def create_bronze(root_dir: Path) -> list[str]:
    data_dir = root_dir / "data"
    bronze_dir = data_dir / "bronze"
    if not data_dir.exists():
        console.print("creating data directory...")
        data_dir.mkdir()

    # add bronze parent dir
    if not bronze_dir.exists():
        console.print("creating bronze directory...")
        bronze_dir.mkdir()

    # add bronze directories
    bronze_directories = get_bronze_directories()
    for directory in bronze_directories:
        bronze_path = bronze_dir / directory

        dir_exists = bronze_path.exists()
        dt_exists = False
        try:
            _ = DeltaTable(bronze_path)
            dt_exists = True
        except TableNotFoundError:
            pass

        if not dir_exists or not dt_exists:
            console.print(f"creating bronze delta table: {directory}")
            create_bronze_table(bronze_path)

    return bronze_directories


def create_databases(root_dir: Path) -> None:
    db_names = ["frontend", "backend"]
    for db_name in db_names:
        db = root_dir / "data" / f"{db_name}.duckdb"
        if db.exists():
            continue

        console.print(f"creating {db_name} database...")
        with duckdb.connect(db) as con:
            con.execute("select 1 as test;").fetchall()


def create_views(root_dir: Path, bronze_directories: list[str]) -> None:
    view_init_script = root_dir / "data" / "create_views.sql"
    base_cmd = 'create or replace view VIEW_NAME as select * from delta_scan("data/bronze/VIEW_NAME");\n'
    backend_db = root_dir / "data" / "backend.duckdb"
    if view_init_script.exists() and validate_backend_db_views(backend_db).views.passes:
        return

    console.print("creating view init script...")
    with open(view_init_script, "w") as f:
        for directory in bronze_directories:
            create_cmd = base_cmd.replace("VIEW_NAME", directory)
            f.write(create_cmd)

    console.print("creating views...")
    with open(view_init_script, "r") as view_init_script:
        commands = view_init_script.readlines()

    with duckdb.connect(backend_db) as con:
        for command in commands:
            con.execute(command).commit()


def create_all_resources(root_dir: Path) -> None:
    up_required = test_up_required(verbose=False)
    if not up_required:
        console.print("resources already exist - exiting early")
        return

    console.print("building scaffold...")

    create_dotenv(root_dir)
    bronze_directories = create_bronze(root_dir)
    create_databases(root_dir)
    create_views(root_dir, bronze_directories)

    results = test(verbose=False)
    if not results.passes(ValidationType.up):
        print("-" * 40)
        console.print("⚠️ setup not complete")
        console.print(results.valid.list_fails())


def run_dagster_job(job_name: str | None) -> None:
    available_jobs = ["github_metrics_daily_4", "bigquery_backfill", "bigquery_daily"]

    if job_name is None:
        for job in available_jobs:
            os.system(f"dagster job execute -j {job}")
        return

    if job_name not in available_jobs:
        console.print(f"job {job_name} not found. available jobs: {available_jobs}")
        return

    os.system(f"dagster job execute -j {job_name}")


@scaffold_app.command(help="build resources")
@timeit
def up(ctx: typer.Context) -> None:
    root_dir = Path(__file__).parents[3]
    create_all_resources(root_dir)
    # run dagster init
    # get initial data


@scaffold_app.command(help="seed data - this will take a while and consume api limits")
def seed(job_name: str | None):
    run_dagster_job(job_name)


@scaffold_app.command(help="destroy resources")
@timeit
def down(ctx: typer.Context) -> None:
    down_required = test_down_required(verbose=False)
    if not down_required:
        console.print("resources do not exist - exiting early")
        return

    typer.confirm("Are you sure you want to delete all resources?", abort=True)
    console.print("tearing down scaffold...")
    root_dir = Path(__file__).parents[3]
    # remove .env file
    dot_env_location = root_dir / ".env"
    if dot_env_location.exists():
        console.print("removing .env file...")
        dot_env_location.unlink()

    # remove bronze directories
    data_dir = root_dir / "data" / "bronze"
    if data_dir.exists():
        shutil.rmtree(data_dir)

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
