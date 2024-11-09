from enum import StrEnum, auto
from typing import Any

import requests

from ampere.common import (
    write_delta_table,
    DeltaWriteConfig,
    get_model_primary_key,
    get_current_time,
    get_db_con,
)
from ampere.models import PyPIDownload


class PyPIEndpoint(StrEnum):
    system = auto()
    python_minor = auto()
    overall = auto()


def get_pypi_downloads(
    package: str, endpoints: list[PyPIEndpoint], assert_success: bool
) -> list[dict[str, Any]]:
    base_url = f"https://pypistats.org/api/packages/{package}"

    print(f"getting for {package}")
    results = []
    for endpoint in endpoints:
        url = f"{base_url}/{endpoint.value}"
        print(url)
        response = requests.get(url)
        if response.status_code != 200 and assert_success:
            raise ValueError(f"{response.status_code}")

        response_json = response.json()
        results.append(response_json)

    return results


def parse_pypi_downloads(results: list[dict[str, Any]]) -> list[PyPIDownload]:
    parsed_results: list[PyPIDownload] = []
    for endpoint in results:
        for result in endpoint["data"]:
            parsed_results.append(
                PyPIDownload(
                    package=endpoint["package"],
                    type=endpoint["type"],
                    category=result["category"],
                    date=result["date"],
                    downloads=result["downloads"],
                    retrieved_at=get_current_time(),
                )
            )

    return parsed_results


def refresh_pypi_minor_version_downloads(package_name: str, config: DeltaWriteConfig):
    endpoints_to_get = [
        PyPIEndpoint.system,
        PyPIEndpoint.python_minor,
        PyPIEndpoint.overall,
    ]
    results = get_pypi_downloads(package_name, endpoints_to_get, True)
    parsed_results = parse_pypi_downloads(results)
    write_delta_table(parsed_results, config.table_dir, config.table_name, config.pks)


def get_repos_with_releases() -> list[str]:
    con = get_db_con()
    records = con.sql(
        """
        with
            release_repos as (
                select distinct
                    repo_id
                from releases
            ),
            repo_details as (
                select
                    a.repo_id,
                    a.repo_name,
                    unnest(a.language) as "language"
                from repos               a
                inner join release_repos b
                on a.repo_id = b.repo_id
            )
        select
            repo_name
        from repo_details
        where
            language.name = 'Python'   
        """
    ).fetchall()

    return [i[0] for i in records]


def main():
    downloads_minor_config = DeltaWriteConfig(
        table_dir="bronze",
        table_name=PyPIDownload.__tablename__,  # pyright: ignore [reportArgumentType]
        pks=get_model_primary_key((PyPIDownload)),
    )

    repos_with_releases = get_repos_with_releases()
    if len(repos_with_releases) == 0:
        print("no repos to collect stats for! exiting early")
        return

    for repo in repos_with_releases:
        refresh_pypi_minor_version_downloads(
            repo,
            downloads_minor_config,
        )


if __name__ == "__main__":
    main()
