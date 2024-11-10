import time
from dataclasses import dataclass
from typing import Optional

from google.cloud import bigquery
from google.cloud.bigquery.table import RowIterator

from ampere.common import DeltaWriteConfig, get_model_primary_key, write_delta_table
from ampere.models import PyPIDownload


@dataclass
class PyPIQueryConfig:
    repo_list: list[str]
    min_date: str
    max_date: Optional[str] = None
    dry_run: bool = True


def get_pypi_downloads_from_bigquery(config: PyPIQueryConfig) -> Optional[RowIterator]:
    max_date_where = ""
    if config.max_date is not None:
        max_date_where = (
            f"and TIMESTAMP_TRUNC(timestamp, day) <= timestamp ('{config.max_date}')"
        )

    repo_list_formatted = "'" + "', '".join(config.repo_list) + "'"
    cmd = f"""
        select
            project,
            timestamp_trunc(`timestamp`, hour)          as `timestamp`,
            coalesce(country_code, 'unknown')           as `country_code`,
            coalesce(file.version, 'unknown')           as `package_version`,
            coalesce(details.python, 'unknown')         as `python_version`,
            coalesce(details.distro.name, 'unknown')    as `system_distro_name`,
            coalesce(details.distro.version, 'unknown') as `system_distro_version`,
            coalesce(details.system.name, 'unknown')    as `system_name`,
            coalesce(details.system.release, 'unknown') as `system_release`,
            count(*)                                    as download_count
        from `bigquery-public-data.pypi.file_downloads`
        where 
            TIMESTAMP_TRUNC(timestamp, day) >= timestamp ('{config.min_date}') 
            {max_date_where} 
            and project in ({repo_list_formatted})
        group by all
        """

    print(cmd)
    if config.dry_run:
        return

    client = bigquery.Client()
    query_job = client.query(cmd)
    start_time = time.time()
    results = query_job.result()

    elapsed_time = time.time() - start_time
    print(f"query finished in {elapsed_time:.2f} seconds")
    return results


def parse_pypi_downloads(results: RowIterator) -> list[PyPIDownload]:
    parsed_results = []
    for row in results:
        parsed_results.append(
            PyPIDownload(
                project=row.project,
                timestamp=row.timestamp,
                country_code=row.country_code,
                package_version=row.package_version,
                python_version=row.python_version,
                system_distro_name=row.system_distro_name,
                system_distro_version=row.system_distro_version,
                system_name=row.system_release,
                system_release=row.system_release,
                download_count=row.download_count,
                retrieved_at=row.retrieved_at,
            )
        )
    return parsed_results


def refresh_pypi_downloads_from_bigquery(
    query_config: PyPIQueryConfig, write_config: DeltaWriteConfig
) -> None:
    results = get_pypi_downloads_from_bigquery(query_config)
    if results is None:
        print("no results to write!")
        return

    parsed_results = parse_pypi_downloads(results)
    write_delta_table(
        parsed_results,
        write_config.table_dir,
        write_config.table_name,
        write_config.pks,
    )


def main():
    query_config = PyPIQueryConfig(
        repo_list=["quinn", "falsa", "levi", "tsumugi-spark"],
        min_date="2024-11-09",
        max_date="20224-11-09",
        dry_run=True,
    )

    write_config = DeltaWriteConfig(
        table_dir="bronze",
        table_name=PyPIDownload.__tablename__,  # pyright: ignore [reportArgumentType]
        pks=get_model_primary_key(PyPIDownload),
    )

    refresh_pypi_downloads_from_bigquery(query_config, write_config)


if __name__ == "__main__":
    main()
