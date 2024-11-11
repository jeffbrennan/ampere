import time
from dataclasses import dataclass
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

from ampere.common import (
    DeltaWriteConfig,
    get_model_primary_key,
    write_delta_table,
    get_current_time,
)
from ampere.models import PyPIDownload


@dataclass
class PyPIQueryConfig:
    repo_list: list[str]
    min_date: str
    max_date: Optional[str] = None
    dry_run: bool = True


def get_pypi_downloads_from_bigquery(config: PyPIQueryConfig) -> Optional[pd.DataFrame]:
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
    query_job = client.query_and_wait(cmd)
    start_time = time.time()
    results = query_job.to_dataframe()

    elapsed_time = time.time() - start_time
    print(f"query finished in {elapsed_time:.2f} seconds")
    return results


def parse_pypi_downloads(results: pd.DataFrame) -> list[PyPIDownload]:
    results["retrieved_at"] = get_current_time()
    parsed_results = [PyPIDownload(**row) for row in results.to_dict(orient="records")]
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
    load_dotenv()
    query_config = PyPIQueryConfig(
        repo_list=["quinn"],
        min_date="2017-09-15",
        max_date="2018-03-06",
        dry_run=False,
    )

    write_config = DeltaWriteConfig(
        table_dir="bronze",
        table_name=PyPIDownload.__tablename__,  # pyright: ignore [reportArgumentType]
        pks=get_model_primary_key(PyPIDownload),
    )

    refresh_pypi_downloads_from_bigquery(query_config, write_config)


if __name__ == "__main__":
    main()
