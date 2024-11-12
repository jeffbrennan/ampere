import datetime
import time
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

from ampere.common import (
    DeltaWriteConfig,
    get_model_primary_key,
    write_delta_table,
    get_current_time,
    get_db_con,
)
from ampere.models import PyPIDownload, PyPIQueryConfig


def format_list_sql_query(input_list: list[str]) -> str:
    return "'" + "', '".join(input_list) + "'"


def record_pypi_query(query: PyPIQueryConfig) -> None:
    config = DeltaWriteConfig(
        table_dir="bronze",
        table_name=PyPIQueryConfig.__tablename__,  # pyright: ignore [reportArgumentType]
        pks=get_model_primary_key(PyPIQueryConfig),
    )

    write_delta_table(
        [query],
        config.table_dir,
        config.table_name,
        config.pks,
    )


def get_pypi_downloads_from_bigquery(
    config: PyPIQueryConfig, dry_run: bool = True
) -> Optional[pd.DataFrame]:
    max_date_where = ""
    if config.max_date is not None:
        max_date_where = (
            f"and TIMESTAMP_TRUNC(timestamp, day) <= timestamp ('{config.max_date}')"
        )

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
            and project = '{config.repo}'
        group by all
        """

    print(cmd)
    if dry_run:
        return

    client = bigquery.Client()
    query_job = client.query_and_wait(cmd)
    start_time = time.time()
    results = query_job.to_dataframe()

    elapsed_time = time.time() - start_time
    print(f"query finished in {elapsed_time:.2f} seconds")

    record_pypi_query(config)
    return results


def parse_pypi_downloads(results: pd.DataFrame) -> list[PyPIDownload]:
    results["retrieved_at"] = get_current_time()
    parsed_results = [PyPIDownload(**row) for row in results.to_dict(orient="records")]
    return parsed_results


def refresh_pypi_downloads_from_bigquery(
    query_config: PyPIQueryConfig, write_config: DeltaWriteConfig, dry_run: bool = True
) -> None:
    results = get_pypi_downloads_from_bigquery(query_config, dry_run)
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


def get_pypi_download_query_dates() -> list[PyPIQueryConfig]:
    max_query_days = 30

    con = get_db_con()

    # preferred data source when possible
    query_dates = con.sql(
        """
        select repo, max(min_date) as min_date
        from pypi_download_queries
        group by repo
        """
    ).fetchall()

    # inconsistent bigquery data usage when querying current day data
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    max_date = datetime.datetime.strftime(yesterday, "%Y-%m-%d")

    queries = []
    for record in query_dates:
        min_date_str = record[1]
        min_date = datetime.datetime.strptime(min_date_str, "%Y-%m-%d")
        days_to_query = yesterday - min_date
        if days_to_query.days > max_query_days:
            raise ValueError(
                f"check `pypi_download_queries` table for {record[0]} - newest date: {min_date_str}"
            )

        queries.append(
            PyPIQueryConfig(
                repo=record[0],
                min_date=min_date_str,
                max_date=max_date,
                retrieved_at=get_current_time(),
            )
        )
    return queries


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
    print(records)

    return [i[0] for i in records]


def main():
    load_dotenv()

    write_config = DeltaWriteConfig(
        table_dir="bronze",
        table_name=PyPIDownload.__tablename__,  # pyright: ignore [reportArgumentType]
        pks=get_model_primary_key(PyPIDownload),
    )

    queries = get_pypi_download_query_dates()
    for query in queries:
        refresh_pypi_downloads_from_bigquery(query, write_config, False)


if __name__ == "__main__":
    main()
