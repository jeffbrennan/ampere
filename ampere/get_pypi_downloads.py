import datetime
import time
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

from ampere.common import (
    DeltaTableWriteMode,
    DeltaWriteConfig,
    get_backend_db_con,
    get_current_time,
    get_model_primary_key,
    write_delta_table,
)
from ampere.models import PyPIDownload, PyPIQueryConfig


def record_pypi_query(query: PyPIQueryConfig) -> None:
    config = DeltaWriteConfig(
        table_dir="bronze",
        table_name=PyPIQueryConfig.__tablename__,  # pyright: ignore [reportArgumentType]
        pks=get_model_primary_key(PyPIQueryConfig),
        mode=DeltaTableWriteMode.APPEND,
    )

    write_delta_table([query], config)


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
            count(*)                                    as download_count,
            current_timestamp()                         as retrieved_at
        from `bigquery-public-data.pypi.file_downloads`
        where 
            TIMESTAMP_TRUNC(timestamp, day) >= timestamp ('{config.min_date}') 
            {max_date_where} 
            and project = '{config.repo}'
        group by all
        """

    print(cmd)
    if dry_run:
        return None

    client = bigquery.Client()
    query_job = client.query_and_wait(cmd)
    start_time = time.time()
    results = query_job.to_dataframe()

    elapsed_time = time.time() - start_time
    print(f"query finished in {elapsed_time:.2f} seconds")

    return results


def refresh_pypi_downloads_from_bigquery(
    query_config: PyPIQueryConfig, write_config: DeltaWriteConfig, dry_run: bool = True
) -> int:
    results = get_pypi_downloads_from_bigquery(query_config, dry_run)
    if results is None:
        print("dry run - exiting early")
        return 0

    if len(results) == 0:
        print(
            f"0 downloads found for time period: {query_config.min_date} - {query_config.max_date}"
        )
        record_pypi_query(query_config)
        return 0
    write_delta_table(results, write_config)

    record_pypi_query(query_config)
    return len(results)


def get_pypi_download_query_dates() -> list[PyPIQueryConfig]:
    max_query_days = 45

    con = get_backend_db_con()

    query_dates = con.sql(
        """
        select repo, max(max_date) as data_copied_through_date
        from pypi_download_queries
        group by repo
        """
    ).fetchall()

    # inconsistent bigquery data usage when querying current day data
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    max_date = datetime.datetime.strftime(yesterday, "%Y-%m-%d")

    queries = []
    for record in query_dates:
        repo = record[0]
        min_date_str = record[1]
        min_date = datetime.datetime.strptime(min_date_str, "%Y-%m-%d")
        days_to_query = yesterday - min_date

        if days_to_query.days > max_query_days:
            raise ValueError(
                f"check `pypi_download_queries` table for {record[0]} - newest date: {min_date_str}"
            )

        if days_to_query.days < 1:
            print(f"{repo} has data through {min_date_str}. skipping")
            continue

        queries.append(
            PyPIQueryConfig(
                repo=repo,
                min_date=min_date_str,
                max_date=max_date,
                retrieved_at=get_current_time(),
            )
        )
    return queries


def get_repos_with_releases() -> list[str]:
    con = get_backend_db_con()
    records = con.sql(
        """
        with
            release_repos as (
                select distinct
                    repo_id
                from stg_releases
            ),
            repo_details as (
                select
                    a.repo_id,
                    a.repo_name,
                    unnest(a.language) as "language"
                from stg_repos               a
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


def get_backfill_queries(
    repo: str,
    min_date: datetime.datetime,
    max_date: Optional[datetime.datetime] = None,
    max_days_per_chunk: int = 15,
) -> list[PyPIQueryConfig]:
    max_date_final = max_date

    if max_date_final is None:
        n_days_to_fill = (get_current_time() - min_date).days
    else:
        n_days_to_fill = (max_date_final - min_date).days

    queries = []
    if n_days_to_fill < max_days_per_chunk:
        max_date = min_date + datetime.timedelta(days=n_days_to_fill)
        queries.append(
            PyPIQueryConfig(
                repo=repo,
                min_date=datetime.datetime.strftime(min_date, "%Y-%m-%d"),
                max_date=datetime.datetime.strftime(max_date, "%Y-%m-%d"),
                retrieved_at=get_current_time(),
            )
        )
        return queries

    chunks = n_days_to_fill // max_days_per_chunk + 1
    for _ in range(1, chunks):
        max_date = min_date + datetime.timedelta(days=max_days_per_chunk)
        queries.append(
            PyPIQueryConfig(
                repo=repo,
                min_date=datetime.datetime.strftime(min_date, "%Y-%m-%d"),
                max_date=datetime.datetime.strftime(max_date, "%Y-%m-%d"),
                retrieved_at=get_current_time(),
            )
        )
        min_date = max_date

    if max_date_final is not None:
        return queries

    yesterday = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)
    if max_date is not None and max_date > yesterday:
        return queries

    max_date = datetime.datetime.strftime(yesterday, "%Y-%m-%d")  # type: ignore
    queries.append(
        PyPIQueryConfig(
            repo=repo,
            min_date=datetime.datetime.strftime(min_date, "%Y-%m-%d"),
            max_date=max_date,  # type: ignore
            retrieved_at=get_current_time(),
        )
    )
    return queries


def add_backfill_to_table(
    repo: str,
    min_date: datetime.datetime,
    max_date: Optional[datetime.datetime],
    max_days_per_chunk: int,
    dry_run: bool,
):
    queries = get_backfill_queries(repo, min_date, max_date, max_days_per_chunk)

    print(f"backfilling {repo} {'-' * 20}")
    for query in queries:
        print(query.min_date, "->", query.max_date)

    refresh_all_pypi_downloads(queries, dry_run)


def refresh_all_pypi_downloads(
    queries: Optional[list[PyPIQueryConfig]] = None, dry_run: bool = True
) -> int:
    load_dotenv()

    write_config = DeltaWriteConfig(
        table_dir="bronze",
        table_name=PyPIDownload.__tablename__,  # pyright: ignore [reportArgumentType]
        pks=get_model_primary_key(PyPIDownload),
        mode=DeltaTableWriteMode.APPEND,  # less resource intensive than merge
    )

    if queries is None:
        queries = get_pypi_download_query_dates()

    if len(queries) == 0:
        print("nothing to write! exiting early")
        return 0

    records_added = 0
    for query in queries:
        records_added += refresh_pypi_downloads_from_bigquery(
            query, write_config, dry_run
        )
    return records_added


if __name__ == "__main__":
    refresh_all_pypi_downloads(dry_run=True)
