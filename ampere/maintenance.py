import datetime
import os
import time
from pathlib import Path

import duckdb

from ampere.common import get_backend_db_con, get_frontend_db_con


def create_new_frontend_db():
    base_dir = Path(__file__).parents[1] / "data"
    new_db_dir = base_dir / "frontend_versioned"
    new_db_dir.mkdir(exist_ok=True)

    today_str = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")

    new_db = new_db_dir / f"frontend_{today_str}.duckdb"
    symlink_db = base_dir / "frontend.duckdb"

    os.symlink(new_db, str(symlink_db) + "_tmp")
    os.rename(str(symlink_db) + "_tmp", symlink_db)
    print(f"{time.ctime()}: Symlink updated to point to {new_db}")


def write_backend_tables_to_frontend() -> None:
    tables = [
        "int_network_stargazers",
        "int_internal_followers",
        "int_network_follower_details",
        "stg_repos",
        "mart_downloads_summary",
        "mart_feed_events",
        "mart_issues",
        "mart_issues_summary",
        "mart_stargazers_pivoted",
        "mart_repo_summary",
    ]

    backend_con = get_backend_db_con()
    frontend_con = get_frontend_db_con(read_only=False)
    for table in tables:
        print(f"writing {table}...")
        df = backend_con.sql(f"SELECT * FROM {table}").to_df()
        duckdb.sql(f"CREATE TABLE {table} AS SELECT * FROM df", connection=frontend_con)


def cleanup_frontend_versions() -> None:
    max_num_versions =  5
    base_dir = Path(__file__).parents[1] / "data" / "frontend_versioned"

    all_versions = sorted(base_dir.glob("*.duckdb"))
    if len(all_versions) < max_num_versions:
        print("no versions to delete. exiting early")
        return
    
    versions_to_delete = all_versions[0:-max_num_versions]
    for version in versions_to_delete:
        print("deleting", version.as_posix())
        version.unlink()

def copy_backend_to_frontend() -> None:
    create_new_frontend_db()
    write_backend_tables_to_frontend()
    cleanup_frontend_versions()

if __name__ == "__main__":
    copy_backend_to_frontend()
