from pathlib import Path

import duckdb

from ampere.common import get_backend_db_con, get_frontend_db_con, timeit


def create_new_frontend_db():
    base_dir = Path(__file__).parents[1] / "data"
    db_path = base_dir / "frontend.duckdb"

    print("deleting existing file...")
    db_path.unlink(missing_ok=True)

    print("creating new file...")
    get_frontend_db_con(read_only=False)


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


@timeit
def copy_backend_to_frontend() -> None:
    create_new_frontend_db()
    write_backend_tables_to_frontend()


if __name__ == "__main__":
    copy_backend_to_frontend()
