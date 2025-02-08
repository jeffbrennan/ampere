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
        "int_downloads_melted",
        "int_downloads_melted_daily",
        "int_downloads_melted_weekly",
        "int_downloads_melted_monthly",
        "int_network_stargazers",
        "int_internal_followers",
        "int_network_follower_details",
        "int_status_details",
        "stg_repos",
        "mart_downloads_summary",
        "mart_feed_events",
        "mart_issues",
        "mart_issues_summary",
        "mart_stargazers_pivoted",
        "mart_repo_summary",
        "mart_status_details",
    ]

    backend_con = get_backend_db_con()
    frontend_con = get_frontend_db_con(read_only=False)
    for table in tables:
        offset = 0
        batch_size = 200_000
        first_batch = True
        while True:
            print(f"writing {table} records {offset:,} to {(offset + batch_size):,}...")

            df = backend_con.sql(
                f"SELECT * FROM {table} LIMIT {batch_size} OFFSET {offset}"
            ).to_df()

            if df.empty:
                break

            if first_batch:
                duckdb.sql(
                    f"CREATE TABLE {table} AS SELECT * FROM df", connection=frontend_con
                )
                first_batch = False
            else:
                duckdb.sql(
                    f"INSERT INTO {table} SELECT * FROM df", connection=frontend_con
                )
            offset += batch_size


def write_backend_views_to_frontend() -> None:
    backend_con = get_backend_db_con()
    frontend_con = get_frontend_db_con(read_only=False)

    views_to_copy = [
        "int_status_summary",
        "int_status_summary_pivoted",
        "mart_status_summary",
    ]

    views_to_copy_sql = "'" + "', '".join(views_to_copy) + "'"
    views = backend_con.sql(f"""
        select table_name, view_definition
        from information_schema.views
        where table_schema = 'main'
        and table_name in ({views_to_copy_sql})
    """).fetchall()

    for view_name, view_definition in views:
        view_definition_clean = view_definition.replace("backend", "frontend")
        print(f"creating view {view_name}...")
        frontend_con.sql(f"{view_definition_clean}")


@timeit
def copy_backend_to_frontend() -> None:
    create_new_frontend_db()
    write_backend_tables_to_frontend()
    write_backend_views_to_frontend()


if __name__ == "__main__":
    copy_backend_to_frontend()
