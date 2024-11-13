from typing import Optional

from ampere.common import get_db_con
from ampere.get_pypi_downloads import add_backfill_to_table
import datetime

def backfill_repo(repo: str, min_date: Optional[datetime.datetime], repo_dependency: Optional[str], dry_run: bool):
    if min_date is None and repo_dependency is None:
        raise ValueError(
            "expecting either `min_date` or `repo_dependency` to be provided"
        )


    if min_date is None and repo_dependency is not None:
        con = get_db_con()
        min_date = con.sql(
            f"select created_at from repos where repo_name = '{repo_dependency}'"
        ).fetchall()[0][0]

    if min_date is None:
        raise ValueError("expecting min date to be set")
    add_backfill_to_table(repo, min_date, dry_run)


if __name__ == "__main__":
    backfill_repo("deltalake", None, 'levi', False)
