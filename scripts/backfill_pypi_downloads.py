import datetime
from typing import Annotated, Optional

import typer

from ampere.common import get_backend_db_con
from ampere.get_pypi_downloads import add_backfill_to_table

app = typer.Typer()


@app.command()
def backfill(
    repo: Annotated[str, typer.Option()],
    min_date: Optional[str] = None,
    max_date: Optional[str] = None,
    repo_dependency: Optional[str] = None,
    max_days_per_chunk: int = 15,
    dry_run: bool = True,
) -> None:
    if min_date is None and repo_dependency is None:
        raise ValueError(
            "expecting either `min_date` or `repo_dependency` to be provided"
        )

    if min_date is not None:
        min_date_dt = datetime.datetime.strptime(min_date, "%Y-%m-%d").replace(
            tzinfo=datetime.timezone.utc
        )
    else:
        con = get_backend_db_con()
        min_date_dt = con.sql(
            f"select created_at from stg_repos where repo_name = '{repo_dependency}'"
        ).fetchall()[0][0]

    if max_date is not None:
        max_date_dt = datetime.datetime.strptime(max_date, "%Y-%m-%d").replace(
            tzinfo=datetime.timezone.utc
        )
    else:
        max_date_dt = None

    add_backfill_to_table(
        repo,
        min_date_dt,
        max_date_dt,
        max_days_per_chunk,
        dry_run,
    )


if __name__ == "__main__":
    app()
