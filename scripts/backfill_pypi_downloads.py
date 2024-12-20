import datetime
from typing import Annotated, Optional

import typer

from ampere.common import get_db_con
from ampere.get_pypi_downloads import add_backfill_to_table

app = typer.Typer()


@app.command()
def backfill(
    repo: Annotated[str, typer.Option()],
    min_date: Optional[str] = None,
    repo_dependency: Optional[str] = None,
    max_days_per_chunk: int = 15,
    dry_run: bool = True,
):
    if min_date is None and repo_dependency is None:
        raise ValueError(
            "expecting either `min_date` or `repo_dependency` to be provided"
        )

    if repo_dependency is None:
        min_date_dt = datetime.datetime.strptime(min_date, "%Y-%m-%d")  # type: ignore
    else:
        con = get_db_con()
        min_date_dt = con.sql(
            f"select created_at from repos where repo_name = '{repo_dependency}'"
        ).fetchall()[0][0]

    add_backfill_to_table(repo, min_date_dt, max_days_per_chunk, dry_run)


if __name__ == "__main__":
    app()
