from ampere.common import get_db_con
from ampere.get_pypi_downloads import add_backfill_to_table


def backfill_pyspark():
    con = get_db_con()
    min_date = con.sql(
        "select created_at from repos where repo_name = 'quinn'"
    ).fetchall()[0][0]
    print(min_date)
    add_backfill_to_table("pyspark", min_date, True)


if __name__ == "__main__":
    backfill_pyspark()
