from pathlib import Path

import typer
from deltalake import DeltaTable

app = typer.Typer()


@app.command()
def delete(table_name: str, predicate: str, dry_run: bool = True) -> None:
    tbl_path = Path(__file__).parents[1] / "data" / "bronze" / table_name
    delta_table = DeltaTable(tbl_path)

    print(predicate)
    if dry_run:
        return
    delta_table.delete(predicate)


if __name__ == "__main__":
    app()
