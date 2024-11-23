import json
from pathlib import Path

from deltalake import DeltaTable

from ampere.common import create_header


def vacuum_delta_table(table_path: Path, retention_hours: int = 14 * 24) -> None:
    print("vacuuming...")
    delta_table = DeltaTable(str(table_path))
    results = delta_table.vacuum(14 * 24)
    print("vacuumed", len(results), "files")


def optimize_delta_table(table_path: Path) -> None:
    print("optimizing...")
    delta_table = DeltaTable(str(table_path))
    results = delta_table.optimize.compact()
    print("files added: ", results["numFilesAdded"])
    print("files removed: ", results["numFilesRemoved"])

    if results["numFilesAdded"] > 0:
        size_added = json.loads(results["filesAdded"])["totalSize"]
        size_added_mb = size_added / 1000 / 1000
        print(f"file size (mb) {size_added_mb:.02f}")


def get_delta_tables(base_path: Path) -> list[Path]:
    return [x for x in base_path.iterdir() if x.is_dir()]


def cleanup_delta_table(table_path: Path, retention_hours: int = 14 * 24) -> None:
    optimize_delta_table(table_path)
    vacuum_delta_table(table_path, retention_hours)


def cleanup_delta_tables():
    delta_base_path = Path(__file__).parents[1] / "data" / "bronze"
    all_delta_tables = get_delta_tables(delta_base_path)
    print(create_header(80, "AMPERE MAINTENANCE", True, "="))
    for delta_table_path in all_delta_tables:
        print(create_header(80, delta_table_path.stem, False, "-"))

        optimize_delta_table(delta_table_path)
        vacuum_delta_table(delta_table_path)


if __name__ == "__main__":
    cleanup_delta_tables()
