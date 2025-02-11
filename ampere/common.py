import datetime
import json
import os
import time
from dataclasses import dataclass
from enum import StrEnum, auto
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Optional, TypeVar

import duckdb
import pandas as pd
import polars as pl
from deltalake import DeltaTable, write_deltalake
from duckdb import DuckDBPyConnection
from sqlmodel import SQLModel
from sqlmodel.main import SQLModelMetaclass

SQLModelType = TypeVar("SQLModelType", bound=SQLModel)


@dataclass
class RefreshConfig:
    model: SQLModelMetaclass
    get_func: Callable


class DeltaTableWriteMode(StrEnum):
    MERGE = auto()
    APPEND = auto()
    OVERWRITE = auto()
    OVERWRITE_WITH_SCHEMA = auto()


@dataclass
class DeltaWriteConfig:
    table_dir: str
    table_name: str
    pks: list[str]
    mode: DeltaTableWriteMode


def format_list_sql_query(input_list: list[str]) -> str:
    return "'" + "', '".join(input_list) + "'"


def create_header(header_length: int, title: str, center: bool, spacer: str):
    if center:
        spacer_len = (header_length - len(title)) // 2
        output = f"{spacer * spacer_len}{title}{spacer * spacer_len}"
    else:
        output = f"{title}{spacer * (header_length - len(title))}"

    if len(output) < header_length:
        output += spacer * (header_length - len(output))
    if len(output) > header_length:
        output = spacer * header_length + "\n" + output

    return output


def get_secret(secret_name: str) -> str:
    import dotenv

    dotenv.load_dotenv()
    secret = os.environ.get(secret_name)
    if secret is None:
        raise ValueError()
    return secret


def get_current_time() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def write_delta_table(
    records: list[SQLModelType] | pd.DataFrame | pl.DataFrame,
    config: DeltaWriteConfig,
    cleanup: bool = True,
) -> None:
    data_dir = Path(__file__).parents[1] / "data" / config.table_dir
    table_path = data_dir / config.table_name
    if isinstance(records, pd.DataFrame):
        df = records
    elif isinstance(records, pl.DataFrame):
        df = records.to_pandas(use_pyarrow_extension_array=True)
    else:
        df = pd.DataFrame.from_records([i.model_dump() for i in records])

    delta_log_dir = table_path / "_delta_log"
    print(f"writing {len(records)} to {table_path}...")
    if not delta_log_dir.exists():
        table_path.mkdir(exist_ok=True, parents=True)
        write_deltalake(table_path, df, mode="error")
        return

    if config.mode == DeltaTableWriteMode.APPEND:
        write_deltalake(table_path, df, mode="append")
        print("append complete")
        return

    elif config.mode == DeltaTableWriteMode.OVERWRITE:
        write_deltalake(table_path, df, mode="overwrite")
        print("overwrite complete")
        return

    elif config.mode == DeltaTableWriteMode.OVERWRITE_WITH_SCHEMA:
        write_deltalake(table_path, df, mode="overwrite", schema_mode="overwrite")
        print("overwrite (including table schema) complete")
        return

    delta_table = DeltaTable(table_path)
    predicate_str = " and ".join([f"s.{i} = t.{i}" for i in config.pks])
    merge_results = (
        delta_table.merge(
            df,
            predicate=predicate_str,
            source_alias="s",
            target_alias="t",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )
    print(merge_results)

    if cleanup:
        cleanup_delta_table(table_path)


def get_model_primary_key(model: SQLModelMetaclass) -> list[str]:
    pks = []
    for k, v in model.model_fields.items():
        if hasattr(v, "primary_key"):
            pks.append(k)

    return pks


def get_model_foreign_key(model: SQLModelMetaclass, fk_name: str) -> Optional[str]:
    for k, v in model.model_fields.items():
        if not hasattr(v, "foreign_key"):
            continue
        if v.foreign_key == fk_name:
            return k


def get_frontend_db_con(read_only: bool = True) -> DuckDBPyConnection:
    db_path = Path(__file__).parents[1] / "data" / "frontend.duckdb"
    return duckdb.connect(
        str(db_path),
        read_only=read_only,
        config={"TimeZone": "UTC"},
    )


def get_backend_db_con(read_only: bool = True) -> DuckDBPyConnection:
    db_path = Path(__file__).parents[1] / "data" / "backend.duckdb"
    return duckdb.connect(
        str(db_path),
        read_only=read_only,
        config={"TimeZone": "UTC"},
    )


def timeit(func):
    # https://dev.to/kcdchennai/python-decorator-to-measure-execution-time-54hk
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(
            f"{get_current_time()} -- Function {func.__name__} Took {total_time * 1000:.2f} ms"
        )
        return result

    return timeit_wrapper


def vacuum_delta_table(table_path: Path, retention_hours: int = 14 * 24) -> None:
    print("vacuuming...")
    delta_table = DeltaTable(str(table_path))
    results = delta_table.vacuum(retention_hours)
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


def divide_chunks(list_to_chunk: list[Any], n: int):
    # https://stackoverflow.com/a/48135727
    for i in range(0, len(list_to_chunk), n):
        yield list_to_chunk[i : i + n]
