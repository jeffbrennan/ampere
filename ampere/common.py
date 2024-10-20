import datetime
import os
import time
from dataclasses import dataclass
from enum import StrEnum
from functools import wraps
from pathlib import Path
from typing import Callable, Optional

import dotenv
import duckdb
import pandas as pd
from deltalake import DeltaTable, write_deltalake
from duckdb import DuckDBPyConnection
from sqlmodel.main import SQLModelMetaclass

from ampere.models import SQLModelType


@dataclass
class RefreshConfig:
    model: SQLModelMetaclass
    get_func: Callable


@dataclass
class DeltaWriteConfig:
    table_dir: str
    table_name: str
    pks: list[str]


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


def get_token(secret_name: str) -> str:
    dotenv.load_dotenv()
    token = os.environ.get(secret_name)
    if token is None:
        raise ValueError()
    return token


def get_current_time() -> datetime.datetime:
    current_time = datetime.datetime.now()
    return datetime.datetime.strptime(
        current_time.isoformat(timespec="seconds"), "%Y-%m-%dT%H:%M:%S"
    )


def write_delta_table(
    records: list[SQLModelType], table_dir: str, table_name: str, pks: list[str]
) -> None:
    data_dir = Path(__file__).parents[1] / "data" / table_dir
    table_path = data_dir / table_name

    df = pd.DataFrame.from_records([i.model_dump() for i in records])
    delta_log_dir = table_path / "_delta_log"
    print(f"writing {len(records)} to {table_path}...")
    if not delta_log_dir.exists():
        table_path.mkdir(exist_ok=True, parents=True)
        write_deltalake(table_path, df, mode="error")
        return

    delta_table = DeltaTable(table_path)
    predicate_str = " and ".join([f"s.{i} = t.{i}" for i in pks])
    merge_results = (
        delta_table.merge(
            df,
            predicate=predicate_str,
            source_alias="s",
            target_alias="t",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .when_not_matched_by_source_delete()
        .execute()
    )
    print(merge_results)


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


def get_db_con() -> DuckDBPyConnection:
    db_path = Path(__file__).parents[1] / "data" / "ampere.duckdb"
    return duckdb.connect(str(db_path))


def timeit(func):
    # https://dev.to/kcdchennai/python-decorator-to-measure-execution-time-54hk
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f"Function {func.__name__} Took {total_time:.2f} seconds")
        return result

    return timeit_wrapper
