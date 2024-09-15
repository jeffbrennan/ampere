import datetime
import os
from pathlib import Path

import dotenv
import pandas as pd
from deltalake import write_deltalake, DeltaTable

from ampere.models import SQLModelType


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
    return datetime.datetime.strptime(current_time.isoformat(timespec='seconds'), "%Y-%m-%dT%H:%M:%S")


def write_delta_table(records: list[SQLModelType], table_dir: str, table_name: str, pk: str) -> None:
    data_dir = Path(__file__).parents[1] / "data" / table_dir
    table_path = data_dir / table_name

    df = pd.DataFrame.from_records([vars(i) for i in records])
    delta_log_dir = table_path / "_delta_log"
    print(f"writing {len(records)} to {table_path}...")
    if not delta_log_dir.exists():
        table_path.mkdir(exist_ok=True, parents=True)
        write_deltalake(table_path, df, mode="error")
        return

    delta_table = DeltaTable(table_path)
    merge_results = (
        delta_table
        .merge(df,
               predicate=f"s.{pk} = t.{pk}",
               source_alias="s",
               target_alias="t", )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .when_not_matched_by_source_delete()
        .execute()
    )
    print(merge_results)
