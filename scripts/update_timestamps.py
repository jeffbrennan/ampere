from pathlib import Path

import polars as pl

from ampere.common import (
    DeltaTableWriteMode,
    DeltaWriteConfig,
    get_model_primary_key,
    write_delta_table,
)
from ampere.models import PyPIDownload


def main(dry_run: bool = True):
    table_name = str(PyPIDownload.__tablename__)
    write_config = DeltaWriteConfig(
        table_dir="bronze",
        table_name=table_name,
        pks=get_model_primary_key(PyPIDownload),
    )
    tbl_dir = Path(__file__).parents[1] / "data" / "bronze"
    tbl_path = tbl_dir / f"{table_name}CLONE"
    updated_df = pl.scan_delta(str(tbl_path)).with_columns(
        pl.col("retrieved_at")
        .cast(pl.Datetime)
        .dt.replace_time_zone("UTC")
        .alias("retrieved_at")
    )
    if dry_run:
        print(updated_df.collect().head())
        return
    chunk_size = 5_000_000
    offset = 0
    while True:
        chunked_df = updated_df.slice(offset, chunk_size).collect()
        n_records = chunked_df.shape[0]
        if n_records == 0:
            break

        if offset == 0:
            mode = DeltaTableWriteMode.OVERWRITE_WITH_SCHEMA
        else:
            mode = DeltaTableWriteMode.APPEND

        write_delta_table(
            chunked_df,
            write_config.table_dir,
            write_config.table_name,
            write_config.pks,
            cleanup=False,
            mode=mode,
        )
        offset += chunk_size

    print("done")


if __name__ == "__main__":
    main(dry_run=False)
