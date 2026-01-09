import os
import uuid
import polars as pl
from pathlib import Path
from .logger import logger
from .models import CONSTS


def is_empty(lf: pl.LazyFrame) -> bool:
    return lf.limit(1).collect().height == 0


def update_flag_5min(
    hist_path: Path | str = CONSTS.update_flag_5min.hist_path,
    daily_path: Path | str = CONSTS.update_flag_5min.daily_path_flag,
    no_referensi_col: str = CONSTS.update_flag_5min.no_referensi,
    flag_col: str = CONSTS.update_flag_5min.flag_col,
) -> None:
    """
    Updating the historical TF online 5 min data by adding daily flagged transaction.

    Params:
    ------
    n_days: int
        Total number of days to look back for the flagged TF Online data.
    hist_path: Path | str
        Path where the historical TF Online data stored.
    daily_path: Path | str
        Path where the daily flagged TF Online data stored.
    no_referensi_col: str
        Column of no_referensi to use.
    flag_col: str
        Column of flag to use.

    Returns:
    -------
    None
    """
    hist_path = Path(hist_path)
    daily_path = Path(daily_path)
    hist_path.parent.mkdir(parents=True, exist_ok=True)
    os.makedirs(os.path.dirname(hist_path), exist_ok=True)
    tmp_path = f"{hist_path}.{uuid.uuid4().hex}.tmp"

    logger.info(f"Reading daily flag TF Online data from {daily_path}...")

    if daily_path.exists():
        try:
            daily_flag_df = pl.read_parquet(daily_path).lazy()
            logger.info("Daily flag TF online data read succeeded.")
        except Exception as e:
            logger.error(f"Error reading parquet data : {e}")
            logger.error(
                "Update TF online flag to Historical Data Process terminated.")
            return
    else:
        logger.info("There is no daily flag TF online data found.")
        logger.info("Update Flag to Historical data finished.")
        return

    if is_empty(daily_flag_df):
        logger.info("There is no daily flag TF online data found.")
        logger.info("Update flag process finished.")
        return

    # Check if Historical Data exist
    if hist_path.exists():
        logger.info(f"Reading historical data from {hist_path}...")
        existing_df = pl.read_parquet(hist_path).lazy()
        # Ensure flag column is Int8
        existing_df = existing_df.with_columns(
            [pl.col(c).cast(pl.Int8) for c in flag_col]
        )
        logger.info("Historical data read successfully.")

        new_flag_refs = (
            daily_flag_df
            .select(no_referensi_col)
            .collect()
            .to_series()
            .to_list()
        )
        # Check if new_refs already updated as flag 1 in historical
        has_unflagged = (
            existing_df
            .filter(
                (pl.col(no_referensi_col).is_in(new_flag_refs)) &
                (pl.col(flag_col) == 0)
            )
            .select(pl.len())
            .collect()[0, 0] > 0
        )
        # Append only if there are new records
        if has_unflagged:
            delta_df = (
                existing_df
                .filter(
                    (pl.col(no_referensi_col).is_in(new_flag_refs)) &
                    (pl.col(flag_col) == 0)
                )
            )
            logger.info("New records found in daily flag TF Online data.")
            logger.info(
                f"Flagging {delta_df.select(pl.len()).collect()[0, 0]} new records to historical TF online data..")

            # Updating flagged records colunm 'flag' to 1
            updated_exist_df = existing_df.with_columns(
                pl.when(pl.col(no_referensi_col).is_in(new_flag_refs))
                .then(pl.lit(1, dtype=pl.Int8))
                .otherwise(pl.col(flag_col))
                .alias(flag_col)
            )
            updated_exist_df.collect(streaming=False).write_parquet(
                tmp_path,
                compression="zstd",
                statistics=True,
                use_pyarrow=True
            )
            # Atomic replace (POSIX-safe)
            os.replace(tmp_path, hist_path)
            logger.info("Historical parquet data updated successfully.")
        else:
            logger.info("There is no new flag TF Online found.")
            logger.info("Flagged records have been added previously.")
            logger.info("Updating flag record finished.")
            return
    else:
        logger.error("Historical data to update not found.")
        logger.error("Updating flag into historical data terminated.")
        return
    logger.info("Process completed.")


def update_flag_10min(
    hist_path: Path | str = CONSTS.update_flag_10min.hist_path,
    daily_path: Path | str = CONSTS.update_flag_10min.daily_path_flag,
    no_referensi_col: str = CONSTS.update_flag_10min.no_referensi,
    flag_col: str = CONSTS.update_flag_10min.flag_col,
) -> None:
    """
    Updating the historical TF online 5 min data by adding daily flagged transaction.

    Params:
    ------
    n_days: int
        Total number of days to look back for the flagged TF Online data.
    hist_path: Path | str
        Path where the historical TF Online data stored.
    daily_path: Path | str
        Path where the daily flagged TF Online data stored.
    no_referensi_col: str
        Column of no_referensi to use.
    flag_col: str
        Column of flag to use.

    Returns:
    -------
    None
    """
    hist_path = Path(hist_path)
    daily_path = Path(daily_path)
    hist_path.parent.mkdir(parents=True, exist_ok=True)
    os.makedirs(os.path.dirname(hist_path), exist_ok=True)
    tmp_path = f"{hist_path}.{uuid.uuid4().hex}.tmp"

    logger.info(f"Reading daily flag TF Online data from {daily_path}...")

    if daily_path.exists():
        try:
            daily_flag_df = pl.read_parquet(daily_path).lazy()
            logger.info("Daily flag TF online data read succeeded.")
        except Exception as e:
            logger.error(f"Error reading parquet data : {e}")
            logger.error(
                "Update TF online flag to Historical Data Process terminated.")
            return
    else:
        logger.info("There is no daily flag TF online data found.")
        logger.info("Update Flag to Historical data finished.")
        return

    if is_empty(daily_flag_df):
        logger.info("There is no daily flag TF online data found.")
        logger.info("Update flag process finished.")
        return

    # Check if Historical Data exist
    if hist_path.exists():
        logger.info(f"Reading historical data from {hist_path}...")
        existing_df = pl.read_parquet(hist_path).lazy()
        # Ensure flag column is Int8
        existing_df = existing_df.with_columns(
            [pl.col(c).cast(pl.Int8) for c in flag_col]
        )
        logger.info("Historical data read successfully.")

        new_flag_refs = (
            daily_flag_df
            .select(no_referensi_col)
            .collect()
            .to_series()
            .to_list()
        )
        # Check if new_refs already updated as flag 1 in historical
        has_unflagged = (
            existing_df
            .filter((pl.col(no_referensi_col).is_in(new_flag_refs)) &
                    (pl.col(flag_col) == 0)
                    )
            .select(pl.len())
            .collect()[0, 0] > 0
        )
        # Append only if there are new records
        if has_unflagged:
            delta_df = (
                existing_df
                .filter(
                    (pl.col(no_referensi_col).is_in(new_flag_refs)) &
                    (pl.col(flag_col) == 0)
                )
            )
            logger.info("New records found in daily flag TF Online data.")
            logger.info(
                f"Flagging {delta_df.select(pl.len()).collect()[0, 0]} new records to historical TF online data..")

            # Updating flagged records colunm 'flag' to 1
            updated_exist_df = existing_df.with_columns(
                pl.when(pl.col(no_referensi_col).is_in(new_flag_refs))
                .then(pl.lit(1, dtype=pl.Int8))
                .otherwise(pl.col(flag_col))
                .alias(flag_col)
            )

            updated_exist_df.collect(streaming=False).write_parquet(
                tmp_path,
                compression="zstd",
                statistics=True,
                use_pyarrow=True
            )
            # Atomic replace (POSIX-safe)
            os.replace(tmp_path, hist_path)
            logger.info("Historical parquet data updated successfully.")
        else:
            logger.info("There is no new flag TF Online found.")
            logger.info("Flagged records have been added previously.")
            logger.info("Updating flag record finished.")
            return
    else:
        logger.error("Historical data to update not found.")
        logger.error("Updating flag into historical data terminated.")
        return
    logger.info("Process completed.")


def update_flag_50mio_e(
    hist_path: Path | str = CONSTS.update_flag_50mio_early.hist_path,
    daily_path: Path | str = CONSTS.update_flag_50mio_early.daily_path_flag,
    no_referensi_col: str = CONSTS.update_flag_50mio_early.no_referensi,
    flag_col: str = CONSTS.update_flag_50mio_early.flag_col,
) -> None:
    """
    Updating the historical TF online 5mio early data by adding daily flagged transaction.

    Params:
    ------
    n_days: int
        Total number of days to look back for the flagged TF Online data.
    hist_path: Path | str
        Path where the historical TF Online data stored.
    daily_path: Path | str
        Path where the daily flagged TF Online data stored.
    no_referensi_col: str
        Column of no_referensi to use.
    flag_col: str
        Column of flag to use.

    Returns:
    -------
    None
    """
    hist_path = Path(hist_path)
    daily_path = Path(daily_path)
    hist_path.parent.mkdir(parents=True, exist_ok=True)
    os.makedirs(os.path.dirname(hist_path), exist_ok=True)
    tmp_path = f"{hist_path}.{uuid.uuid4().hex}.tmp"

    logger.info(f"Reading daily flag TF Online data from {daily_path}...")

    if daily_path.exists():
        try:
            daily_flag_df = pl.read_parquet(daily_path).lazy()
            logger.info("Daily flag TF online data read succeeded.")
        except Exception as e:
            logger.error(f"Error reading parquet data : {e}")
            logger.error(
                "Update TF online flag to Historical Data Process terminated.")
            return
    else:
        logger.info("There is no daily flag TF online data found.")
        logger.info("Update Flag to Historical data finished.")
        return

    if is_empty(daily_flag_df):
        logger.info("There is no daily flag TF online data found.")
        logger.info("Update flag process finished.")
        return

    # Check if Historical Data exist
    if hist_path.exists():
        logger.info(f"Reading historical data from {hist_path}...")
        existing_df = pl.read_parquet(hist_path).lazy()
        # Ensure flag column is Int8
        existing_df = existing_df.with_columns(
            [pl.col(c).cast(pl.Int8) for c in flag_col]
        )
        logger.info("Historical data read successfully.")

        new_flag_refs = (
            daily_flag_df
            .select(no_referensi_col)
            .collect()
            .to_series()
            .to_list()
        )

        # Check if new_refs already updated as flag 1 in historical
        has_unflagged = (
            existing_df
            .filter(
                (pl.col(no_referensi_col).is_in(new_flag_refs)) &
                (pl.col(flag_col) == 0)
            )
            .select(pl.len())
            .collect()[0, 0] > 0
        )
        # Append only if there are new records
        if has_unflagged:
            delta_df = (
                existing_df
                .filter(
                    (pl.col(no_referensi_col).is_in(new_flag_refs)) &
                    (pl.col(flag_col) == 0)
                )
            )
            logger.info("New records found in daily flag TF Online data.")
            logger.info(
                f"Flagging {delta_df.select(pl.len()).collect()[0, 0]} new records to historical TF online data..")

            # Updating flagged records colunm 'flag' to 1
            updated_exist_df = existing_df.with_columns(
                pl.when(pl.col(no_referensi_col).is_in(new_flag_refs))
                .then(pl.lit(1, dtype=pl.Int8))
                .otherwise(pl.col(flag_col))
                .alias(flag_col)
            )

            updated_exist_df.collect(streaming=False).write_parquet(
                tmp_path,
                compression="zstd",
                statistics=True,
                use_pyarrow=True
            )
            # Atomic replace (POSIX-safe)
            os.replace(tmp_path, hist_path)
            logger.info("Historical parquet data updated successfully.")
        else:
            logger.info("There is no new flag TF Online found.")
            logger.info("Flagged records have been added previously.")
            logger.info("Updating flag record finished.")
            return
    else:
        logger.error("Historical data to update not found.")
        logger.error("Updating flag into historical data terminated.")
        return
    logger.info("Process completed.")


def update_flag_50mio(
    hist_path: Path | str = CONSTS.update_flag_50mio.hist_path,
    daily_path: Path | str = CONSTS.update_flag_50mio.daily_path_flag,
    no_referensi_col: str = CONSTS.update_flag_50mio.no_referensi,
    flag_col: str = CONSTS.update_flag_50mio.flag_col,
) -> None:
    """
    Updating the historical TF online 5mio data by adding daily flagged transaction.

    Params:
    ------
    n_days: int
        Total number of days to look back for the flagged TF Online data.
    hist_path: Path | str
        Path where the historical TF Online data stored.
    daily_path: Path | str
        Path where the daily flagged TF Online data stored.
    no_referensi_col: str
        Column of no_referensi to use.
    flag_col: str
        Column of flag to use.

    Returns:
    -------
    None
    """
    hist_path = Path(hist_path)
    daily_path = Path(daily_path)
    hist_path.parent.mkdir(parents=True, exist_ok=True)
    os.makedirs(os.path.dirname(hist_path), exist_ok=True)
    tmp_path = f"{hist_path}.{uuid.uuid4().hex}.tmp"

    logger.info(f"Reading daily flag TF Online data from {daily_path}...")

    if daily_path.exists():
        try:
            daily_flag_df = pl.read_parquet(daily_path).lazy()
            logger.info("Daily flag TF online data read succeeded.")
        except Exception as e:
            logger.error(f"Error reading parquet data : {e}")
            logger.error(
                "Update TF online flag to Historical Data Process terminated.")
            return
    else:
        logger.info("There is no daily flag TF online data found.")
        logger.info("Update Flag to Historical data finished.")
        return

    if is_empty(daily_flag_df):
        logger.info("There is no daily flag TF online data found.")
        logger.info("Update flag process finished.")
        return

    # Check if Historical Data exist
    if hist_path.exists():
        logger.info(f"Reading historical data from {hist_path}...")
        existing_df = pl.read_parquet(hist_path).lazy()
        # Ensure flag column is Int8
        existing_df = existing_df.with_columns(
            [pl.col(c).cast(pl.Int8) for c in flag_col]
        )
        logger.info("Historical data read successfully.")

        new_flag_refs = (
            daily_flag_df
            .select(no_referensi_col)
            .collect()
            .to_series()
            .to_list()
        )

        # Check if new_refs already updated as flag 1 in historical
        has_unflagged = (
            existing_df.filter(
                (pl.col(no_referensi_col).is_in(new_flag_refs)) &
                (pl.col(flag_col) == 0)
            )
            .select(pl.len())
            .collect()[0, 0] > 0
        )
        # Append only if there are new records
        if has_unflagged:
            delta_df = (
                existing_df
                .filter(
                    (pl.col(no_referensi_col).is_in(new_flag_refs)) &
                    (pl.col(flag_col) == 0)
                )
            )
            logger.info("New records found in daily flag TF Online data.")
            logger.info(
                f"Flagging {delta_df.select(pl.len()).collect()[0, 0]} new records to historical TF online data..")

            # Updating flagged records colunm 'flag' to 1
            updated_exist_df = existing_df.with_columns(
                pl.when(pl.col(no_referensi_col).is_in(new_flag_refs))
                .then(pl.lit(1, dtype=pl.Int8))
                .otherwise(pl.col(flag_col))
                .alias(flag_col)
            )

            updated_exist_df.collect(streaming=False).write_parquet(
                tmp_path,
                compression="zstd",
                statistics=True,
                use_pyarrow=True
            )
            # Atomic replace (POSIX-safe)
            os.replace(tmp_path, hist_path)
            logger.info("Historical parquet data updated successfully.")
        else:
            logger.info("There is no new flag TF Online found.")
            logger.info("Flagged records have been added previously.")
            logger.info("Updating flag record finished.")
            return
    else:
        logger.error("Historical data to update not found.")
        logger.error("Updating flag into historical data terminated.")
        return
    logger.info("Process completed.")
