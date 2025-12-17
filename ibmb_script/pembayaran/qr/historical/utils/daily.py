import polars as pl
from pathlib import Path
from .logger import logger
from .models import CONSTS


def is_empty(lf: pl.LazyFrame) -> bool:
    return lf.limit(1).collect().height == 0


def update_flag_5mio(
    history_path: Path | str = CONSTS.update_flag_5mio.hist_path,
    daily_path: Path | str = CONSTS.update_flag_5mio.daily_path_flag,
    no_referensi_col: str = CONSTS.update_flag_5mio.no_referensi,
    flag_col: str = CONSTS.update_flag_5mio.flag_col,
) -> None:
    """
    Updating the historical pembayaran QR 5 min data by adding daily flagged transaction.

    Params:
    ------
    n_days: int
        Total number of days to look back for the flagged pembayaran QR data.
    history_path: Path | str
        Path where the historical pembayaran QR data stored.
    daily_path: Path | str
        Path where the daily flagged pembayaran QR data stored.
    no_referensi_col: str
        Column of no_referensi to use.
    flag_col: str
        Column of flag to use.

    Returns:
    -------
    None
    """
    history_path = Path(history_path)
    daily_path = Path(daily_path)
    history_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(
        f"Reading daily flag pembayaran QR data from {history_path}...")

    if daily_path.exists():
        try:
            daily_flag_df = pl.scan_parquet(daily_path)
            logger.info("Daily flag pembayaran QR data read succeeded.")
        except Exception as e:
            logger.error(f"Error reading parquet data : {e}")
            logger.error(
                "Update pembayaran QR flag to Historical Data Process terminated.")
            return
    else:
        logger.info("There is no daily flag pembayaran QR data found.")
        logger.info("Update Flag to Historical data finished.")
        return

    if is_empty(daily_flag_df):
        logger.info("There is no daily flag pembayaran QR data found.")
        logger.info("Update flag process finished.")
        return

    # Check if Historical Data exist
    if history_path.exists():
        logger.info(f"Reading historical data from {history_path}...")
        existing_df = pl.scan_parquet(history_path)
        logger.info("Historical data read successfully.")

        new_flag_refs = (daily_flag_df
                         .select(no_referensi_col)
                         .collect()
                         .to_series()
                         .to_list()
                         )

        # Check if new_refs already updated as flag 1 in historical
        is_new_refs_exist = (existing_df.filter(
            pl.col(no_referensi_col).is_in(new_flag_refs))
            .select((pl.col(flag_col) == 1).all())
            .collect()
            .item()
        )
        # Append only if there are new records
        if not is_new_refs_exist:
            delta_df = existing_df.filter(
                (pl.col(no_referensi_col).is_in(new_flag_refs)) &
                (pl.col(flag_col) == 0)
            )
            logger.info("New records found in daily flag pembayaran QR data.")
            logger.info(
                f"Flagging {delta_df.select(pl.len()).collect()[0, 0]} new records to historical pembayaran QR data..")

            # Updating flagged records colunm 'flag' to 1
            updated_exist_df = (
                existing_df.with_columns(
                    (pl.col(flag_col) | pl.col(no_referensi_col).is_in(
                        new_flag_refs)).alias(flag_col))
            )
            updated_exist_df.collect().write_parquet(history_path)
            logger.info("Historical parquet data updated successfully.")
        else:
            logger.info("There is no new flag pembayaran QR found.")
            logger.info("Flagged records have been added previously.")
            logger.info("Updating flag record finished.")
            return
    else:
        logger.error("Historical data to update not found.")
        logger.error("Updating flag into historical data terminated.")
        return
    logger.info("Process completed.")


def update_flag_10min(
    history_path: Path | str = CONSTS.update_flag_10min.hist_path,
    daily_path: Path | str = CONSTS.update_flag_10min.daily_path_flag,
    no_referensi_col: str = CONSTS.update_flag_10min.no_referensi,
    flag_col: str = CONSTS.update_flag_10min.flag_col,
) -> None:
    """
    Updating the historical pembayaran QR 5 min data by adding daily flagged transaction.

    Params:
    ------
    n_days: int
        Total number of days to look back for the flagged pembayaran QR data.
    history_path: Path | str
        Path where the historical pembayaran QR data stored.
    daily_path: Path | str
        Path where the daily flagged pembayaran QR data stored.
    no_referensi_col: str
        Column of no_referensi to use.
    flag_col: str
        Column of flag to use.

    Returns:
    -------
    None
    """
    history_path = Path(history_path)
    daily_path = Path(daily_path)
    history_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(
        f"Reading daily flag pembayaran QR data from {history_path}...")

    if daily_path.exists():
        try:
            daily_flag_df = pl.scan_parquet(daily_path)
            logger.info("Daily flag pembayaran QR data read succeeded.")
        except Exception as e:
            logger.error(f"Error reading parquet data : {e}")
            logger.error(
                "Update pembayaran QR flag to Historical Data Process terminated.")
            return
    else:
        logger.info("There is no daily flag pembayaran QR data found.")
        logger.info("Update Flag to Historical data finished.")
        return

    if is_empty(daily_flag_df):
        logger.info("There is no daily flag pembayaran QR data found.")
        logger.info("Update flag process finished.")
        return

    # Check if Historical Data exist
    if history_path.exists():
        logger.info(f"Reading historical data from {history_path}...")
        existing_df = pl.scan_parquet(history_path)
        logger.info("Historical data read successfully.")

        new_flag_refs = (daily_flag_df
                         .select(no_referensi_col)
                         .collect()
                         .to_series()
                         .to_list()
                         )

        # Check if new_refs already updated as flag 1 in historical
        is_new_refs_exist = (existing_df.filter(
            pl.col(no_referensi_col).is_in(new_flag_refs))
            .select((pl.col(flag_col) == 1).all())
            .collect()
            .item()
        )
        # Append only if there are new records
        if not is_new_refs_exist:
            delta_df = existing_df.filter(
                (pl.col(no_referensi_col).is_in(new_flag_refs)) &
                (pl.col(flag_col) == 0)
            )
            logger.info("New records found in daily flag pembayaran QR data.")
            logger.info(
                f"Flagging {delta_df.select(pl.len()).collect()[0, 0]} new records to historical pembayaran QR data..")

            # Updating flagged records colunm 'flag' to 1
            updated_exist_df = (
                existing_df.with_columns(
                    (pl.col(flag_col) | pl.col(no_referensi_col).is_in(
                        new_flag_refs)).alias(flag_col))
            )
            updated_exist_df.collect().write_parquet(history_path)
            logger.info("Historical parquet data updated successfully.")
        else:
            logger.info("There is no new flag pembayaran QR found.")
            logger.info("Flagged records have been added previously.")
            logger.info("Updating flag record finished.")
            return
    else:
        logger.error("Historical data to update not found.")
        logger.error("Updating flag into historical data terminated.")
        return
    logger.info("Process completed.")
