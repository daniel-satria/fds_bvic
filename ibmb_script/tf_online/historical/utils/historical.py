import os
import polars as pl
from typing import List
from datetime import datetime, timedelta
from pathlib import Path
from .models import CONSTS, dtypes
from .logger import logger


def is_empty(lf: pl.LazyFrame) -> bool:
    return lf.limit(1).collect().height == 0


def update_historical(
    n_days: int = CONSTS.historical.n_days,
    input_folder: str = CONSTS.historical.input_folder,
    file_prefix: str = CONSTS.historical.file_prefix,
    file_suffix: str = CONSTS.historical.file_suffix,
    date_format: str = CONSTS.date.date_file_format,
    tf_online_hist_path: Path | str = CONSTS.historical.tf_online_hist_path,
    transaction_date_col: str = CONSTS.historical.transaction_date,
    account_number_col: str = CONSTS.historical.account_number,
    flag_col: List[str] = CONSTS.historical.flag_col,
    usecols: List[str] = CONSTS.historical.usecols,
    transaction_status_col: List[int] = CONSTS.historical.transaction_status,
    transaction_state_col: List[int] = CONSTS.historical.transaction_state,
    transaction_category_col: List[str] = CONSTS.historical.transaction_category,
    transaction_status: List[int] = CONSTS.filter.transaction_status,
    transaction_state: List[int] = CONSTS.filter.transaction_state,
    transaction_category: List[str] = CONSTS.filter.transaction_category,
    no_referensi_col: str = CONSTS.historical.no_referensi,
    null_values: List[str] = CONSTS.historical.null_values,
    dtypes: dict = dtypes
) -> None:
    """
    Load daily IBMB files and filter for transaction online records only.

    Params:
    -------
    n_days : int
        How many days back to load (including today).
    input_folder : str
        Directory where the files are stored.
    file_prefix : str
        Optional prefix for filenames (e.g., "data_").
    file_suffix : str
        File extension (e.g., ".csv").
    date_format : str
        Format of the date inside the filename.
    tf_online_hist_path : Path | str
        Path to save the filtered tf_online historical data.
    transaction_date_col: str
        Column name to be used a transaction date.
    account_number_col: str
        Column name to be used as client account number.
    flag_col: List[str]
        Column name to be used as a flag.
    usecols : List[str]
        List of columns to read from the CSV files.
    transaction_status_col : str
        Column of transaction status codes to filter.
    transaction_state_col : str
        Column of transaction state codes to filter.
    transaction_category_col : str
        Column of transaction categories to filter.
    transaction_status : List[int]
        List of transaction status codes to filter.
    transaction_state : List[int]
        List of transaction state codes to filter.
    transaction_category : List[str]
        List of transaction categories to filter.
    no_referensi: List[str]
        Unique identifier for every transaction.
    null_values: List[str]
        Values to be treated as null.
    dtypes: dict
        Schema for the IBMB daily data

    Returns:
    --------
    None
    """
    dataframes = []
    today = datetime.today()
    tf_online_hist_path = Path(tf_online_hist_path)
    tf_online_hist_path.parent.mkdir(parents=True, exist_ok=True)

    for i in range(n_days):
        date_str = (today - timedelta(days=i)).strftime(date_format)
        filename = f"{file_prefix}{date_str}{file_suffix}"
        filepath = os.path.join(input_folder, filename)

        if os.path.exists(filepath):
            try:
                logger.info(f"Loading: {filepath}")
                # Load daily all IBMB csv data
                df = pl.scan_csv(
                    filepath,
                    separator='|',
                    new_columns=usecols,
                    null_values=null_values,
                    dtypes=dtypes
                )
            except Exception as e:
                logger.error(f"Error loading {filepath}: {e}")
                logger.error("Update historical data terminated.")
                return

            # Filter tf_online only data
            try:
                df = (
                    df.filter(
                        (pl.col(transaction_status_col).is_in
                         (transaction_status)) &
                        (pl.col(transaction_state_col).is_in
                         (transaction_state)) &
                        (pl.col(transaction_category_col).is_in
                         (transaction_category))
                    )
                    .sort(by=[transaction_date_col, account_number_col])
                )
                df_rows, df_cols = df.collect().shape
                logger.info(f"Loaded {df_rows} rows and {df_cols} columns.")
            except Exception:
                logger.info(f"Error filtering tf_online Data : {e}")
                logger.info("Update Historical process terminated.")
                return

            dataframes.append(df)

        else:
            logger.warning(f"File not found: {filepath}.")

    # If no files found
    if not dataframes:
        logger.warning("No files found.")
        logger.warning("Updating historical tf_online process finished.")
        return
    logger.info("Concatenating all daily files...")

    try:
        tf_online_daily_df = pl.concat(dataframes)
        tf_online_daily_df = tf_online_daily_df.sort(
            by=[transaction_date_col, account_number_col])
        logger.info("Concatenating all daily files succeed.")
    except Exception as e:
        logger.error(f"Error concatenating files: {e}.")
        logger.error("Updating historical tf_online process terminated.")
        return

    total_rows, total_cols = tf_online_daily_df.collect().shape
    logger.info(
        f"Concatenated DataFrame has {total_rows} rows and {total_cols} columns.")

    # Check if flag columns already exists
    for col in flag_col:
        if CONSTS.historical.flag_col not in tf_online_daily_df.collect_schema().names():
            tf_online_daily_df = (tf_online_daily_df.with_columns(
                pl.lit(0).cast(pl.Int8).alias(col))
            )

    # If there existing historical data
    if tf_online_hist_path.exists():
        logger.info("Historical Data Exist...")
        logger.info("Loading Historical Data...")
        try:
            tf_online_hist_df = (pl.scan_parquet(
                tf_online_hist_path)
            )  # Load historical data
            logger.info("Loading Historical Data succeed.")
        except Exception as e:
            logger.info(f"Error loading historical data: {e}")

        try:
            logger.info("Searching delta in new daily data.")
            delta_new = tf_online_daily_df.join(
                tf_online_hist_df,
                on=no_referensi_col,
                how="anti"
            )  # Get records from daily not in history
            logger.info("Searching delta in historical data.")
            delta_old = tf_online_hist_df.join(
                tf_online_hist_df,
                on=no_referensi_col,
                how="anti"
            )  # Get records from history not in daily
        except Exception as e:
            logger.info(
                f"Error during sorting unique records with anti-join: {e}")

        if is_empty(delta_new) and is_empty(delta_old):
            logger.info("There is no historical data to be updated")
            logger.info("Update Historical Process finished")
            return

        # Concatenating all unique records
        logger.info(
            "Concatenating new transfer online data into historical data...")
        tf_online_hist_df = pl.concat([delta_new, delta_old])
        logger.info(f"Saving historical data into {tf_online_hist_path}...")
        tf_online_hist_df.collect().write_parquet(tf_online_hist_path)
        logger.info(f"Saving historical data succeed.")
    else:
        logger.info("There is no existing historical parquet data found.")
        logger.info("Making new historical parquet data...")

    try:
        tf_online_daily_df.collect().write_parquet(tf_online_hist_path)
        logger.info(
            "Succeeded saving the updated historical transfer online records.")
    except Exception as e:
        logger.error(
            f"Error saving the historical transfer online records: {e}")
        logger.error("Update historical process terminated.")
        return
