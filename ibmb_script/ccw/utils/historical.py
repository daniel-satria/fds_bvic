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
    ccw_hist_path: Path | str = CONSTS.historical.ccw_hist_path,
    transaction_date_col: str = CONSTS.historical.transaction_date,
    account_number_col: str = CONSTS.historical.account_number,
    flag_col: str = CONSTS.historical.flag_col,
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
    Load daily IBMB files and filter for CCW transactions only.

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
    ccw_hist_path : Path | str
        Path to save the filtered CCW historical data.
    transaction_date_col: str
        Column name to be used a transaction date.
    account_number_col: str
        Column name to be used as client account number.
    flag_col: str
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
    ccw_hist_path = Path(ccw_hist_path)
    ccw_hist_path.parent.mkdir(parents=True, exist_ok=True)

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
                df_rows = df.select(pl.len()).collect()[0, 0]
                df_cols = len(df.collect_schema().names())

                logger.info(
                    f"Loaded {df_rows} rows and {df_cols} columns.")
                dataframes.append(df)
            except Exception as e:
                logger.error(f"Error loading {filepath}: {e}")
                logger.error("Update historical data terminated.")
                return
        else:
            logger.warning(f"File not found: {filepath}.")

    # If no files found
    if not dataframes:
        logger.warning("No files found.")
        logger.warning("Updating historical CCW process finished.")
        return
    logger.info("Concatenating all daily files...")

    try:
        df_all = pl.concat(dataframes)
        logger.info("Concatenating all daily files succeed.")
    except Exception as e:
        logger.error(f"Error concatenating files: {e}.")
        logger.error("Updating historical CCW process terminated.")
        return

    total_rows = df_all.select(pl.len()).collect()[0, 0]
    total_cols = len(df_all.collect_schema().names())
    logger.info(
        f"Concatenated DataFrame has {total_rows} rows and {total_cols} columns.")

    # Filter settled CCW transactions
    logger.info("Filtering CCW records only...")

    # Filter CCW Daily data only
    try:
        ccw_daily_df = (
            df_all.filter(
                (pl.col(transaction_status_col).is_in
                 (transaction_status)) &
                (pl.col(transaction_state_col).is_in
                 (transaction_state)) &
                (pl.col(transaction_category_col).is_in
                 (transaction_category))
            )
            .sort(by=[transaction_date_col, account_number_col])
        )
        logger.info("Succeeded filtering Daily CCW records.")
        logger.info(
            f"Total CCW records after filtering: {ccw_daily_df.select(pl.len()).collect()[0, 0]}")
    except Exception as e:
        logger.info(f"Error filterring CCW data: {e}")
        logger.info("Update Historical process terminated.")
        return

    if "flag" not in ccw_daily_df.collect_schema().names():
        ccw_daily_df = (ccw_daily_df.with_columns(
            pl.lit(0).cast(pl.Int8).alias(flag_col))
        )

    # If there existing historical data
    if ccw_hist_path.exists():
        logger.info("Historical Data Exist...")
        logger.info("Loading Historical Data...")
        try:
            ccw_hist_df = (pl.scan_parquet(
                ccw_hist_path)
            )  # Load historical data
            logger.info("Loading Historical Data succeed.")
        except Exception as e:
            logger.info(f"Error loading historical data: {e}")

        try:
            logger.info("Searching delta in new daily data.")
            delta_new = ccw_daily_df.join(
                ccw_hist_df,
                on=no_referensi_col,
                how="anti"
            )  # Get records from daily not in history
            logger.info("Searching delta in historical data.")
            delta_old = ccw_hist_df.join(
                ccw_hist_df,
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
        logger.info("Concatenating new CCW data into historical data...")
        ccw_hist_df = pl.concat([delta_new, delta_old])
        logger.info(f"Saving historical data into {ccw_hist_path}...")
        ccw_hist_df.collect().write_parquet(ccw_hist_path)
        logger.info(f"Saving historical data succeed.")
    else:
        logger.info("There is no existing historical parquet data found.")
        logger.info("Making new historical parquet data...")

    try:
        ccw_daily_df.collect().write_parquet(ccw_hist_path)
        logger.info(
            "Succeeded saving the updated historical CCW records.")
    except Exception as e:
        logger.error(
            f"Error saving the historical CCW records: {e}")
        logger.error("Update historical process terminated.")
        return


def update_flag(
    n_days: int = CONSTS.update.n_days,
    history_path: Path | str = CONSTS.update.ccw_hist_path,
    daily_path: Path | str = CONSTS.update.daily_path_flag,
    no_referensi_col: str = CONSTS.update.no_referensi,
    temp_date_col: str = CONSTS.update.temp_date_col,
    flag_col: str = CONSTS.update.flag_col,
) -> None:
    """
    Updating the historical CCW data by adding daily flagged CCW data.

    Params:
    ------
    n_days: int
        Total number of days to look back for the flagged CCW data.
    history_path: Path | str
        Path where the historical CCW data stored.
    daily_path: Path | str
        Path where the daily flagged CCW data stored.
    no_referensi_col: str
        Column of no_referensi to use.
    temp_date_col: str
        Temporary column name for date.
    flag_col: str
        Column of flag to use.

    Returns:
    -------
    None
    """
    cutoff_date = datetime.now().date() - timedelta(days=n_days)
    history_path = Path(history_path)
    daily_path = Path(daily_path)
    history_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Reading daily flag CCW data from {history_path}...")
    logger.info(f"Filtering for date: {cutoff_date}...")

    if daily_path.exists():
        try:
            daily_flag_df = pl.scan_parquet(daily_path)
            daily_flag_df = daily_flag_df.filter(
                pl.col(temp_date_col) >= cutoff_date
            )
            logger.info("Daily flag CCW data read succeed.")
        except Exception as e:
            logger.error(f"Error reading parquet data : {e}")
            logger.error(
                "Update CCW Flag to Historical Data Process terminated.")
            return
    else:
        logger.info("There is no daily flag CCW data found.")
        logger.info("Update Flag to Historical data finished.")
        return

    if daily_flag_df.collect().is_empty():
        logger.info("There is no daily flag CCW data found.")
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
            logger.info("New records found in daily flag CCW data.")
            logger.info(
                f"Flagging {delta_df.select(pl.len()).collect()[0, 0]} new records to historical CW data..")
            # Updating flagged records colunm 'flag' to 1
            updated_exist_df = (existing_df.with_columns(
                (pl.col(flag_col) | pl.col(no_referensi_col).is_in(
                    new_flag_refs)).alias(flag_col))
            )
            updated_exist_df.collect().write_parquet(history_path)
            logger.info("Historical parquet data updated successfully.")
        else:
            logger.info("There is no new flag CCW found.")
            logger.info("Flagged records have been added previously.")
            logger.info("Updating flag record finished.")
            return
    else:
        logger.error("Historical data to update not found.")
        logger.error("Updating flag into historical data terminated.")
        return
    logger.info("Process completed.")
