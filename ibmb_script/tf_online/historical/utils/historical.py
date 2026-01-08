import os
import uuid
import polars as pl
from typing import List
from datetime import datetime, timedelta
from pathlib import Path
from .models import CONSTS, dtypes
from .logger import logger


def is_empty(lf: pl.LazyFrame) -> bool:
    return lf.limit(1).collect().height == 0


def update_historical(
    job_title: str = CONSTS.job.title,
    n_days: int = CONSTS.historical.n_days,
    input_folder: str = CONSTS.historical.input_folder,
    file_prefix: str = CONSTS.historical.file_prefix,
    file_suffix: str = CONSTS.historical.file_suffix,
    date_format: str = CONSTS.date.date_file_format,
    hist_path: Path | str = CONSTS.historical.hist_path,
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
    hist_path : Path | str
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
    hist_path = Path(hist_path)
    hist_path.parent.mkdir(parents=True, exist_ok=True)
    os.makedirs(os.path.dirname(hist_path), exist_ok=True)
    tmp_path = f"{hist_path}.{uuid.uuid4().hex}.tmp"

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
                    # columns=usecols,
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
                )
                df_rows = df.select(pl.len()).collect()[0, 0]
                df_cols = len(df.collect_schema().names())
                logger.info(f"Loaded {df_rows} rows and {df_cols} columns.")
            except Exception as e:
                logger.info(f"Error filtering tf_online Data : {e}")
                logger.info("Update Historical process terminated.")
                return

            dataframes.append(df)

        else:
            logger.warning(f"File not found: {filepath}.")

    # If no files found
    if not dataframes:
        logger.warning("No files found.")
        logger.warning(f"Updating {job_title} process finished.")
        return
    else:
        logger.info("Concatenating all daily files...")
        try:
            daily_df = pl.concat(dataframes)
            daily_df = daily_df.sort(
                by=[transaction_date_col, account_number_col])
            logger.info("Concatenating all daily files succeed.")
        except Exception as e:
            logger.error(f"Error concatenating files: {e}.")
            logger.error(f"Updating {job_title} process terminated.")
            return

    total_rows = daily_df.select(pl.len()).collect()[0, 0]
    total_cols = len(daily_df.collect_schema().names())
    logger.info(
        f"Concatenated DataFrame has {total_rows} rows and {total_cols} columns.")

    # Check if flag columns already exists
    existing_cols = set(daily_df.collect_schema().names())
    missing_flags = [
        pl.lit(0).cast(pl.Int8).alias(col)
        for col in flag_col
        if col not in existing_cols
    ]
    if missing_flags:
        daily_df = daily_df.with_columns(missing_flags)

    # If there existing historical data
    if hist_path.exists():
        logger.info("Historical Data Exist...")
        logger.info("Loading Historical Data...")
        try:
            # Load historical data
            hist_df = (
                pl.read_parquet(hist_path).lazy()
            )
            logger.info("Loading Historical Data succeed.")
        except Exception as e:
            logger.info(f"Error loading historical data: {e}")

        # Ensure historical df has all flag columns
        hist_existing_cols = set(
            hist_df.collect_schema().names())
        hist_missing_flags = [
            pl.lit(0).cast(pl.Int8).alias(col)
            for col in flag_col
            if col not in hist_existing_cols
        ]
        if hist_missing_flags:
            hist_df = hist_df.with_columns(
                hist_missing_flags)

        try:
            logger.info("Searching delta in daily data.")
            # Only append NEW records
            delta_new = daily_df.join(
                hist_df,
                on=no_referensi_col,
                how="anti"
            )
            if is_empty(delta_new):
                logger.info("No new records to append.")
                final_df = hist_df
            else:
                final_df = pl.concat(
                    [hist_df, delta_new],
                    how="vertical",
                    rechunk=True,
                )
        except Exception as e:
            logger.info(
                f"Error during sorting unique records with anti-join: {e}")

        logger.info(f"Saving historical data into {hist_path}...")
        logger.info(f"Rows: {final_df.select(pl.len()).collect()[0, 0]}")
        final_df.collect(streaming=False).write_parquet(
            tmp_path,
            compression="zstd",
            statistics=True,
            use_pyarrow=True
        )
        # Atomic replace (POSIX-safe)
        os.replace(tmp_path, hist_path)
        logger.info(f"Saving historical data succeed.")
    else:
        logger.info(f"There is no {job_title} data found.")
        logger.info("Making new historical parquet data...")

        try:
            final_df = daily_df
            logger.info(f"Saving historical data into {hist_path}...")
            logger.info(f"Rows: {final_df.select(pl.len()).collect()[0, 0]}")
            final_df.collect(streaming=False).write_parquet(
                tmp_path,
                compression="zstd",
                statistics=True,
                use_pyarrow=True
            )
            # Atomic replace (POSIX-safe)
            os.replace(tmp_path, hist_path)
            logger.info(
                f"Succeeded saving the updated {job_title}.")
        except Exception as e:
            logger.error(
                f"Error saving {job_title}: {e}")
            logger.error("Update historical process terminated.")
            return
