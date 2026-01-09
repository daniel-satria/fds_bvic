import os
import uuid
import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
from .logger import logger
from .models import CONSTS, dtypes


DROP_COLS = ["flag_10min", "flag_5mio"]


def is_empty(lf: pl.LazyFrame) -> bool:
    return lf.limit(1).collect().height == 0


def filter_date(
    df: pl.DataFrame,
    n_days: int = CONSTS.params.n_days,
    transaction_date_col: str = CONSTS.flag.transaction_date,
    temp_date_col: str = CONSTS.flag.temp_date_col
) -> pl.DataFrame | None:
    """
    Filter dataframe to include only transactions in specific dates.

    Params:
    -------
    df: pl.DataFrame
        Dataframe to be filtered.
    n_days: int
        Total number of days to look back for the flagged CCW data. 
    transaction_date : str
        Column used for index in rolling calculation.
    temp_date_col : str
        Temporary column for storing date.

    Returns:
    -------
    None
    """
    try:
        cutoff_date = datetime.now().date() - timedelta(days=n_days)
        logger.info(f"Cutoff Date: {cutoff_date}")
        df = df.with_columns(
            pl.col(transaction_date_col).dt.date().alias(temp_date_col))
        df = df.filter(
            pl.col(temp_date_col) >= cutoff_date
        )
        return df
    except Exception as e:
        logger.error(f"Error filtering data: {e}")


def flag_non_qr_5mio(
    job_title: str = CONSTS.job.title,
    hist_path: Path | str = CONSTS.flag.hist_path,
    output_path: Path | str = CONSTS.flag.output_path,
    usecols: list = CONSTS.flag.usecols,
    transaction_date_col: str = CONSTS.flag.transaction_date,
    account_number_col: str = CONSTS.flag.account_number,
    no_referensi_col: str = CONSTS.flag.no_referensi,
    flag_col: str = CONSTS.flag.flag_col,
    temp_date_col: str = CONSTS.flag.temp_date_col,
    rolling_window: int = CONSTS.params.rolling_window,
    threshold: int = CONSTS.params.threshold,
) -> None:
    """
    Count transactions based on rolling window time frame.
    Params:
    -------
    hist_path: Path | str
        Path to the historical E-wallet data.
    output_path: Path | str
        Path to save daily flagged E-wallet data.
    transaction_date_col:
        Column date to use as index.
    account_number_col:
        Column account_number of the transaction data.
    no_referensi_col:
        Column Referensi number to identify unique of the transaction data.
    flag_col:
        Column name to add as marking to flagged data.
    rolling_window: int
        The rolling window size in minutes.
    threshold: int
        The minimum total number of transactions for flagging the accounts.

    Returns:
    --------
    None
    """
    logger.info(f"Processing {job_title} data...")
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    os.makedirs(os.path.dirname(hist_path), exist_ok=True)
    tmp_path = f"{hist_path}.{uuid.uuid4().hex}.tmp"

    try:
        logger.info(f"Loading historical data...")
        df = pl.scan_parquet(hist_path)
        df = df.drop(DROP_COLS, strict=False)
        df = df.sort(transaction_date_col)

        logger.info(
            f"Total {job_title} records: {df.select(pl.len()).collect()[0, 0]}")
        logger.info(f"Succeeded loading historical data.")
    except Exception as e:
        logger.error(f"Error loading historical data: {e}.")
        logger.error(
            f"{job_title} Process terminated.")
        return

    # Filter based on days
    try:
        logger.info("Filtering daily data...")
        df = filter_date(df=df)
        logger.info("Filtering finished.")
        logger.info(
            f"Data after filtering: {df.select(pl.len()).collect()[0, 0]}")
    except Exception as e:
        logger.error(f"Error filtering data: {e}")
        logger.error(f"{job_title} process terminated.")
        return

    # Calculating flag transaction
    try:
        logger.info("Calculating flagged transaction data...")

        # Step 1: Self-join on account number within 24h window
        df_rolled = (
            df.join(
                df,
                on=account_number_col,
                how="left",
                suffix="_r"
            )
            .filter(
                (pl.col(f"{transaction_date_col}_r") >= pl.col(transaction_date_col) - pl.duration(hours=rolling_window)) &
                (pl.col(f"{transaction_date_col}_r")
                 <= pl.col(transaction_date_col))
            )
        )
        # Step 2: Group sums on the original transaction
        df_sums = (
            df_rolled
            .group_by(no_referensi_col)
            .agg(
                pl.sum("transaction_amount_r").alias("rolling_24h_sum"))
        )
        # Step 3: Join sums back and filter >= 5M
        df_flagged_accounts = (
            df_rolled
            .join(
                df_sums,
                on=no_referensi_col
            ).filter(pl.col("rolling_24h_sum") >= threshold))

        # Step 4: EXTRACT KEY (these transactions exceeded 5M)
        keys = (
            df_flagged_accounts
            .select([account_number_col, no_referensi_col])
            .unique()
        )
        # Step 5: Returns only rows that match the keys
        flag_only = (
            df.join(
                keys,
                on=[account_number_col, no_referensi_col],
                how="semi"
            ).with_columns(pl.lit(1).cast(pl.Int8).alias(flag_col))
        )
    except Exception as e:
        logger.error(
            f"Error calculating {job_title}: {e}.")
        logger.error(
            f"Flag {job_title} Process terminated.")
        return

    if not is_empty(flag_only):
        try:
            rows = flag_only.select(pl.len()).collect()[0, 0]
            logger.info(f"Found {rows} flagged records.")
            logger.info(
                f"Saving daily {job_title} ...")
            logger.info(f"Saving into {output_path}.")
            flag_only.collect(streaming=False).write_parquet(
                tmp_path,
                compression="zstd",
                statistics=True,
                use_pyarrow=True
            )
            # Atomic replace (POSIX-safe)
            os.replace(tmp_path, hist_path)
            logger.info(
                f"Saving daily flagged {job_title}  succeed.")
        except Exception as e:
            logger.error(
                f"Error saving {job_title} : {e}")
            logger.error(
                f"{job_title} Process terminated.")
            return
    else:
        dtypes_list = list(dtypes.values())
        # Make blank dataframe with same schema if no flagged data found
        flag_only = df = pl.DataFrame(
            {col: pl.Series(col, dtype=col_type) for col, col_type in zip(usecols, dtypes_list)})
        flag_only = flag_only.with_columns(
            pl.lit(None, dtype=pl.Int8).alias(flag_col))
        flag_only = flag_only.with_columns(
            pl.lit(None, dtype=pl.Int8).alias(temp_date_col))
        flag_only.write_parquet(
            tmp_path,
            compression="zstd",
            statistics=True,
            use_pyarrow=True
        )
        # Atomic replace (POSIX-safe)
        os.replace(tmp_path, hist_path)
        logger.info(
            f"There is no daily {job_title} found.")
        logger.info("Process finished")
