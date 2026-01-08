import os
import uuid
import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
from .logger import logger
from .models import CONSTS, dtypes


DROP_COLS = ["flag_5min", "flag_10min", "flag_50mio", "flag_50mio_early"]


def is_empty(lf: pl.LazyFrame) -> bool:
    return lf.limit(1).collect().height == 0


def filter_date(
    df: pl.DataFrame,
    n_days: int = CONSTS.params_tf_online_10min.n_days,
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


def flag_tf_online_10min(
    job_title: str = CONSTS.job.title,
    hist_path: Path | str = CONSTS.flag.hist_path,
    output_path: Path | str = CONSTS.flag.output_path,
    usecols: list = CONSTS.flag.usecols,
    transaction_date_col: str = CONSTS.flag.transaction_date,
    account_number_col: str = CONSTS.flag.account_number,
    no_referensi_col: str = CONSTS.flag.no_referensi,
    flag_col: str = CONSTS.flag.flag_col,
    temp_date_col: str = CONSTS.flag.temp_date_col,
    rolling_window: int = CONSTS.params_tf_online_10min.rolling_window,
    threshold: int = CONSTS.params_tf_online_10min.threshold,
) -> None:
    """
    Count transactions based on rolling window time frame.
    Params:
    -------
    hist_path: Path | str
        Path to the historical transfer online data.
    output_path: Path | str
        Path to save daily flagged tf_online data.
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
    logger.info(f"Processing {job_title}...")
    output_path = Path(output_path)
    # output_path.parent.mkdir(parents=True, exist_ok=True)
    os.makedirs(os.path.dirname(hist_path), exist_ok=True)
    tmp_path = f"{hist_path}.{uuid.uuid4().hex}.tmp"

    try:
        logger.info("Loading historical data...")
        df = pl.read_parquet(hist_path).lazy()
        df = df.drop(DROP_COLS, strict=False)
        df = df.sort(transaction_date_col)

        logger.info(
            f"Total records: {df.select(pl.len()).collect()[0, 0]}")
        logger.info("Succeeded loading historical data.")
    except Exception as e:
        logger.error(f"Error loading historical data: {e}.")
        logger.error(f"{job_title} Process terminated.")
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
        logger.error(f"{job_title} Process terminated.")
        return

    try:
        logger.info("Calculating flagged transaction data...")
        df_rolling = (df.rolling(
            index_column=transaction_date_col,
            period=rolling_window,
            group_by=account_number_col
        ).agg(pl.len()).rename({"len": "rolling_count"})
        )
        df_with_rolling = (df.join(
            df_rolling,
            on=[account_number_col, transaction_date_col],
            how="left")
        )
        burst_end = (df_rolling.filter(
            pl.col("rolling_count") >= threshold).select([account_number_col, transaction_date_col])
        )
        # burst_end: ensure "t_start" column exists
        burst_end = (burst_end.with_columns(
            (pl.col(f"{transaction_date_col}") - pl.duration(minutes=10)).alias("t_start"))
        )
        df_with_rolling_with_burst = (df_with_rolling.join(
            burst_end,
            on=[f"{account_number_col}"],
            how="inner")
        )
        flag_only = (df_with_rolling_with_burst.filter(
            (pl.col(transaction_date_col) >= pl.col("t_start")) &
            (pl.col(transaction_date_col) <= pl.col("transaction_date_right"))).unique(subset=[no_referensi_col])
        )
        flag_only = (flag_only.with_columns(
            pl.lit(1).cast(pl.Int8).alias(flag_col))
        )
    except Exception as e:
        logger.error(
            f"Error calculation {job_title}: {e}.")
        logger.error(
            f"{job_title} Process terminated.")
        return

    if not is_empty(flag_only):
        try:
            rows = flag_only.select(pl.len()).collect()[0, 0]
            logger.info(f"Found {rows} flagged records.")
            logger.info(
                f"Saving {job_title}...")
            logger.info(f"Saving into {output_path}.")
            logger.info(f"Rows: {flag_only.select(pl.len()).collect()[0, 0]}")
            flag_only.collect(streaming=False).write_parquet(
                tmp_path,
                compression="zstd",
                statistics=True,
                use_pyarrow=True
            )
            # Atomic replace (POSIX-safe)
            os.replace(tmp_path, output_path)
            logger.info(
                f"Saving {job_title} succeed.")
        except Exception as e:
            logger.error(
                f"Error saving {job_title}: {e}")
            logger.error(
                f"{job_title} Process terminated.")
            return
    else:
        dtypes_list = list(dtypes.values())

        # Make blank dataframe with same schema if no flagged data found
        flag_only = df = pl.DataFrame(
            {col: pl.Series(col, dtype=col_type) for col, col_type in zip(usecols, dtypes_list)})
        flag_only = flag_only.with_columns(
            pl.lit(
                None,
                dtype=pl.Int8).alias(flag_col))
        flag_only = flag_only.with_columns(
            pl.lit(
                None,
                dtype=pl.Int8).alias(temp_date_col))
        logger.info(f"Rows: {flag_only.select(pl.len()).collect()[0, 0]}")
        flag_only.write_parquet(
            tmp_path,
            compression="zstd",
            statistics=True,
            use_pyarrow=True
        )
        # Atomic replace (POSIX-safe)
        os.replace(tmp_path, output_path)
        logger.info(
            f"There is no {job_title} found.")
        logger.info("Process finished")
