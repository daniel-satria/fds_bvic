import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
from .logger import logger
from .models import CONSTS


def filter_date(
    df: pl.DataFrame,
    n_days: int = CONSTS.params_ccw.n_days,
    transaction_date_col: str = CONSTS.daily.transaction_date,
    temp_date_col: str = CONSTS.daily.temp_date_col
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


def flag_ccw(
    ccw_hist_path: Path | str = CONSTS.daily.ccw_hist_path,
    output_path: Path | str = CONSTS.daily.output_path,
    transaction_date_col: str = CONSTS.daily.transaction_date,
    account_number_col: str = CONSTS.daily.account_number,
    no_referensi_col: str = CONSTS.daily.no_referensi,
    flag_col: str = CONSTS.daily.flag_col,
    rolling_window: int = CONSTS.params_ccw.rolling_window,
    threshold: int = CONSTS.params_ccw.threshold,
) -> None:
    """
    Count transactions based on rolling window time frame.
    Params:
    -------
    ccw_hist_path: Path | str
        Path to the historical CCW data file.
    output_path: Path | str
        Path to save daily flagged CCW data.
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
    logger.info("Processing Flagged CCW data...")
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        logger.info("Loading historical CCW data...")
        df = pl.scan_parquet(ccw_hist_path)
        logger.info(
            f"Total CCW records: {df.select(pl.len()).collect()[0, 0]}")
        logger.info("Succeeded loading historical CCW data.")
    except Exception as e:
        logger.error(f"Error loading historical CCW data: {e}.")
        logger.error("Flag CCW Process terminated.")
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
        logger.error("Flag CCW Process terminated.")
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
        ccw_flag_only = (df_with_rolling_with_burst.filter(
            (pl.col(transaction_date_col) >= pl.col("t_start")) &
            (pl.col(transaction_date_col) <= pl.col("transaction_date_right"))).unique(subset=[no_referensi_col])
        )
        ccw_flag_only = (ccw_flag_only.with_columns(
            pl.lit(1).cast(pl.Int8).alias(flag_col))
        )
        if not ccw_flag_only.collect().is_empty():
            try:
                rows = ccw_flag_only.select(pl.len()).collect()[0, 0]
                logger.info(f"Found {rows} flagged records.")
                logger.info("Saving daily flagged CCW transactions...")
                logger.info(f"Saving into {output_path}.")
                ccw_flag_only.collect().write_parquet(output_path)
                logger.info("Saving daily flagged CCW transactions succeed.")
            except Exception as e:
                logger.error(
                    f"Error saving daily flagged CCW transactions: {e}")
                logger.error("Flag CCW Process terminated.")
                return
        else:
            logger.info("There is no daily flagged CCW transaction found.")
            logger.info("Process finished")
    except Exception as e:
        logger.error(f"Error calculation flagged CCW transactions: {e}.")
        logger.error("Flag CCW Process terminated.")
        return
