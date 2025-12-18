import polars as pl
from pathlib import Path
from .logger import logger
from .models import CONSTS


DROP_COLS = ["flag_10min", "flag_5mio"]


def is_empty(lf: pl.LazyFrame) -> bool:
    return lf.limit(1).collect().height == 0


def flag_10min(
    job_title: str = CONSTS.job.title,
    hist_path: Path | str = CONSTS.flag.hist_path,
    output_path: Path | str = CONSTS.flag.output_path,
    transaction_date_col: str = CONSTS.flag.transaction_date,
    account_number_col: str = CONSTS.flag.account_number,
    no_referensi_col: str = CONSTS.flag.no_referensi,
    flag_col: str = CONSTS.flag.flag_col,
    rolling_window: int = CONSTS.params.rolling_window,
    rolling_window_int: int = CONSTS.params.rolling_window_int,
    threshold: int = CONSTS.params.threshold,
) -> None:
    """
    Count transactions based on rolling window time frame.
    Params:
    -------
    hist_path: Path | str
        Path to the historical E-wallet.
    output_path: Path | str
        Path to save daily flagged E-wallet.
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
    logger.info(f"Processing Flagged {job_title} data...")
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        logger.info(f"Loading historical {job_title} data...")
        df = pl.scan_parquet(hist_path)
        df = df.drop(DROP_COLS, strict=False)

        logger.info(
            f"Total {job_title} records: {df.select(pl.len()).collect()[0, 0]}")
        logger.info(f"Succeeded loading historical {job_title} data.")
    except Exception as e:
        logger.error(f"Error loading historical {job_title} data: {e}.")
        logger.error(f"Flag {job_title} Process terminated.")
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
            (pl.col(f"{transaction_date_col}") - pl.duration(minutes=rolling_window_int)).alias("t_start"))
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
        if not is_empty(flag_only):
            try:
                rows = flag_only.select(pl.len()).collect()[0, 0]
                logger.info(f"Found {rows} flagged records.")
                logger.info(
                    f"Saving daily flagged {job_title} transactions...")
                logger.info(f"Saving into {output_path}.")
                flag_only.collect().write_parquet(output_path)
                logger.info(
                    f"Saving daily flagged {job_title} transactions succeed.")
            except Exception as e:
                logger.error(
                    f"Error saving daily flagged {job_title} transactions: {e}")
                logger.error(f"Flag {job_title} Process terminated.")
                return
        else:
            logger.info(
                f"There is no daily flagged {job_title} transaction found.")
            logger.info("Process finished")
    except Exception as e:
        logger.error(
            f"Error calculation flagged {job_title} transactions: {e}.")
        logger.error(f"Flag {job_title} Process terminated.")
        return
