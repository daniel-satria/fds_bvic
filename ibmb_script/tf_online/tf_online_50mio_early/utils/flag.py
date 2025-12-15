import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
from .logger import logger
from .models import CONSTS


DROP_COLS = ["flag_5min", "flag_10min", "flag_50mio"]


def is_empty(lf: pl.LazyFrame) -> bool:
    return lf.limit(1).collect().height == 0


def filter_date(
    df: pl.DataFrame,
    time_start: int = CONSTS.params_tf_online_50mio_e.time_range_start,
    time_end: int = CONSTS.params_tf_online_50mio_e.time_range_start,
    n_days: int = CONSTS.params_tf_online_50mio_e.n_days,
    transaction_date_col: str = CONSTS.flag.transaction_date,
    temp_date_col: str = CONSTS.flag.temp_date_col,
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
            pl.col(transaction_date_col).dt.date().alias(temp_date_col)
        )
        df = df.filter(
            (pl.col(temp_date_col) >= cutoff_date) &
            (pl.col(transaction_date_col).dt.hour().is_between(
                time_start,
                time_end))
        )
        return df
    except Exception as e:
        logger.error(f"Error filtering data: {e}")


def flag_tf_online_50m_early(
    tf_online_hist_path: Path | str = CONSTS.flag.tf_online_hist_path,
    output_path: Path | str = CONSTS.flag.output_path,
    transaction_date_col: str = CONSTS.flag.transaction_date,
    account_number_col: str = CONSTS.flag.account_number,
    no_referensi_col: str = CONSTS.flag.no_referensi,
    flag_col: str = CONSTS.flag.flag_col,
    threshold: int = CONSTS.params_tf_online_50mio_e.threshold,
) -> None:
    """
    Count transactions based on rolling window time frame.
    Params:
    -------
    tf_online_hist_path: Path | str
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
    logger.info("Processing Flagged transfer online data...")
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        logger.info("Loading historical transfer online data...")
        df = pl.scan_parquet(tf_online_hist_path)
        df = df.drop(DROP_COLS, strict=False)

        logger.info(
            f"Total tf_online records: {df.select(pl.len()).collect()[0, 0]}")
        logger.info("Succeeded loading historical transfer online data.")
    except Exception as e:
        logger.error(f"Error loading historical transfer online data: {e}.")
        logger.error(
            "Flag transfer online 50 millions IDR Process terminated.")
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
        logger.error(
            "Flag transfer online 50 millions IDR Process terminated.")
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
                (pl.col(f"{transaction_date_col}_r") >= pl.col(transaction_date_col) - pl.duration(hours=24)) &
                (pl.col(f"{transaction_date_col}_r")
                 <= pl.col(transaction_date_col))
            )
        )

        # Step 2: Group sums on the original transaction
        df_sums = (
            df_rolled
            .group_by(no_referensi_col)
            .agg(pl.sum("transaction_amount_r").alias("rolling_24h_sum"))
        )

        # Step 3: Join sums back and filter >= 50M
        df_flagged_accounts = (
            df_rolled
            .join(df_sums, on=no_referensi_col)
            .filter(pl.col("rolling_24h_sum") >= threshold)
        )

        # Step 4: Extract keys (these transactions exceeded 50M)
        keys = (
            df_flagged_accounts
            .select([account_number_col, no_referensi_col])
            .unique()
        )

        # Step 5: Returns only rows that match the keys
        tf_online_flag_only = (
            df.join(
                keys,
                on=[account_number_col, no_referensi_col],
                how="semi"
            ).with_columns(pl.lit(1).cast(pl.Int8).alias(flag_col))
        )

    except Exception as e:
        logger.error(
            f"Error calculation flagged transfer online 50 millions IDR: {e}.")
        logger.error(
            "Flag transfer online 50 millions IDR Process terminated.")
        return

    if not is_empty(tf_online_flag_only):
        try:
            rows = tf_online_flag_only.select(pl.len()).collect()[0, 0]
            logger.info(f"Found {rows} flagged records.")
            logger.info(
                "Saving daily flagged transfer online transactions...")
            logger.info(f"Saving into {output_path}.")
            tf_online_flag_only.collect().write_parquet(output_path)
            logger.info(
                "Saving daily flagged transfer online transactions succeed.")
        except Exception as e:
            logger.error(
                f"Error saving daily flagged transfer online transactions: {e}")
            logger.error(
                "Flag transfer online 50 millions IDR Process terminated.")
            return
    else:
        logger.info(
            "There is no daily flagged transfer online 50 millions IDR transaction found.")
        logger.info("Process finished")
