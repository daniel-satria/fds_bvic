import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
from .logger import logger
from .models import CONSTS


DROP_COLS = ["flag_10min", "flag_5mio"]


def is_empty(lf: pl.LazyFrame) -> bool:
    return lf.limit(1).collect().height == 0


def flag_non_qr_5mio(
    hist_path: Path | str = CONSTS.flag.non_qr_hist_path,
    output_path: Path | str = CONSTS.flag.output_path,
    transaction_date_col: str = CONSTS.flag.transaction_date,
    account_number_col: str = CONSTS.flag.account_number,
    no_referensi_col: str = CONSTS.flag.no_referensi,
    flag_col: str = CONSTS.flag.flag_col,
    rolling_window: int = CONSTS.params_non_qr_5mio.rolling_window,
    threshold: int = CONSTS.params_non_qr_5mio.threshold,
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
    logger.info("Processing Flagged transfer online data...")
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        logger.info("Loading historical transfer online data...")
        df = pl.scan_parquet(hist_path)
        df = df.drop(DROP_COLS, strict=False)

        logger.info(
            f"Total tf_online records: {df.select(pl.len()).collect()[0, 0]}")
        logger.info("Succeeded loading historical transfer online data.")
    except Exception as e:
        logger.error(f"Error loading historical transfer online data: {e}.")
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

        # Step 3: Join sums back and filter >= 50M
        df_flagged_accounts = (
            df_rolled
            .join(
                df_sums,
                on=no_referensi_col
            ).filter(pl.col("rolling_24h_sum") >= threshold))

        # Step 4: EXTRACT KEY (these transactions exceeded 50M)
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
