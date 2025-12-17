import yaml
from pathlib import Path
from typing import List
from pydantic import BaseModel, ValidationError
import polars as pl
from .constant import CONFIG_FILE_PATH
from .logger import logger


# Shared settings block (the anchor)
class ColIdentifier(BaseModel):
    non_qr_hist_path: str
    transaction_date: str
    account_number: str
    transaction_status: str
    transaction_state: str
    transaction_category: str
    no_referensi: str
    temp_date_col: str
    flag_col: str
    usecols: List[str]
    n_days: int
    null_values: List[str]


# Process-specific configurations
class FlagCOnfig(ColIdentifier, frozen=True):
    output_path: Path | str


# Non-QR 5 mio in a one transaction
class ParamsNonQRConfig(ColIdentifier, frozen=True):
    rolling_window: int
    threshold: int


class DateConfig(BaseModel, frozen=True):
    date_file_format: str
    datetime_format: str


class AppConfig(BaseModel, frozen=True):
    flag: FlagCOnfig
    params_non_qr_5mio: ParamsNonQRConfig
    date: DateConfig
    dtypes: dict[str, str]


dtype_map = {
    "Utf8": pl.Utf8,
    "Int8": pl.Int8,
    "Float64": pl.Float64,
    "Datetime": pl.Datetime,
}


def load_constants(
    path: Path | str = CONFIG_FILE_PATH
) -> AppConfig | None:
    try:
        with open(path, "r") as f:
            data = yaml.safe_load(f)
        return AppConfig(**data)

    except ValidationError as e:
        logger.error(f"Parameter validation failed: {e}")
        return None

    except Exception as e:
        logger.exception(f"Error loading params: {e}")
        return None


CONSTS = load_constants()
if CONSTS is None:
    raise RuntimeError("Failed to load config.")

dtypes = {k: dtype_map[v] for k, v in CONSTS.dtypes.items()}
