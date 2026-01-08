import yaml
from pathlib import Path
from typing import List
from pydantic import BaseModel, ValidationError
import polars as pl
from .constant import CONFIG_FILE_PATH
from .logger import logger


class JobConfig(BaseModel):
    title: str


# Shared settings block (the anchor)
class ColIdentifier(BaseModel):
    hist_path: str
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


# TF Online 50 mio early in-a-day parameters
class ParamsTFOnlineConfig(ColIdentifier, frozen=True):
    rolling_window: int
    threshold: int
    time_range_start: int
    time_range_end: int


class DateConfig(BaseModel, frozen=True):
    date_file_format: str
    datetime_format: str


class AppConfig(BaseModel, frozen=True):
    job: JobConfig
    flag: FlagCOnfig
    params_tf_online_50mio_e: ParamsTFOnlineConfig
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
