import yaml
import polars as pl
from pathlib import Path
from typing import List
from pydantic import BaseModel, ValidationError, field_validator
from .constant import CONFIG_FILE_PATH
from .logger import logger


# Shared settings block (the anchor)
class JobConfig(BaseModel):
    title: str


class ColIdentifier(BaseModel):
    non_qr_hist_path: str
    transaction_date: str
    account_number: str
    transaction_status: str
    transaction_state: str
    transaction_category: str
    no_referensi: str
    temp_date_col: str
    flag_col: List[str]
    usecols: List[str]
    n_days: int
    null_values: List[str]


# Process-specific configurations
class HistoricalConfig(ColIdentifier, frozen=True):
    input_folder: str
    file_prefix: str
    file_suffix: str


class UpdateFlag10minConfig(ColIdentifier, frozen=True):
    daily_path_flag: Path | str
    flag_col: str


class UpdateFlag5mioConfig(ColIdentifier, frozen=True):
    daily_path_flag: Path | str
    flag_col: str


# Filter block
class FilterConfig(BaseModel, frozen=True):
    transaction_status: List[int]
    transaction_state: List[int]
    transaction_category: List[str]

    @field_validator('transaction_status', 'transaction_state', mode="before")
    def convert_to_int(cls, v):
        return [int(x) for x in v]


class DateConfig(BaseModel, frozen=True):
    date_file_format: str
    datetime_format: str


class AppConfig(BaseModel, frozen=True):
    job: JobConfig
    historical: HistoricalConfig
    update_flag_10min: UpdateFlag10minConfig
    update_flag_5mio: UpdateFlag5mioConfig
    filter: FilterConfig
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
