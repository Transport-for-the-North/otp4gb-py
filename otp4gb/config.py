"""Functionality for handling the YAML config file."""
from __future__ import annotations

import datetime
import json
import logging
import os
import pathlib
import sys
from typing import NamedTuple, Optional

import pydantic

from otp4gb.config_base import BaseConfig
from otp4gb import cost, routing


ROOT_DIR = os.path.dirname(os.path.abspath(sys.argv[0]))
BIN_DIR = os.path.abspath("bin")
CONF_DIR = os.path.abspath("config")
ASSET_DIR = os.path.join(ROOT_DIR, "assets")
LOG_DIR = os.path.join(ROOT_DIR, "logs")

# if you're running on a virtual machine (no virtual memory/page disk) this must not exceed the total amount of RAM.
PREPARE_MAX_HEAP = os.environ.get("PREPARE_MAX_HEAP", "25G")
SERVER_MAX_HEAP = os.environ.get("SERVER_MAX_HEAP", "25G")
LOG = logging.getLogger(__name__)


class Bounds(NamedTuple):
    """Bounding box."""

    min_lat: float
    min_lon: float
    max_lat: float
    max_lon: float

    @classmethod
    def from_dict(cls, data: dict[str, float]) -> Bounds:
        values = []
        missing = []
        invalid = []
        for nm in cls._fields:
            if nm not in data:
                missing.append(nm)
            else:
                try:
                    values.append(float(data[nm]))
                except ValueError:
                    invalid.append(str(data[nm]))

        if missing:
            raise ValueError(f"missing values: {', '.join(missing)}")
        if invalid:
            raise TypeError(f"invalid values: {', '.join(invalid)}")

        return cls(*values)


class TimePeriod(pydantic.BaseModel):
    """Data required for a single time period."""

    name: str
    travel_time: datetime.time
    search_window_minutes: Optional[int] = None


class ProcessConfig(BaseConfig):
    """Class for managing (and parsing) the YAML config file."""

    date: datetime.date
    extents: Bounds
    osm_file: str
    gtfs_files: list[str]
    time_periods: list[TimePeriod]
    modes: list[list[routing.Mode]]
    generalised_cost_factors: cost.GeneralisedCostFactors
    centroids: str
    destination_centroids: Optional[str] = None
    iterinary_aggregation_method: cost.AggregationMethod = cost.AggregationMethod.MEAN
    max_walk_distance: int = 2500
    number_of_threads: pydantic.conint(ge=0, le=10) = 0
    no_server: bool = False
    crowfly_max_distance: Optional[float] = None

    # Makes a classmethod not recognised by pylint, hence disabling self check
    @pydantic.validator("extents", pre=True)
    def _extents(cls, value):  # pylint: disable=no-self-argument
        if not isinstance(value, dict):
            return value
        return Bounds.from_dict(value)


def load_config(folder: pathlib.Path) -> ProcessConfig:
    file = pathlib.Path(folder) / "config.yml"
    return ProcessConfig.load_yaml(file)


def write_build_config(folder: pathlib.Path, date: datetime.date) -> None:
    """Load default build config values, update and write to graph folder.

    Parameters
    ----------
    folder : pathlib.Path
        Folder to save the build config to.
    date : datetime.date
        Date of the transit data.
    """
    folder = pathlib.Path(folder)
    filename = "build-config.json"

    default_path = pathlib.Path(CONF_DIR) / filename
    if default_path.is_file():
        LOG.info("Loading default build config from: %s", default_path)
        with open(default_path, "rt") as file:
            data = json.load(file)
    else:
        data = {}

    data["transitServiceStart"] = (date - datetime.timedelta(1)).isoformat()
    data["transitServiceEnd"] = (date + datetime.timedelta(1)).isoformat()

    config_path = folder / filename
    with open(config_path, "wt") as file:
        json.dump(data, file)
    LOG.info("Written build config: %s", config_path)
