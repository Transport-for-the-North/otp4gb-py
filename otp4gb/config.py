"""Functionality for handling the YAML config file."""
from __future__ import annotations

import datetime
import json
import logging
import os
import pathlib
from typing import Optional

import pydantic
import caf.toolkit

from otp4gb import cost, parameters, routing, util
from otp4gb.centroids import Bounds


ROOT_DIR = pathlib.Path().absolute()
BIN_DIR = ROOT_DIR / "bin"
CONF_DIR = ROOT_DIR / "config"
ASSET_DIR = ROOT_DIR / "assets"
LOG_DIR = ROOT_DIR / "logs"

# if you're running on a virtual machine (no virtual memory/page disk)
# this must not exceed the total amount of RAM.
PREPARE_MAX_HEAP = os.environ.get("PREPARE_MAX_HEAP", "25G")
SERVER_MAX_HEAP = os.environ.get("SERVER_MAX_HEAP", "25G")
LOG = logging.getLogger(__name__)


# Pylint incorrectly flags no-member for pydantic.BaseModel
class TimePeriod(pydantic.BaseModel):  # pylint: disable=no-member
    """Data required for a single time period."""

    name: str
    travel_time: datetime.time
    search_window_minutes: Optional[int] = None


class IsochroneParams(pydantic.BaseModel):
    """
    Information required for traveltime isochrone request
    """
    departure_date_time: datetime.datetime
    cutoffs_seconds: list[int]
    modes: list[list[routing.Mode]]


# class ProcessIsoConfig(caf.toolkit.BaseConfig):
#     """Class for managing & parsing the YAML config file for Isochrones"""
#
#     departure_datetime: datetime.datetime
#     cutoffs = list[int]
#     modes = list[Any]
#
#     # @pydantic.validator("departure_datetime", pre=True)
#     # def _check_datetime(cls, value: str):
#     #     # pylint: disable=no-self-argument
#     #     if not isinstance(value, datetime.datetime):
#     #         # if departure_date_time is incorrect format, below will flag
#     #         value = datetime.datetime.strptime(value, "%Y-%m-%d %H:%M")
#     #         return value
#     #     else:
#     #         raise ValueError("""Expected non-zero length string in datetime""" +
#     #                          """ format '%Y-%m-%d %H:%M', got %s""" % type(value))
#
#     @classmethod
#     def _load_yaml(cls, file: pathlib.Path) -> ProcessIsoConfig:
#         """Loads in isochrone YAML config"""
#         with open(file, "r") as file:
#             config = yaml.safe_load(file)
#             return config
#

class ProcessConfig(caf.toolkit.BaseConfig):
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
    no_server: Optional[bool] = False
    hostname: Optional[str] = "localhost"
    port: Optional[int] = 8080
    crowfly_max_distance: Optional[float] = None
    ruc_lookup: Optional[parameters.RUCLookup] = None
    irrelevant_destinations: Optional[parameters.IrrelevantDestinations] = None
    previous_trips: Optional[parameters.PreviousTrips] = None
    # TODO(JH): Create ProcessIsoConfig class
    # Isochrone params below
    iso_centroids: str
    iso_lat_col: str
    iso_long_col: str
    iso_id_col: str
    iso_datetime: datetime.datetime
    cutoffs: list[int]
    iso_modes: list[list[routing.Mode]]

    # Makes a classmethod not recognised by pylint, hence disabling self check
    @pydantic.validator("extents", pre=True)
    def _extents(cls, value):  # pylint: disable=no-self-argument
        if not isinstance(value, dict):
            return value
        return Bounds.from_dict(value)

    @pydantic.validator("destination_centroids")
    def _empty_str(cls, value: str | None):
        # pylint: disable=no-self-argument
        """Return None if string is empty, otherwise return string"""
        if value is None or len(value) == 0:
            return None

        return value

    @pydantic.validator("crowfly_max_distance", pre=True)
    def _empty_distance(cls, value: str | None):
        # pylint: disable=no-self-argument
        """Returns None if string is empty, otherwise return string as float"""
        if value is None or len(value.replace(" ", "")) == 0:
            return None

        return float(value)

    @pydantic.validator("hostname", pre=True)
    def _check_hostname(cls, value: str | None):
        # pylint: disable=no-self-argument
        """Returns hostname 'localhost' unless `config.hostname` is specified"""
        if value is None or len(value) == 0:
            value = "localhost"
            return value
        else:
            return value

    @pydantic.validator("port", pre=True)
    def _check_port(cls, value: int | None):
        # pylint disable=no-self-argument
        """Returns 8080 port unless `config.port` is specified"""
        if value is None or len(str(value).replace(" ", "")) == 0:
            value = 8080
            return value
        else:
            return value

    @pydantic.validator("iso_datetime", pre=True)
    def _check_datetime(cls, value: str):
        # pylint: disable=no-self-argument
        if not isinstance(value, datetime.datetime):
            # if iso_datetime is incorrect format, below will flag
            value = datetime.datetime.strptime(value, "%Y-%m-%d %H:%M")
            return value
        else:
            raise ValueError("""Expected non-zero length string in datetime with""" +
                             """ format '%Y-%m-%d %H:%M', got %s""" % value)

    @pydantic.validator("cutoffs")
    def _check_cutoffs(cls, cutoff_list: list):
        # pylint: disable=no-self-argument
        """Converts supplied cutoffs (int - secs) to datetime.timedelta (secs)"""
        for i, cutoff in enumerate(cutoff_list):
            if not isinstance(cutoff, int):
                cutoff = int(cutoff)
                cutoff_list[i] = cutoff

            if not isinstance(cutoff, datetime.timedelta):
                cutoff = datetime.timedelta(seconds=cutoff)
                cutoff_list[i] = cutoff

        return cutoff_list


def load_config(folder: pathlib.Path) -> ProcessConfig:
    """Read process config file."""
    file = pathlib.Path(folder) / "config.yml"
    return ProcessConfig.load_yaml(file)


def write_build_config(
    folder: pathlib.Path, date: datetime.date, encoding: str = util.TEXT_ENCODING
) -> None:
    """Load default build config values, update and write to graph folder.

    Parameters
    ----------
    folder : pathlib.Path
        Folder to save the build config to.
    date : datetime.date
        Date of the transit data.
    encoding : str, default `util.TEXT_ENCODING`
        Encoding to use when reading and writing config file.
    """
    folder = pathlib.Path(folder)
    filename = "build-config.json"

    default_path = pathlib.Path(CONF_DIR) / filename
    if default_path.is_file():
        LOG.info("Loading default build config from: %s", default_path)
        with open(default_path, "rt", encoding=encoding) as file:
            data = json.load(file)
    else:
        data = {}

    data["transitServiceStart"] = (date - datetime.timedelta(1)).isoformat()
    data["transitServiceEnd"] = (date + datetime.timedelta(1)).isoformat()

    config_path = folder / filename
    with open(config_path, "wt", encoding=encoding) as file:
        json.dump(data, file)
    LOG.info("Written build config: %s", config_path)


# TODO(MB) Add functions for writing other configs that OTP accepts
# router-config, otp-config
