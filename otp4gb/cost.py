# -*- coding: utf-8 -*-
"""
    Module for calculating the costs between OD pairs.
"""

##### IMPORTS #####
from __future__ import annotations

import datetime
import enum
import functools
import io
import logging
import pathlib
import threading
from typing import Any, Iterator, Optional

import numpy as np
import pandas as pd
import pydantic
import tqdm

from otp4gb import routing, util
from otp4gb.parameters import CalculationParameters

##### CONSTANTS #####
LOG = logging.getLogger(__name__)


##### CLASSES #####
# Pylint incorrectly flags no-member for pydantic.BaseModel
class CostResults(pydantic.BaseModel):  # pylint: disable=no-member
    """Cost results from OTP saved to responses JSON file."""

    origin: routing.Place
    destination: routing.Place
    plan: Optional[routing.Plan]
    error: Optional[routing.RoutePlanError]
    request_url: str


class AggregationMethod(enum.StrEnum):
    """Method to use when aggregating itineraries."""

    MEAN = enum.auto()
    MEDIAN = enum.auto()


class GeneralisedCostFactors(pydantic.BaseModel):  # pylint: disable=no-member
    """Parameters for calculating the generalised cost."""

    wait_time: float
    transfer_number: float
    walk_time: float
    transit_time: float
    walk_distance: float
    transit_distance: float


##### FUNCTIONS #####
def calculate_costs(
    parameters: CalculationParameters,
    generalised_cost_parameters: GeneralisedCostFactors,
    response_file: io.TextIOWrapper | None = None,
    lock: threading.Lock | None = None,
) -> dict[str, Any]:
    """Calculate cost between 2 zones using OTP.

    Parameters
    ----------
    parameters : CalculationParameters
        Parameters for OTP.
    generalised_cost_parameters : GeneralisedCostParameters
        Factors and other parameters for calculating the
        generalised cost.
    response_file : io.TextIOWrapper, optional
        File to save the JSON response too, if None
        JSON responses won't be saved to a file.
    lock : threading.Lock, optional
        Lock object to avoid race conditions when writing
        to `response_file`, ignored if `response_file` is None
        but mandatory otherwise.

    Returns
    -------
    dict[str, Any]
        Cost statistics for the cost matrix.
    """
    rp_params = routing.RoutePlanParameters(
        fromPlace=f"{parameters.origin.lat}, {parameters.origin.lon}",
        toPlace=f"{parameters.destination.lat}, {parameters.destination.lon}",
        date=parameters.datetime.date(),
        time=parameters.datetime.time(),
        mode=parameters.modes,
        arriveBy=parameters.arrive_by,
        searchWindow=parameters.searchWindow,
        wheelchair=parameters.wheelchair,
        maxWalkDistance=parameters.max_walk_distance,
    )
    url, result = routing.get_route_itineraries(parameters.server_url, rp_params)

    if result.plan is not None:
        result.plan.date = result.plan.date.astimezone(parameters.datetime.tzinfo)

        for itinerary in result.plan.itineraries:
            itinerary.startTime = itinerary.startTime.astimezone(
                parameters.datetime.tzinfo
            )
            itinerary.endTime = itinerary.endTime.astimezone(parameters.datetime.tzinfo)

            # Update itineraries with generalised cost
            generalised_cost(itinerary, generalised_cost_parameters)

    cost_res = CostResults(
        origin=parameters.origin,
        destination=parameters.destination,
        plan=result.plan,
        error=result.error,
        request_url=url,
    )
    if response_file is not None:
        if lock is None:
            raise ValueError("lock is required if response_file is not None")

        with lock:
            response_file.write(cost_res.model_dump_json() + "\n")

    return _matrix_costs(cost_res)


def generalised_cost(
    itinerary: routing.Itinerary, factors: GeneralisedCostFactors
) -> None:
    """Calculate the generalised cost and update value in `itinerary`.

    Times given in `itinerary` shoul be in seconds and are converted
    to minutes for the calculation.
    Distances given in `itinerary` should be in metres and are converted
    to km for the calculation.

    Parameters
    ----------
    itinerary : routing.Itinerary
        Route itinerary.
    factors : GeneralisedCostParameters
        Factors for generalised cost calculation.
    """
    wait_time = itinerary.waitingTime * factors.wait_time
    transfer_penalty = itinerary.transfers * factors.transfer_number
    walk_time = itinerary.walkTime / 60 * factors.walk_time
    transit_time = itinerary.transitTime / 60 * factors.transit_time
    walk_distance = itinerary.walkDistance / 1000 * factors.walk_distance

    transit_distance = sum(
        l.distance for l in itinerary.legs if l.mode in routing.Mode.transit_modes()
    )
    transit_distance = transit_distance / 1000 * factors.transit_distance

    itinerary.generalised_cost = (
        wait_time
        + transfer_penalty
        + walk_time
        + transit_time
        + walk_distance
        + transit_distance
    )


def _matrix_costs(result: CostResults) -> dict:
    matrix_values: dict[str, str | int | float | datetime.datetime] = {
        "origin": result.origin.name,
        "destination": result.destination.name,
        "origin_id": result.origin.id,
        "destination_id": result.destination.id,
        "origin_zone_system": result.origin.zone_system,
        "destination_zone_system": result.destination.zone_system,
    }

    if result.plan is None:
        return matrix_values

    matrix_values["number_itineraries"] = len(result.plan.itineraries)
    if matrix_values["number_itineraries"] == 0:
        return matrix_values

    stats = [
        "duration",
        "travel_distance",
        "walkTime",
        "transitTime",
        "waitingTime",
        "walkDistance",
        "otp_generalised_cost",
        "transfers",
        "generalised_cost",
    ]
    for s in stats:
        values = []
        for it in result.plan.itineraries:
            if s == "travel_distance":
                # Calculate total travel distance because it isn't provided
                val = functools.reduce(lambda x, y: x + y.distance, it.legs, 0.0)

            else:
                val = getattr(it, s, np.nan)

            values.append(val)

        matrix_values[f"mean_{s}"] = np.nanmean(values)

        if matrix_values["number_itineraries"] > 1:
            matrix_values[f"median_{s}"] = np.nanmedian(values)
            matrix_values[f"min_{s}"] = np.nanmin(values)
            matrix_values[f"max_{s}"] = np.nanmax(values)
            matrix_values[f"num_nans_{s}"] = np.sum(np.isnan(values))

    matrix_values["min_startTime"] = min(i.startTime for i in result.plan.itineraries)
    matrix_values["max_startTime"] = max(i.startTime for i in result.plan.itineraries)
    matrix_values["min_endTime"] = min(i.endTime for i in result.plan.itineraries)
    matrix_values["max_endTime"] = max(i.endTime for i in result.plan.itineraries)

    return matrix_values


def _write_matrix_files(
    data: list[dict[str, float]],
    matrix_file: pathlib.Path,
    aggregation: AggregationMethod,
) -> None:
    matrix = pd.DataFrame(data)

    metrics_file = matrix_file.with_name(matrix_file.stem + "-metrics.csv")
    matrix.to_csv(metrics_file, index=False)
    LOG.info("Written cost metrics to %s", metrics_file)

    try:
        LOG.info(
            "Minimum departure time from all responses {:%x %X %Z}".format(
                matrix["min_startTime"].min()
            )
        )
        LOG.info(
            "Maximum arrival time from all responses {:%x %X %Z}".format(
                matrix["max_endTime"].max()
            )
        )
    except KeyError as error:
        LOG.warning("Start / end times unavailable in matrix: %s", error)

    gen_cost_column = f"{aggregation}_generalised_cost"

    if gen_cost_column not in matrix.columns:
        LOG.error(
            "Generalised cost column (%s) not found in "
            "matrix metrics, so skipping matrix creation",
            gen_cost_column,
        )
        return

    # Pivot generalised cost
    gen_cost = matrix.pivot(
        index="origin_id", columns="destination_id", values=gen_cost_column
    )
    gen_cost.to_csv(matrix_file)
    LOG.info("Written %s generalised cost matrix to %s", aggregation, matrix_file)


def _build_cost_matrix_internal(
    jobs: list[CalculationParameters],
    generalised_cost_parameters: GeneralisedCostFactors,
    workers: int,
    responses_fo: io.TextIOWrapper | None = None,
    lock: threading.Lock | None = None,
) -> list[dict[str, Any]]:
    """Run `calculate_costs` for all jobs, using multiple threads."""
    iterator: Iterator[dict[str, Any]] = util.multithread_function(
        workers,
        calculate_costs,
        jobs,
        dict(
            response_file=responses_fo,
            lock=lock,
            generalised_cost_parameters=generalised_cost_parameters.model_copy(),
        ),
    )

    matrix_data = []
    for res in tqdm.tqdm(
        iterator,
        total=len(jobs),
        desc="Calculating costs",
        dynamic_ncols=True,
        smoothing=0,
    ):
        matrix_data.append(res)

    return matrix_data


def build_cost_matrix(
    jobs: list[CalculationParameters],
    matrix_file: pathlib.Path,
    generalised_cost_parameters: GeneralisedCostFactors,
    aggregation_method: AggregationMethod,
    workers: int = 0,
    write_raw_responses: bool = True,
) -> None:
    """Create cost matrix for all zone to zone pairs.

    Parameters
    ----------
    jobs : list[CalculationParameters]
        List of requests to send to OTP.
    matrix_file : pathlib.Path
        Path to save the cost matrix to.
    generalised_cost_parameters : GeneralisedCostParameters
        Factors and other parameters for calculating the
        generalised cost.
    aggregation_method : AggregationMethod
        Aggregation method used for generalised cost matrix.
    workers : int, default 0
        Number of threads to create during calculations.
    write_raw_responses : bool, default True
        If True, write raw responses data to a JSON lines file with
        the path "{matrix_file}-response_data.jsonl".
    """
    LOG.info("Calculating costs for %s", matrix_file.name)

    if write_raw_responses:
        response_file = matrix_file.with_name(f"{matrix_file.stem}-response_data.jsonl")
        with open(response_file, "wt", encoding=util.TEXT_ENCODING) as responses:
            matrix_data = _build_cost_matrix_internal(
                jobs=jobs,
                generalised_cost_parameters=generalised_cost_parameters,
                workers=workers,
                responses_fo=responses,
                lock=threading.Lock(),
            )

    else:
        matrix_data = _build_cost_matrix_internal(
            jobs=jobs,
            generalised_cost_parameters=generalised_cost_parameters,
            workers=workers,
        )

    LOG.info("Written responses to %s", response_file)
    _write_matrix_files(matrix_data, matrix_file, aggregation_method)


def iterate_responses(response_file: io.TextIOWrapper) -> Iterator[CostResults]:
    """Iterate through and parse responses JSON lines file.

    Parameters
    ----------
    response_file : io.TextIOWrapper
        JSON lines file to read from.

    Yields
    ------
    CostResults
        Cost results for a single OD pair.
    """
    for line in response_file:
        yield CostResults.model_validate_json(line)


def cost_matrix_from_responses(
    *responses_files: pathlib.Path,
    matrix_file: pathlib.Path,
    aggregation_method: AggregationMethod,
) -> None:
    """Create cost matrix CSV from responses (upto multiple) JSON lines files.

    Parameters
    ----------
    positional args (responses_files) : Pathlib.Path
       pathlib.Path(s) to JSON lines file(s) containing `CostResults`.
    matrix_file : pathlib.Path
        Path to CSV file to output cost metrics to.
    aggregation_method : AggregationMethod
        Aggregation method used for generalised cost matrix.
    """

    matrix_data = []

    LOG.info("Compiling %s OTP response files", len(responses_files))
    for responses_file in responses_files:
        LOG.info("Processing file %s", responses_file)
        with open(responses_file, "rt", encoding=util.TEXT_ENCODING) as responses:
            for line in tqdm.tqdm(
                responses, desc="Calculating cost matrix", dynamic_ncols=True
            ):
                results = CostResults.model_validate_json(line)
                # TODO(MB) Recalculate generalised cost if new parameters are provided
                matrix_data.append(_matrix_costs(results))

    _write_matrix_files(matrix_data, matrix_file, aggregation_method)
