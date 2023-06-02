# -*- coding: utf-8 -*-
"""
    Module for calculating the costs between OD pairs.
"""

##### IMPORTS #####
# Standard imports
import datetime
import enum
import io
import itertools
import logging
import pathlib
import threading
import os
from typing import Any, Iterator, NamedTuple, Optional

# Third party imports
import geopandas as gpd
import numpy as np
import pandas as pd
import pydantic
import tqdm

# Local imports
from otp4gb import routing, util, centroids


##### CONSTANTS #####
LOG = logging.getLogger(__name__)
CROWFLY_DISTANCE_CRS = "EPSG:27700"

# TODO(MB) These shouldn't be done outside a function as they stop this module from being imported
# Name & Path to current run output folder to save compiled zone centroids
# output_dir_path = os.path.abspath(base_sys.argv[1])
# output_dir_name = os.path.basename(output_dir_path)


##### CLASSES #####
class CostSettings(NamedTuple):
    """Settings for the OTP routing."""

    server_url: str
    modes: list[routing.Mode]
    datetime: datetime.datetime
    arrive_by: bool = False
    search_window_seconds: Optional[int] = None
    wheelchair: bool = False
    max_walk_distance: float = 1000
    crowfly_max_distance: Optional[float] = None


class CalculationParameters(NamedTuple):
    """Parameters for `calculate_costs` function."""

    server_url: str
    modes: list[str]
    datetime: datetime.datetime
    origin: routing.Place
    destination: routing.Place
    arrive_by: bool = False
    searchWindow: Optional[int] = None
    wheelchair: bool = False
    max_walk_distance: float = 1000
    crowfly_max_distance: Optional[float] = None


class CostResults(pydantic.BaseModel):
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


class GeneralisedCostFactors(pydantic.BaseModel):
    """Parameters for calculating the generalised cost."""

    wait_time: float
    transfer_number: float
    walk_time: float
    transit_time: float
    walk_distance: float
    transit_distance: float


##### FUNCTIONS #####
def _calculate_distance_matrix(
    origins: gpd.GeoDataFrame, destinations: gpd.GeoDataFrame, crs: str
) -> pd.DataFrame:
    """Calculate distances between all `origins` and `destinations`.

    Geometries are converted to `crs` before calculating distance.
    """
    distances = pd.DataFrame(
        {
            "origin": np.repeat(origins.index, len(destinations)),
            "destination": np.tile(destinations.index, len(origins)),
        }
    )

    origins = origins.to_crs(crs)
    destinations = destinations.to_crs(crs)

    for name, data in (("origin", origins), ("destination", destinations)):
        distances = distances.merge(
            data["geometry"],
            left_on=name,
            how="left",
            validate="m:1",
            right_index=True,
        )
        distances.rename(columns={"geometry": f"{name}_centroid"}, inplace=True)

    distances.loc[:, "distance"] = gpd.GeoSeries(distances["origin_centroid"]).distance(
        gpd.GeoSeries(distances["destination_centroid"])
    )

    return distances.set_index(["origin", "destination"])["distance"]


def build_calculation_parameters(
    zones: centroids.ZoneCentroids,
    settings: CostSettings,
    crowfly_max_distance: Optional[float] = None,
    crowfly_distance_crs: str = CROWFLY_DISTANCE_CRS,
) -> list[CalculationParameters]:
    """Build a list of parameters for running `calculate_costs`.

    Parameters
    ----------
    zone_centroids :ZoneCentroids
        Positions of zones to calculate costs between.
    settings : CostSettings
        Additional settings for calculating the costs.
    crowfly_max_distance : float, optional
        Any OD pairs with crow-fly distances greater than this
        won't be included in the list of queries.
    crowfly_distance_crs : str, default `CROWFLY_DISTANCE_CRS`
        Coordinate reference system to convert geometries to
        when calculating crow-fly distances.

    Returns
    -------
    list[CalculationParameters]
        Parameters to run `calculate_costs`.
    """

    def row_to_place(row: pd.Series) -> routing.Place:
        point = row.at["geometry"]

        return routing.Place(
            name=str(row.at[zones.columns.name]),
            id=str(row.name),
            zone_system=str(row.at[zones.columns.system]),
            lon=point.x,
            lat=point.y,
        )

    LOG.info("Building cost calculation parameters")

    zone_columns = [zones.columns.name, zones.columns.system, "geometry"]
    origins = zones.origins.set_index(zones.columns.id)[zone_columns]

    if zones.destinations is None:
        destinations = None
    else:
        destinations = zones.destinations.set_index(zones.columns.id)[zone_columns]

    if crowfly_max_distance is not None and crowfly_max_distance > 0:
        distances = _calculate_distance_matrix(
            origins,
            origins if destinations is None else destinations,
            crowfly_distance_crs,
        )
    else:
        distances = None

    params = []
    for origin, destination in itertools.product(origins.index, origins.index):
        if origin == destination and destinations is None:
            continue

        if distances is not None:
            distance = distances.at[origin, destination]
            if distance > crowfly_max_distance:
                LOG.debug(
                    "Excluding %s - %s because crow-fly distance (%.0f) "
                    "is greater than max distance parameter (%.0f)",
                    origin,
                    destination,
                    distance,
                    crowfly_max_distance,
                )
                continue

        params.append(
            CalculationParameters(
                server_url=settings.server_url,
                modes=[str(m) for m in settings.modes],
                datetime=settings.datetime,
                origin=row_to_place(origins.loc[origin]),
                destination=row_to_place(
                    origins.loc[destination]
                    if destinations is None
                    else destinations.loc[destination]
                ),
                arrive_by=settings.arrive_by,
                searchWindow=settings.search_window_seconds,
                wheelchair=settings.wheelchair,
                max_walk_distance=settings.max_walk_distance,
            )
        )

    return params


# TODO(MB) Integrate LSOA analysis with the new build_calculation_parameters method
def _supersceded_build_calculation_parameters(
    zone_centroids: gpd.GeoDataFrame,  # TODO(MB) Custom centroids class
    centroids_columns: centroids.ZoneCentroidColumns,
    settings: CostSettings,
    crowfly_max_distance: Optional[float] = None,
) -> list[CalculationParameters]:
    global zone_centroids_BnG
    """Build a list of parameters for running `calculate_costs`.

    Parameters
    ----------
    zone_centroids : gpd.GeoDataFrame
        Positions of zones to calculate costs between.
    centroids_columns : centroids.ZoneCentroidColumns
        Names of the columns in `zone_centroids`.
    settings : CostSettings
        Additional settings for calculating the costs.

    Returns
    -------
    list[CalculationParameters]
        Parameters to run `calculate_costs`.
    """

    #### LSOA TRSE ####
    filter_radius = (
        crowfly_max_distance  # crowfly_max_distance specified within config.yml
    )

    def row_to_place(row: pd.Series) -> routing.Place:
        return routing.Place(
            name=row[centroids_columns.name],
            id=row.name,
            zone_system=row[centroids_columns.system],
            lon=row["geometry"].y,
            lat=row["geometry"].x,
        )

    LOG.info("Building cost calculation parameters")
    zone_centroids = zone_centroids.set_index(centroids_columns.id)[
        [centroids_columns.name, centroids_columns.system, "geometry"]
    ]

    # Print statistics for user
    print("\nLSOA analysis maximum crow fly trip distance: {}".format(filter_radius))

    # Load LSOA destination relevance and Rural Urban Classification (RUC) lookups
    LSOA_relevance_path = os.path.join(os.getcwd(), "Data", "LSOA_amenities.csv")
    LSOA_RUC_path = os.path.join(os.getcwd(), "Data", "compiled_LSOA_area_types.csv")
    LSOA_relevance = pd.read_csv(LSOA_relevance_path)
    LSOA_RUC_types = pd.read_csv(LSOA_RUC_path)

    # Set zone id"s as index, matching `zone_centroids`
    LSOA_relevance.set_index("LSOA11CD", inplace=True)
    LSOA_RUC_types.set_index("LSOA11CD", inplace=True)

    # Path to compiled zone centroids (within Data folder)
    compiled_centroids_path = os.path.join(
        os.getcwd(), output_dir_name, "compiled_zone_centroids_with_filter.csv"
    )

    # Define RUC weightings to be applied to maximum radius filter based on Zone origin RUC
    LSOA_RUC_weights = {
        "A1": 1,
        "B1": 1,
        "C1": 1,
        "C2": 1,
        "D1": 1.25,
        "D2": 1.25,
        "E1": 1.5,
        "E2": 1.5,
    }

    # Create GeoDataFrame of all OD pairs & calculate distances between them.
    if (filter_radius != 0) & (os.path.isfile(compiled_centroids_path) is False):
        print(
            "\nA zone centroids file `compiled_zone_centroids_with_filter.csv` could not be found in directory",
            output_dir_name,
        )
        print("Creating `compiled_zone_centroids_with_filter.csv` now.\n")

        # Create copy of zone_centroids to manipulate CRS (filter distance is metres (EPSG:27700))
        zone_centroids_BnG = zone_centroids.copy()
        zone_centroids_BnG = gpd.GeoDataFrame(zone_centroids_BnG)
        zone_centroids_BnG.to_crs("EPSG:27700", inplace=True)

        # For joining destination centroids later
        zone_centroids_BnG_raw = zone_centroids_BnG.copy()

        # Set crs to BnG [EPSG:27700] so distance calc is in metres.
        zone_centroids_BnG.to_crs("EPSG:27700")

        zone_centroids_BnG.drop(columns=["zone_system", "zone_name"], inplace=True)

        # Create a DataFrame of all possible OD combinations:
        origins = []
        destinations = []
        OD_pairs = []
        LOG.info(
            "Constructing trip distance GeoDataFrame and assessing relevant destination zones"
        )
        LSOA_ids = list(zone_centroids.index)

        # Create possible combinations (trips)
        length = 2  # An Origin & Destination
        x = list(range(len(zone_centroids)))  # Possible Origins and Destinations
        mesh = np.meshgrid(*([x] * length))
        result = np.vstack([y.flat for y in mesh]).T

        # Filter out irrelevant trips from total trips above.
        print(
            "\nAnalysing",
            len(result),
            "initial trips for destination relevance & same zone journeys.\n",
        )

        for i in tqdm.tqdm(zip(result)):
            o = i[0][0]
            d = i[0][1]

            # Ignore same zone journeys
            if o == d:
                continue

            # Check for relevance of Destination LSOA
            if LSOA_relevance.loc[LSOA_ids[d]]["totals"] > 0:
                # Destination LSOA contains at least 1 amenity, ergo is relevant
                origin = LSOA_ids[o]
                destin = LSOA_ids[d]

                origins.append(origin)
                destinations.append(destin)
                OD_pairs.append("_".join((origin, destin)))
            else:
                # Destination LSOA is not relevant
                continue

        LOG.info(
            "Removing: %s trips, leaving: %s trips remaining.",
            str((len(result) - len(origins))),
            str(len(origins)),
        )

        LOG.info("Constructing DataFrame of trips")

        # DF of all OD pairs
        OD_pairs = pd.DataFrame(
            data={
                "Origins": origins,
                "Destinations": destinations,
                "OD_pairs": OD_pairs,
            }
        )

        LOG.info("Merging Origins to geometries")

        # Join on zone centroid data for Origins
        zone_centroids_BnG = zone_centroids_BnG.merge(
            how="left",
            right=OD_pairs,
            left_on=zone_centroids_BnG.index,
            right_on="Origins",
        )

        # Re-classify dataframe as GeoDataFrame
        zone_centroids_BnG = gpd.GeoDataFrame(zone_centroids_BnG, crs="EPSG:27700")

        # Rename Origin centroids col to Origin_centroids
        zone_centroids_BnG.rename(
            columns={"geometry": "Origin_centroids"}, inplace=True
        )
        # Re-specify geometry
        zone_centroids_BnG.set_geometry(
            "Origin_centroids", inplace=True, crs="EPSG:27700"
        )

        LOG.info("Merging Destinations to geometries")

        # Join on zone centroids data for Destinations
        zone_centroids_BnG = zone_centroids_BnG.merge(
            how="left",
            right=zone_centroids_BnG_raw,
            left_on="Destinations",
            right_on=zone_centroids_BnG_raw.index,
        )
        # Re-classify dataframe as GeoDataFrame
        zone_centroids_BnG = gpd.GeoDataFrame(zone_centroids_BnG, crs="EPSG:27700")

        # Rename centroids to destination centroids
        zone_centroids_BnG.rename(
            columns={"geometry": "Destination_centroids"}, inplace=True
        )

        LOG.info("Calculating trip distances")
        # Work out distance between all OD pairs
        zone_centroids_BnG["distances"] = zone_centroids_BnG[
            "Origin_centroids"
        ].distance(zone_centroids_BnG["Destination_centroids"])

        # Print run stats for user
        print(
            "\nMaximum trip distance: {} \nMinimum trip distance: {}\n".format(
                str(round(max(zone_centroids_BnG["distances"]), 2)) + " metres.",
                str(round(min(zone_centroids_BnG["distances"]), 2)) + " metres.",
            )
        )

        # Short enough trips are the number of trips within the specified filter radius with Rural weighting applied
        # to the filter distance. Any trip distance greater than this can now be removed.
        short_enough_trips = len(
            zone_centroids_BnG[
                zone_centroids_BnG["distances"]
                < (filter_radius * LSOA_RUC_weights["E2"])
            ]
        )
        all_trips = len(zone_centroids_BnG)
        diff = str(all_trips - short_enough_trips)

        LOG.info("Removing " + str(diff) + " trips for being too far")
        LOG.info("Leaving: " + str(short_enough_trips) + " trips remaining.")
        zone_centroids_BnG = zone_centroids_BnG[
            zone_centroids_BnG["distances"] < filter_radius * LSOA_RUC_weights["E2"]
        ]
        # Likely spent a long time compiling and computing the above distances. Save it in case of crashes.
        # We can re-load above if it has already been compiled.

        # Save as .csv. Although a GeoDataFrame, we no longer require spatial information - only trip distances.
        print(
            "\nSaving compiled zone_centroids_BnG file as csv to:\n    ",
            compiled_centroids_path,
            "\n",
        )

        zone_centroids_BnG = zone_centroids_BnG[
            [
                "Origins",
                "Destinations",
                "OD_pairs",
                centroids_columns.name,
                centroids_columns.system,
                "distances",
            ]
        ]

        zone_centroids_BnG.to_csv(compiled_centroids_path)

    # Check if a maximum trip distance has been passed
    elif (filter_radius != 0) & (os.path.isfile(compiled_centroids_path) is True):
        # Compiled centroids has been found. Loading.
        print(
            "Existing compiled_zone_centroids file found. Loading the DataFrame\n",
            compiled_centroids_path,
            "\n",
        )
        zone_centroids_BnG = pd.read_csv(compiled_centroids_path)

    params = []
    if filter_radius == 0:
        # No filter radius applied - carry on as normal.
        LOG.info("No maximum crowfly trip distance applied - proceeding as normal")

        for o, d in tqdm.tqdm(
            itertools.product(zone_centroids.index, zone_centroids.index)
        ):
            if o == d:
                continue

            params.append(
                CalculationParameters(
                    server_url=settings.server_url,
                    modes=[str(m) for m in settings.modes],
                    datetime=settings.datetime,
                    origin=row_to_place(zone_centroids.loc[o]),
                    destination=row_to_place(zone_centroids.loc[d]),
                    arrive_by=settings.arrive_by,
                    searchWindow=settings.search_window_seconds,
                    wheelchair=settings.wheelchair,
                    max_walk_distance=settings.max_walk_distance,
                )
            )

        return params

    LOG.info("Assessing if trips are too far based on Rural Urban Classifications.")

    # Load & format trips requested lookup
    try:
        trips_requested = pd.read_csv(
            os.path.join(output_dir_path, "trips_previously_requested.csv")
        )

        # set OD_code as index
        trips_requested.set_index("od_code", inplace=True, drop=True)
    except FileNotFoundError:
        print(
            "\nA file `{}/trips_previously_requested.csv` could not be located.".format(
                output_dir_name
            ),
            "\nThus, assessing all found trips",
        )
        trips_requested_file = False

    already_requested = 0
    too_far_destinations = 0
    if filter_radius != 0:
        # Filter radius has been applied - use compiled GDF from above
        compiled_centroids_path = os.path.join(
            os.getcwd(), "Data", "compiled_zone_centroids_with_filter.csv"
        )
        zone_centroids_BnG = pd.read_csv(compiled_centroids_path)
        print("\nTrips to assess:", len(zone_centroids_BnG))

        zone_distances = zone_centroids_BnG.copy()

        # Create DF of journey distances with OD_pairs as index to apply .loc[] with OD_pairs
        zone_distances.set_index("OD_pairs", inplace=True)

        for o, d in tqdm.tqdm(
            zip(zone_centroids_BnG["Origins"], zone_centroids_BnG["Destinations"])
        ):
            # Check if OD journey exceeds maximum distance with RUC weightings applied
            if filter_radius != 0:
                od_code = "_".join((str(o), str(d)))

                if trips_requested_file:
                    if od_code in trips_requested.index:
                        # Trip has been requested in previous run. Skip.
                        already_requested += 1
                        continue

                od_distance = zone_distances.loc[od_code]["distances"]
                # Check area type of origin zone - apply extra radius weighting if origin is rural
                radius_weight = LSOA_RUC_weights[LSOA_RUC_types.loc[o]["RUC11CD"]]

                if od_distance > (filter_radius * radius_weight):
                    too_far_destinations += 1
                    continue

            params.append(
                CalculationParameters(
                    server_url=settings.server_url,
                    modes=[str(m) for m in settings.modes],
                    datetime=settings.datetime,
                    origin=row_to_place(zone_centroids.loc[o]),
                    destination=row_to_place(zone_centroids.loc[d]),
                    arrive_by=settings.arrive_by,
                    searchWindow=settings.search_window_seconds,
                    wheelchair=settings.wheelchair,
                    max_walk_distance=settings.max_walk_distance,
                )
            )

        LOG.info(
            "Additional: "
            + str(too_far_destinations)
            + " journeys removed for being too far based on RUCs."
        )
        LOG.info(
            "With a further: "
            + str(already_requested)
            + " journeys removed for having already been requested."
        )
        LOG.info("Requests will now be sent for " + str(len(params)) + " journeys.")
        return params


def calculate_costs(
    parameters: CalculationParameters,
    response_file: io.TextIOWrapper,
    lock: threading.Lock,
    generalised_cost_parameters: GeneralisedCostFactors,
) -> dict[str, Any]:
    """Calculate cost between 2 zones using OTP.

    Parameters
    ----------
    parameters : CalculationParameters
        Parameters for OTP.
    response_file : io.TextIOWrapper
        File to save the JSON response too.
    lock : threading.Lock
        Lock object to avoid race conditions when writing
        to `response_file`.
    generalised_cost_parameters : GeneralisedCostParameters
        Factors and other parameters for calculating the
        generalised cost.

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
    with lock:
        response_file.write(cost_res.json() + "\n")

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
            val = getattr(it, s)
            # Set value to NaN if it doesn"t exist or isn"t set
            if val is None:
                val = np.nan
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


def build_cost_matrix(
    zone_centroids: centroids.ZoneCentroids,
    settings: CostSettings,
    matrix_file: pathlib.Path,
    generalised_cost_parameters: GeneralisedCostFactors,
    aggregation_method: AggregationMethod,
    workers: int = 0,
    crowfly_max_distance: Optional[float] = None,
) -> None:
    """Create cost matrix for all zone to zone pairs.

    Parameters
    ----------
    zone_centroids : ZoneCentroids
        Zones for the cost matrix.
    settings : CostSettings
        Settings for calculating the costs.
    matrix_file : pathlib.Path
        Path to save the cost matrix to.
    generalised_cost_parameters : GeneralisedCostParameters
        Factors and other parameters for calculating the
        generalised cost.
    aggregation_method : AggregationMethod
        Aggregation method used for generalised cost matrix.
    workers : int, default 0
        Number of threads to create during calculations.
    crowfly_max_distance: Optional[float]
        Maximum permissible crowflies trip distance (NOT on road network)
        for each OD trip
    """
    LOG.info("Calculating costs for %s with settings\n%s", matrix_file.name, settings)
    jobs = build_calculation_parameters(zone_centroids, settings, crowfly_max_distance)

    lock = threading.Lock()
    response_file = matrix_file.with_name(matrix_file.name + "-response_data.jsonl")
    with open(response_file, "wt") as responses:
        iterator = util.multithread_function(
            workers,
            calculate_costs,
            jobs,
            dict(
                response_file=responses,
                lock=lock,
                generalised_cost_parameters=generalised_cost_parameters.copy(),
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
        yield CostResults.parse_raw(line)


def cost_matrix_from_responses(
    responses_file: pathlib.Path,
    matrix_file: pathlib.Path,
    aggregation_method: AggregationMethod,
) -> None:
    """Create cost matrix CSV from responses JSON lines file.

    Parameters
    ----------
    responses_file : pathlib.Path
        Path to JSON lines file containing `CostResults`.
    matrix_file : pathlib.Path
        Path to CSV file to output cost metrics to.
    aggregation_method : AggregationMethod
        Aggregation method used for generalised cost matrix.
    """
    matrix_data = []
    with open(responses_file, "rt") as responses:
        for line in tqdm.tqdm(
            responses, desc="Calculating cost matrix", dynamic_ncols=True
        ):
            results = CostResults.parse_raw(line)
            # TODO(MB) Recalculate generalised cost if new parameters are provided
            matrix_data.append(_matrix_costs(results))

    _write_matrix_files(matrix_data, matrix_file, aggregation_method)
