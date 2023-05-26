import atexit
import datetime
import logging
import os
import pathlib
import sys

from otp4gb.centroids import load_centroids, ZoneCentroidColumns
from otp4gb.config import ASSET_DIR, load_config
from otp4gb.logging import file_handler_factory, get_logger
from otp4gb.otp import Server
from otp4gb.util import Timer
from otp4gb import cost


logger = get_logger()
logger.setLevel(logging.INFO)

FILENAME_PATTERN = (
    "Buffered{buffer_size}m_IsochroneBy_{mode}_ToWorkplaceZone_"
    "{location_name}_ToArriveBy_{arrival_time:%Y%m%d_%H%M}_"
    "within_{journey_time:_>4n}_mins.geojson"
)


def main():
    _process_timer = Timer()

    @atexit.register
    def report_time():
        logger.info("Finished in %s", _process_timer)

    try:
        opt_base_folder = os.path.abspath(sys.argv[1])
    except IndexError as error:
        logger.error("No path provided")
        raise ValueError("Base folder not provided") from error

    log_file = file_handler_factory(
        "process.log", os.path.join(opt_base_folder, "logs")
    )
    logger.addHandler(log_file)

    config = load_config(opt_base_folder)

    # Start OTP Server
    server = Server(opt_base_folder)
    if not config.no_server:
        logger.info("Starting server")
        server.start()

    # Load Northern Boundaries
    logger.info("Loading centroids")
    centroids = load_centroids(
        pathlib.Path(ASSET_DIR) / config.centroids,
        pathlib.Path(ASSET_DIR) / config.destination_centroids,
        # TODO(MB) Read parameters for config to define column names
        zone_columns=ZoneCentroidColumns(),
        extents=config.extents,
    )

    logger.info(
        "Considering %d centroids",
        len(centroids.origins),
    )

    for time_period in config.time_periods:
        search_window_seconds = None
        if time_period.search_window_minutes is not None:
            search_window_seconds = time_period.search_window_minutes * 60

        travel_datetime = datetime.datetime.combine(
            config.date, time_period.travel_time
        )
        # Assume time is in local timezone
        travel_datetime = travel_datetime.astimezone()
        logger.info(
            "Given date / time is assumed to be in local timezone: %s",
            travel_datetime.tzinfo,
        )

        for modes in config.modes:
            print()  # Empty line space in cmd window
            logger.info(
                "Calculating costs for %s - %s", time_period.name, ", ".join(modes)
            )
            cost_settings = cost.CostSettings(
                server_url="http://localhost:8080",
                modes=modes,
                datetime=travel_datetime,
                arrive_by=True,
                search_window_seconds=search_window_seconds,
                max_walk_distance=config.max_walk_distance,
                crowfly_max_distance=config.crowfly_max_distance,
            )

            matrix_path = pathlib.Path(
                opt_base_folder
            ) / "costs/{tp_name}/{modes}_costs_{dt:%Y%m%dT%H%M}.csv".format(
                tp_name=time_period.name, modes="_".join(modes), dt=travel_datetime
            )
            matrix_path.parent.mkdir(exist_ok=True, parents=True)

            cost.build_cost_matrix(
                centroids,
                cost_settings,
                matrix_path,
                config.generalised_cost_factors,
                config.iterinary_aggregation_method,
                config.number_of_threads,
                config.crowfly_max_distance,
            )

    # Stop OTP Server
    server.stop()


if __name__ == "__main__":
    main()
