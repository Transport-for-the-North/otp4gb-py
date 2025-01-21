import logging
import os
import shutil
import subprocess
from typing import Optional

from otp4gb.centroids import Bounds
from otp4gb.config import ASSET_DIR, BIN_DIR, PREPARE_MAX_HEAP

logger = logging.getLogger(__name__)


def filter_gtfs_files(
    gtfs_files: list[str],
    output_dir: os.PathLike,
    date: str,
    extents: Optional[Bounds] = None,
):
    location = None
    if extents is not None:
        location = (
            f"{extents.min_lat}:{extents.min_lon}:{extents.max_lat}:{extents.max_lon}"
        )

    logger.debug(location)

    timetable_files = [os.path.join(ASSET_DIR, f) for f in gtfs_files]

    for timetable_file in timetable_files:
        gtfs_filter(
            timetable_file,
            output_dir=output_dir,
            date_filter=date,
            location_filter=location,
        )


def gtfs_filter(
    timetable_file: os.PathLike,
    output_dir: os.PathLike,
    date_filter: str,
    location_filter: Optional[str] = None,
):
    logger.debug(timetable_file)
    temp_folder = "zip_tmp"
    jar_file = os.path.join(BIN_DIR, "gtfs-filter-0.1.jar")

    name_base = os.path.splitext(os.path.basename(timetable_file))[0]
    logger.info("Processing GTFS file %s", name_base)
    output_file = os.path.join(output_dir, name_base + "_filtered")

    command = [
        "java",
        "-Xmx{}".format(PREPARE_MAX_HEAP),
        "-jar",
        jar_file,
        timetable_file,
        "-d",
        date_filter,
        "-o",
        temp_folder,
    ]

    if location_filter is not None:
        command.extend(["-l", location_filter])
        logger.info("Filtering GTFS with location")
    else:
        logger.info("No location used for GTFS filtering")

    # TODO(MB) Add check to see if only the agency file remains (all dates
    # and routes have been filtered out), if this is the case don't create
    # the zip file for this area

    logger.debug(command)
    subprocess.run(command, shell=True)
    shutil.make_archive(output_file, "zip", temp_folder)
    shutil.rmtree(temp_folder)
