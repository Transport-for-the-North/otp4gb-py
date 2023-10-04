import subprocess
import re
from datetime import date, timedelta
import getopt
import logging
import os
import shutil
import sys
from yaml import safe_load

from otp4gb.gtfs_filter import filter_gtfs_files
from otp4gb.osmconvert import osm_convert
from otp4gb.config import ASSET_DIR, CONF_DIR, load_config, write_build_config
from otp4gb.centroids import Bounds
from otp4gb.otp import prepare_graph, OTP_VERSION

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


def load_bounds() -> dict[str, Bounds]:
    with open(os.path.join("otp4gb", "bounds.yml")) as bounds_file:
        bounds = safe_load(bounds_file)

    for nm, values in bounds.items():
        bounds[nm] = Bounds.from_dict(values)

    return bounds


def usage(exit_code=1):
    usage_string = """
prepare.py [-F|--force] -b bounds -d date <path to config root>

  -b, --bounds\tBounds (as defined in bounds.py) for the filter
  -d, --date\tDate for routing graph preparation
  -F, --force\tForce recreation
    """
    print(usage_string)
    exit(exit_code)


def main():
    java_output = str(
        subprocess.check_output(["java", "-version"], stderr=subprocess.STDOUT)
    )

    # RegEx pattern check looks for: digit.digit patterns (version numbers)
    version_pattern = '"(\d+\.\d+).*"'
    java_version = re.search(version_pattern, java_output).groups()[0]

    # Check recommended java is being used for OTP version
    if OTP_VERSION[0] == "2":
        # Check java version is 17 (recommended for OTP2.X)
        if java_version[0:2] != "17":
            logger.warning(
                "Java 17 recommended in OTP2.x for optimal performance."
                " Java %s detected", java_version
            )
    elif OTP_VERSION[0] == "1":
        if java_version[0] != "8":
            logger.warning(
                "Java 8 is recommended for OTP1.x. Java %s detected", java_version
            )

    try:
        opts, args = getopt.getopt(sys.argv[1:], "b:d:F", ["bounds=", "date=", "force"])
    except getopt.GetoptError as err:
        logging.error(err)
        sys.exit(2)

    try:
        opt_base_folder = os.path.abspath(args[0])
    except IndexError:
        logger.error("No path provided")
        usage()

    logger.debug("Base folder is %s", opt_base_folder)

    config = load_config(opt_base_folder)
    logger.debug("config = %s", config)

    bounds = load_bounds()
    logger.debug("bounds = %s", bounds)

    if not os.path.exists(opt_base_folder):
        logger.error("Base path %s does not exist", opt_base_folder)
        exit(1)

    opt_force = False
    opt_date = config.date
    extents = config.extents
    # TODO(MB) Update this to use argparser module
    for o, a in opts:
        if o == "-b" or o == "--bounds":
            try:
                extents = bounds[a]
            except:
                logger.error("Invalid bounds %s", a)
                logger.error(
                    "Available bounds ->\n%s", "\n".join([a for a in bounds.keys()])
                )
            continue
        if o == "-F" or o == "--force":
            opt_force = True
            continue
        if o == "-d" or o == "--date":
            try:
                opt_date = date.fromisoformat(a)
            except:
                logger.error("Invalid date %s", a)
                opt_date = None
            continue
        assert False, "Unhandled option"

    filtered_graph_folder = os.path.join(opt_base_folder, "graphs", "filtered")

    if not opt_date:
        logger.error("No date provided")
        usage()
    logger.debug("opt_date is %s", opt_date)

    date_filter_string = "{}:{}".format(opt_date, opt_date + timedelta(days=1))
    logger.debug("date_filter_string is %s", date_filter_string)

    if not extents:
        logger.error("No extents provided")
        usage()
    logger.debug("Extents set to %s", extents)

    if opt_force:
        shutil.rmtree(filtered_graph_folder, ignore_errors=True)

    if os.path.exists(filtered_graph_folder):
        logging.warning(
            "A folder of filtered GTFS and OSM files already exists. "
            "To filter again, delete this folder, or re-run prepare with "
            "force overwrite (-f) enabled."
        )
    else:
        os.makedirs(filtered_graph_folder)

        # We need to crop the osm.pbf file and all of the GTFS public transport files for GB
        # And then put them all in one folder, which we then use to run an open trip planner instance.
        # see https://github.com/odileeds/ATOCCIF2GTFS for timetable files
        # These need to reside in base_folder/input/gtfs
        filter_gtfs_files(
            config.gtfs_files,
            output_dir=filtered_graph_folder,
            date=date_filter_string,
            extents=extents,
        )

        # Crop the osm.pbf map of GB to the bounding box
        # If you are not using Windows a version of osmconvert on your platform may be available via https://wiki.openstreetmap.org/wiki/Osmconvert
        osm_convert(
            os.path.join(ASSET_DIR, config.osm_file),
            os.path.join(filtered_graph_folder, "gbfiltered.pbf"),
            extents=extents,
        )

        write_build_config(filtered_graph_folder, opt_date)
        shutil.copy(os.path.join(CONF_DIR, "router-config.json"), filtered_graph_folder)
        shutil.copy(os.path.join(CONF_DIR, "otp-config.json"), filtered_graph_folder)

    if os.path.exists(os.path.join(filtered_graph_folder, "graph.obj")):
        logging.warning(
            "A graph.obj file already exists and will be used. "
            "To rebuild the transport graph delete the graph.obj file,"
            "or re-run prepare with force overwrite (-f) enabled."
        )
    else:
        prepare_graph(filtered_graph_folder)


if __name__ == "__main__":
    main()
