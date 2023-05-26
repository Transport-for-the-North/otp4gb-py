import logging
import os
import subprocess

from otp4gb.config import BIN_DIR, ROOT_DIR
from otp4gb.centroids import Bounds

logger = logging.getLogger(__name__)
DOCKER = "OSMCONVERT_DOCKER" in os.environ


def _command(input_, bounds, output):
    command = [os.path.join(BIN_DIR, "osmconvert64.exe")]
    if DOCKER:
        output = output.replace(ROOT_DIR, "/mnt")
        input_ = input_.replace(ROOT_DIR, "/mnt")
        command = [
            "docker",
            "run",
            "--rm",
            "-v",
            "{}:/mnt".format(ROOT_DIR),
            "phdax/osmtools",
            "osmconvert",
        ]
    args = [
        input_,
        "-b={}".format(bounds),
        "--complete-ways",
        "-o={}".format(output),
    ]
    return command + args


def osm_convert(input_, output, extents: Bounds):
    bounds = f"{extents.min_lon},{extents.min_lat},{extents.max_lon},{extents.max_lat}"
    logger.debug(bounds)

    expr = _command(input_, bounds, output)
    logger.info("Running osmconvert")
    logger.debug("commandline = %s", " ".join(expr))
    subprocess.run(expr, check=True)
