import logging
import os
import shutil
import subprocess
from typing import Optional

from otp4gb.centroids import Bounds
from otp4gb.config import BIN_DIR, ROOT_DIR

LOG = logging.getLogger(__name__)
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


def osm_convert(input_, output, extents: Optional[Bounds] = None):
    if extents is None:
        LOG.info("Copying OSM file '%s' to '%s' without filtering", input_, output)
        shutil.copy(input_, output)
        return

    LOG.info("Running osmconvert to filter '%s'\n%s", input_, extents)

    bounds = f"{extents.min_lon},{extents.min_lat},{extents.max_lon},{extents.max_lat}"
    expr = _command(input_, bounds, output)
    LOG.debug("commandline = %s", " ".join(str(i) for i in expr))

    subprocess.run(expr, check=True)
    LOG.info("Created filterd OSM file '%s'", output)
