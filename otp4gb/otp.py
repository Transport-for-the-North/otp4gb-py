from __future__ import annotations

import atexit
import datetime as dt
import logging
import os
import pathlib
import shutil
import subprocess
import time
import urllib.parse
import urllib.request
import warnings

import psutil

from otp4gb import centroids, config, cost, gtfs_filter, osmconvert, parameters, util

LOG = logging.getLogger(__name__)
OTP_VERSION = "2.1.0"
GRAPH_FILE_NAME = "graph.obj"
GRAPH_FILE_SUBPATH = f"graphs/filtered/{GRAPH_FILE_NAME}"
MAX_RETRIES = 10


def _java_command(heap):
    otp_jar_file = os.path.join(config.BIN_DIR, f"otp-{OTP_VERSION}-shaded.jar")
    return [
        "java",
        "-Xmx{}".format(heap),
        "--add-opens",
        "java.base/java.util=ALL-UNNAMED",
        "--add-opens",
        "java.base/java.io=ALL-UNNAMED",
        "-jar",
        otp_jar_file,
    ]


def prepare_graph(build_dir: pathlib.Path) -> None:
    """Run OTP build command to create graph.obj file."""
    build_dir = pathlib.Path(build_dir)
    build_dir.mkdir(exist_ok=True)

    log_path = build_dir / f"otp_prepare-{dt.datetime.today():%Y%m%d}.log"
    LOG.info(
        "Running OTP build command, this may take some time especially"
        " for larger areas (national)\nLog messages saved to '%s'",
        log_path,
    )

    command = _java_command(config.PREPARE_MAX_HEAP) + ["--build", build_dir, "--save"]
    LOG.debug("OTP build command: %s", " ".join(str(i) for i in command))

    timer = util.Timer()
    with open(log_path, "at", encoding="utf-8") as file:
        subprocess.run(command, check=True, stdout=file, stderr=subprocess.STDOUT)

    graph_file = build_dir / GRAPH_FILE_NAME
    if graph_file.is_file():
        LOG.info("Finished creating OTP graph file in %s: '%s'", timer, graph_file)
    else:
        raise FileNotFoundError(f"error creating OTP graph: '{graph_file}'")


class Server:

    def __init__(self, base_dir, port=8080):
        self.base_dir = base_dir
        self.port = int(port)
        self.process = None

        self.log_path = (
            pathlib.Path(base_dir) / f"logs/otp_server-{dt.datetime.today():%Y%m%d}.log"
        )
        self.log_path.parent.mkdir(exist_ok=True)
        self.log_fo = open(self.log_path, "at", encoding="utf-8")

    def start(self):
        LOG.info("Checking for existing OTP server started")
        server_up = self._check_server(max_retries=0, raise_error=False)

        if server_up:
            warnings.warn("Server already on so attempting to force shutdown")
            self.end_java_subprocess()

            if self._check_server(max_retries=0, raise_error=False):
                raise ValueError("OTP server failed to shut-down")

        command = _java_command(config.SERVER_MAX_HEAP) + [
            r"graphs\filtered",
            "--load",
            "--port",
            str(self.port),
        ]
        LOG.info("Starting OTP server, log messages saved to '%s'", self.log_path)
        LOG.debug("Running server with %s", " ".join(str(i) for i in command))
        self.process = subprocess.Popen(
            command, cwd=self.base_dir, stdout=self.log_fo, stderr=subprocess.STDOUT
        )
        atexit.register(self.stop)
        self._check_server()
        LOG.info("OTP server started")

    def end_java_subprocess(self):
        check_name = "java.exe"
        warnings.warn(
            f"Killing all processes named {check_name} running on port {self.port}"
        )

        connections_pids = {
            i for i in psutil.net_connections() if i.laddr.port == self.port
        }

        for proc in psutil.process_iter(["pid", "name", "username"]):
            name = proc.info.get("name")
            pid = proc.info.get("pid")

            if name != check_name:
                continue

            if pid in connections_pids:
                LOG.debug("Killing %s (%s)", name, pid)
                proc.kill()
                LOG.debug("Killed")

    def _check_server(
        self, max_retries: int = MAX_RETRIES, raise_error: bool = True
    ) -> bool:
        LOG.info("Checking server")
        wait_time = 30

        server_up = False
        retries = 0
        while not server_up:
            try:
                self.send_request()
                LOG.info("Server responded")
                server_up = True

            except urllib.error.URLError as error:
                if retries > max_retries:
                    msg = (
                        f"Maximum retries ({max_retries}) exceeded for server check "
                        f"and server not available. Server error: {error}"
                    )
                    if raise_error:
                        raise urllib.error.URLError(msg) from error

                    warnings.warn(msg)
                    return False

                retries += 1
                LOG.info(
                    "Server not available. Retry %s. Server error: %s", retries, error
                )
                time.sleep(wait_time)

        LOG.info("Server up %s", server_up)
        return server_up

    def send_request(self, path="", query=None):
        url = self.get_url(path, query)
        LOG.debug("About to make request to %s", url)
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
            },
        )
        with urllib.request.urlopen(request) as r:
            body = r.read().decode(r.info().get_param("charset") or "utf-8")
        return body

    def get_url(self, path="", query=None):
        qs = urllib.parse.urlencode(query, safe=",:") if query else ""
        url = urllib.parse.urlunsplit(
            [
                "http",
                "localhost:" + str(self.port),
                urllib.parse.urljoin("otp/routers/filtered/", path),
                qs,
                None,
            ]
        )
        return url

    def stop(self):
        server_up = self._check_server(raise_error=False)

        if not server_up:
            LOG.info("OTP server is not running")
            return

        LOG.info("Stopping OTP server")
        self.process.terminate()
        self.process.wait(timeout=60)
        self.log_fo.close()

        LOG.info(
            "server_up = %s following initial termination",
            self._check_server(raise_error=False),
        )
        while self._check_server(raise_error=False, max_retries=1):
            self.end_java_subprocess()

        LOG.info("OTP server stopped")


def run_server(*, folder: pathlib.Path, **_) -> None:
    """Run OTP server at given `folder`."""
    LOG.info("Running OTP4GB server")

    if not folder.is_dir():
        raise NotADirectoryError(folder)

    server = Server(folder)
    server.start()

    input("\n\nPress any key to stop server...\n\n")

    server.stop()


def _prepare(folder: pathlib.Path, params: config.ProcessConfig, force: bool) -> None:
    LOG.info("Running OTP4GB prepare")

    graph_file = folder / GRAPH_FILE_SUBPATH

    if graph_file.parent.is_dir() and force:
        LOG.info(
            "Deleting '%s' folder and recreating because force flag was given",
            graph_file.parent,
        )
        shutil.rmtree(graph_file.parent)

    if graph_file.is_file():
        LOG.info(
            "Graph file already exists and will be used (%s)."
            " To rebuild graph file use the 'force' flag",
            graph_file,
        )
        return

    if graph_file.parent.is_dir():
        raise FileNotFoundError(
            "graph.obj file doesn't exist but folder does, unsure why"
            " this is the case. Please fix this or use the 'force'"
            " flag to rebuild everything."
        )

    graph_file.parent.mkdir(parents=True)

    date_filter_string = "{}:{}".format(params.date, params.date + dt.timedelta(days=1))
    LOG.debug("date_filter_string is %s", date_filter_string)
    gtfs_filter.filter_gtfs_files(
        params.gtfs_files,
        output_dir=graph_file.parent,
        date=date_filter_string,
        extents=params.extents,
    )

    # Crop the osm.pbf map of GB to the bounding box
    osmconvert.osm_convert(
        config.ASSET_DIR / params.osm_file,
        graph_file.parent / "gbfiltered.pbf",
        extents=params.extents,
    )

    config.write_build_config(graph_file.parent, params.date)
    shutil.copy(config.CONF_DIR / "router-config.json", graph_file.parent)

    prepare_graph(graph_file.parent)


def _process(folder: pathlib.Path, save_parameters: bool, params: config.ProcessConfig):
    server = Server(folder)
    if save_parameters:
        LOG.info("Saving OTP request parameters without starting OTP")
    elif not params.no_server:
        LOG.info("Starting server")
        server.start()

    LOG.info("Loading centroids")
    # Check if config.destination_centroids has been supplied
    if params.destination_centroids is None:
        destination_centroids_path = None
        LOG.info(
            "No destination centroids detected. Proceeding with %s",
            params.centroids,
        )
    else:
        destination_centroids_path = (
            pathlib.Path(config.ASSET_DIR) / params.destination_centroids
        )

    centroids_data = centroids.load_centroids(
        pathlib.Path(config.ASSET_DIR) / params.centroids,
        destination_centroids_path,
        # TODO(MB) Read parameters for config to define column names
        zone_columns=centroids.ZoneCentroidColumns(),
        extents=params.extents,
    )
    LOG.info("Considering %d centroids", len(centroids_data.origins))

    for time_period in params.time_periods:
        search_window_seconds = None
        if time_period.search_window_minutes is not None:
            search_window_seconds = time_period.search_window_minutes * 60

        travel_datetime = dt.datetime.combine(params.date, time_period.travel_time)
        # Assume time is in local timezone
        travel_datetime = travel_datetime.astimezone()
        LOG.info(
            "Given date / time is assumed to be in local timezone: %s",
            travel_datetime.tzinfo,
        )

        for modes in params.modes:
            print()  # Empty line space in cmd window
            cost_settings = parameters.CostSettings(
                server_url="http://localhost:8080",
                modes=modes,
                datetime=travel_datetime,
                arrive_by=True,
                search_window_seconds=search_window_seconds,
                max_walk_distance=params.max_walk_distance,
                crowfly_max_distance=params.crowfly_max_distance,
            )

            if save_parameters:
                LOG.info(
                    "Building parameters for %s - %s",
                    time_period.name,
                    ", ".join(modes),
                )

                parameters_path = folder / (
                    f"parameters/{time_period.name}_{'_'.join(modes)}"
                    f"_parameters_{travel_datetime:%Y%m%dT%H%M}.csv"
                )
                parameters_path.parent.mkdir(exist_ok=True)

                parameters.save_calculation_parameters(
                    zones=centroids_data,
                    settings=cost_settings,
                    output_file=parameters_path,
                    ruc_lookup=params.ruc_lookup,
                    irrelevant_destinations=params.irrelevant_destinations,
                )
                continue

            LOG.info(
                "Calculating costs for %s - %s", time_period.name, ", ".join(modes)
            )
            matrix_path = folder / (
                f"costs/{time_period.name}/"
                f"{'_'.join(modes)}_costs_{travel_datetime:%Y%m%dT%H%M}.csv"
            )
            matrix_path.parent.mkdir(exist_ok=True, parents=True)

            # TODO: Add requested trips here
            jobs = parameters.build_calculation_parameters(
                zones=centroids_data,
                settings=cost_settings,
                ruc_lookup=params.ruc_lookup,
                irrelevant_destinations=params.irrelevant_destinations,
            )

            cost.build_cost_matrix(
                jobs=jobs,
                matrix_file=matrix_path,
                generalised_cost_parameters=params.generalised_cost_factors,
                aggregation_method=params.iterinary_aggregation_method,
                workers=params.number_of_threads,
                write_raw_responses=params.write_raw_responses,
            )

    # Stop OTP Server
    server.stop()


def run_process(
    *,
    folder: pathlib.Path,
    save_parameters: bool,
    prepare: bool,
    force: bool,
    bounds: str | None = None,
    date: dt.date | None = None,
    **_,
) -> None:
    """Run OTP prepare and processing for given `folder`."""
    LOG.info("Running OTP4GB processing")
    if not folder.is_dir():
        raise NotADirectoryError(folder)

    params = config.load_config(folder)

    if date is not None:
        LOG.warning(
            "date argument provided (%s), which is used instead of config value (%s)",
            f"{date:%Y-%m-%d}",
            f"{params.date:%Y-%m-%d}",
        )
        params.date = date

    if bounds is not None:
        custom_bounds = config.load_bounds()

        if bounds not in custom_bounds:
            raise KeyError(
                f"unknown bounds argument given '{bounds}', "
                f"should be one of {', '.join(custom_bounds.keys())}"
            )

        LOG.warning("bounds argument provided, which is used instead of config value")
        params.extents = custom_bounds[bounds]

    if prepare or force or not (folder / GRAPH_FILE_SUBPATH).is_file():
        _prepare(folder, params, force)

    if prepare:
        LOG.info(
            "Prepare complete, not running routing "
            "processing because prepare flag was given"
        )
        return

    _process(folder, save_parameters, params)
