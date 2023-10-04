# Standard imports
import pathlib
import sys
import os
import threading
import logging
import requests

# Local imports
from otp4gb import isochrone, config, util
from otp4gb.otp import Server
from process import ProcessArgs
from otp4gb.centroids import load_centroids, ZoneCentroidColumns

# 3rd party imports
import tqdm
import datetime as dt

##### CONSTANTS #####

#  TODO(JH): Update this to accept OUTPUT_DIR as a command line argument

if sys.argv[1] == "--mode=client":
    print("Assigning output dir")
    sys.argv[1] = "isochrone_response_test"

# sys.argv[1] should be the output directory
OUTPUT_DIR = pathlib.Path(os.path.join(os.getcwd(), sys.argv[1]))
RESPONSE_PATH = OUTPUT_DIR / "Responses"
RESPONSE_FILE = "response-data.jsonl"
TEXT_ENCODING = "utf-8"
DATETIME_STR_FMT = "%Y-%m-%d %H%M"

# logging formatting
LOGGER_FMT = "[%(levelname)s] %(asctime)s - %(message)s"
LOGGER_LEVEL = logging.INFO


#### Functionality ####
def main():

    # Initialise logging
    logging.basicConfig(level=LOGGER_LEVEL, format=LOGGER_FMT)

    # Process arguments passed from command line
    arguments = ProcessArgs.parse()

    logging.info("Reading %s isochrone config params" % arguments.folder)
    iso_config = config.load_config(arguments.folder)

    logging.info("Loading centroids with filename %s" % iso_config.iso_centroids)
    centroids = load_centroids(
        origins_path=config.ASSET_DIR / iso_config.iso_centroids,
        zone_columns=ZoneCentroidColumns(),
        latitude_column=iso_config.iso_lat_col,
        longitude_column=iso_config.iso_long_col,
    )

    ## Should OTP server be started?
    server = Server(arguments.folder)
    if arguments.save_parameters:
        logging.info("Saving OTP request parameters without starting OTP server")
    elif not iso_config.no_server:
        logging.info("Starting OTP server")
        try:
            requests.get("http://"
                         + iso_config.hostname
                         + ":"
                         + str(iso_config.port)
                         )
            logging.info("Server is already up")
        except requests.exceptions.ConnectionError as e:
            logging.info("Server not found. Starting new instance")
            logging.debug("Connection error: %s" % e)
            server.start()

    ## - Build params to request
    zone_locs = centroids.origins.geometry
    zone_locs = zone_locs[0:1000]  # Create a small subset for testing purposes
    for modes in iso_config.iso_modes:

        # Parse current datetime & routing modes
        cur_datetime = iso_config.iso_datetime.strftime(DATETIME_STR_FMT)
        cur_modes = ", ".join([mode.__str__() for mode in modes])

        response_file_path = RESPONSE_PATH / cur_datetime / cur_modes
        response_file_path.mkdir(parents=True, exist_ok=True)
        response_file_path = RESPONSE_PATH / cur_datetime / cur_modes / RESPONSE_FILE

        # Check if a response file already exists
        if os.path.isfile(response_file_path):
            ans = input(
                "A response file for this config already exists.\nOverwrite?   [y/n]  "
            )
            if ans != "y":
                print("`y` not detected. Stopping process.")
                exit()

        logging.info("Building Isochrone request parameters")
        iso_params = []
        for loc in tqdm.tqdm(
            zone_locs, desc="Building parameters", total=len(zone_locs)
        ):
            iso_params.append(
                isochrone.IsochroneParameters(
                    location=loc,
                    departure_time=iso_config.iso_datetime,
                    cutoff=iso_config.cutoffs,
                    modes=modes,
                )
            )

        if arguments.save_parameters:
            logging.info(
                "Saving isochrone parameters to %s" % response_file_path.parents
            )

            with open(
                response_file_path.with_name("Isochrone_parameters.jsonl"),
                "wt",
                encoding=TEXT_ENCODING,
            ) as param_file:
                for param in iso_params:
                    # TODO(JH) There must be a better way of doing this. Consider
                    # TODO      how it would work loading this back in?
                    # TypeError: Object type 'Point' is not JSON serializable.
                    # Thus, use shapely serialization method, __geo_interface__
                    param.location = param.location.__geo_interface__
                    param_file.write(param.json() + "\n")
            continue

        else:
            print(dt.datetime.now().strftime(DATETIME_STR_FMT))
            logging.info("Requesting %s isochrones from %s" % (cur_modes, cur_datetime))

            lock = threading.Lock()
            with open(
                response_file_path, "wt", encoding=TEXT_ENCODING
            ) as response_file:
                iterator = util.multithread_function(
                    workers=iso_config.number_of_threads,
                    func=isochrone.get_isochrone,
                    args=iso_params,
                    shared_kwargs=dict(
                        server_url="http://"
                        + iso_config.hostname
                        + ":"
                        + str(iso_config.port),
                    ),
                )

                for url, response in tqdm.tqdm(
                    iterator,
                    desc="Requesting Isochrones",
                    total=len(iso_params),
                    dynamic_ncols=True,
                    smoothing=0,
                ):
                    with lock:
                        response_file.write(", ".join((url, response)) + "\n")

            logging.info(
                "Finished requesting isochrone for %s. Shutting down OTP server"
                % cur_modes
            )
            server.stop()

        logging.info("Process finished")
        print(dt.datetime.now().strftime(DATETIME_STR_FMT))


if __name__ == "__main__":
    main()

## 3 - Request data


# # -*- coding: utf-8 -*-
# """Functionality for requesting isochrones from OTP."""
#
# #### IMPORTS ####
# import dataclasses
#
# # Standard imports
# import datetime as dt
# import pytz
# import logging
# from typing import Any
# from urllib import parse
# import sys
#
# # Third party imports
# import pydantic
# import requests
# from shapely import geometry
#
# # Local imports
# from otp4gb import routing
# from otp4gb.config import CONF_DIR, load_isochrone_config
#
# # CONSTANTS ####
# LOG = logging.getLogger(__name__)
# ISOCHRONE_API_ROUTE = "otp/traveltime/isochrone"
# # WALK is not supported standalone arg - mix with another irrelevant mode
# WALK_PAIR_MODE = "FERRY"
# OUTPUT_DIR = sys.argv[1]
# print("TEST OUTPUTDIR:", OUTPUT_DIR)
#
#
# #### CLASSES ####
# @dataclasses.dataclass
# class IsochroneParameters:
#     location: geometry.Point
#     departure_time: dt.datetime
#     cutoff: list[dt.timedelta]
#     modes: list[routing.Mode]
#
#     def parameters(self) -> dict[str, Any]:
#
#         modes = [i.value for i in self.modes]
#
#         for i in range(len(modes)):
#             print(modes[i], type(modes[i]))
#             if modes[i] == "WALK":
#                 modes[i] = ",".join(("WALK", WALK_PAIR_MODE))
#
#         # OTP4GB - Assumed GB Timezone
#         time_zone = pytz.timezone("Europe/London")
#         aware_time = self.departure_time.astimezone(time_zone)
#         print(modes)
#         return dict(
#             location=[str(i) for i in self.location.coords],
#             time=aware_time,  # TODO Should include time zone +00:00
#             cutoff=[_format_cutoff(i) for i in self.cutoff],
#             modes=modes,  # TODO Cannot just be WALK should be WALK and TRANSIT
#         )
#
#
# class IsochroneResult(pydantic.BaseModel):
#     ...
#
#
# #### FUNCTIONS ####
# def _format_cutoff(cutoff: dt.timedelta) -> str:
#     seconds = round(cutoff.total_seconds())
#     minutes = 0
#     hours = 0
#
#     if seconds > 60:
#         minutes, seconds = divmod(seconds, 60)
#
#     if minutes > 60:
#         hours, minutes = divmod(minutes, 60)
#
#     text = ""
#     for name, value in (("H", hours), ("M", minutes), ("S", seconds)):
#         if value > 0:
#             text += f"{value}{name}"
#
#     return text
#
#
# def get_isochrone(server_url: str, parameters: IsochroneParameters) -> IsochroneResult:
#     url = parse.urljoin(server_url, ISOCHRONE_API_ROUTE)
#     print("Using url: %s" % url)
#     error_message = []
#
#     requester = routing.request(url, parameters.parameters())
#     for response in requester:
#
#         if response.status_code == requests.codes.OK:
#             if response.text is None:
#                 error_message.append(f"Retry {response.retry}: {response.message}")
#                 continue
#             result = IsochroneResult.parse_raw(response.text)
#             return result
#
#         error_message.append(f"Retry {response.retry}: {response.message}")
#
#
# if __name__ == "__main__":
#     # config = load_config(OUTPUT_DIR)
#     # config = load_config("isochrone_tests")
#
#     config = load_isochrone_config("isochrone_tests")
#
#     # test = config.IsochroneParams
#
#     # cutoffs = [dt.timedelta(time) for time in config]
#
#     # departure_time = dt.datetime(2023, 4, 12, 8, 30)
#     # TESTING -- Leave below
#     # Sheff:  53.383331, -1.466666
#     # Newcastle: 55.020825, -1.652973
#     # working exmaple url: http://localhost:8080/otp/traveltime/isochrone?location=%2853.383331%2C+-1.466666%29&time=2023-04-12T10:19:03%2B02:00&modes=WALK,TRANSIT&cutoff=60M
#     # params = IsochroneParameters(geometry.Point(53.383331, -1.466666), departure_time, [dt.timedelta(seconds=480), dt.timedelta(seconds=600)], [routing.Mode.WALK])
#     params = IsochroneParameters(location=geometry.Point(53.383331, -1.466666),
#                                  departure_time=dt.datetime(2020, 1, 13, 8, 30),
#                                  cutoff=[
#                                      dt.timedelta(seconds=120),
#                                      dt.timedelta(seconds=240),
#                                      dt.timedelta(seconds=360),
#                                      dt.timedelta(seconds=480),
#                                      dt.timedelta(seconds=600)
#                                  ],
#                                  modes=[routing.Mode.WALK, routing.Mode.TRAM],
#                                  )
#     req = requests.Request("GET", parse.urljoin("http://localhost:8080", ISOCHRONE_API_ROUTE),
#                            params=params.parameters())
#     prepared = req.prepare()
#
#     # req url now incorrectly formatted. replace + with T to specify time
#     prepared.url = prepared.url.replace(departure_time.strftime("%Y-%m-%d+%H"),
#                                         departure_time.strftime("%Y-%m-%dT%H"))
#
#     print(prepared.url)
