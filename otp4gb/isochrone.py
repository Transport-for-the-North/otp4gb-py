# -*- coding: utf-8 -*-
"""Functionality for requesting isochrones from OTP."""

#### IMPORTS ####
import dataclasses

# Standard imports
import datetime as dt
import pytz
import logging
from typing import Any
from urllib import parse
import json

# Third party imports
import pydantic
import requests
from shapely import geometry

# Local imports
from otp4gb import routing

# CONSTANTS ####
LOG = logging.getLogger(__name__)
ISOCHRONE_API_ROUTE = "otp/traveltime/isochrone"
# WALK is not supported standalone arg - mix with another irrelevant mode
WALK_PAIR_MODE = "FERRY"


#### CLASSES ####
class IsochroneParameters(pydantic.BaseModel):
    location: geometry.Point
    departure_time: dt.datetime
    cutoff: list[dt.timedelta]
    modes: list[routing.Mode]

    def parameters(self) -> dict[str, Any]:

        modes = [i.value for i in self.modes]

        # Cannot just be WALK, should be WALK and some other mode
        for i in range(len(modes)):
            if modes[i] == "WALK":
                modes[i] = ",".join(("WALK", WALK_PAIR_MODE))

        # OTP4GB - Assumed GB Timezone (otp 4 GB<--)
        time_zone = pytz.timezone("Europe/London")
        aware_time = self.departure_time.astimezone(time_zone)

        return dict(
            location=[str(i) for i in self.location.coords],
            time=aware_time,
            cutoff=[_format_cutoff(i) for i in self.cutoff],
            modes=modes,
        )

    class Config:
        arbitrary_types_allowed = True


class IsochroneResult(pydantic.BaseModel):
    # """Cost results from OTP saved to responses JSON file."""
    #
    # origin: routing.Place
    # destination: routing.Place
    # plan: Optional[routing.Plan]
    # error: Optional[routing.RoutePlanError]
    # request_url: str
    """Isochrone results from OTP"""
    # location: geometry.Point
    response: str

    # @pydantic.validator("location", pre=True)
    # def _check_point(cls, value):
    #     if not isinstance(value, geometry.Point):
    #         value = geometry.Point(value)


class IsochroneResponse(pydantic.BaseModel):
    """Gets & parses OTP response for Isochrone request"""
    response: str

    def parse_response(self):
        results = json.loads(self.response)

        isochrones = results.get("features")


#### FUNCTIONS ####
def _format_cutoff(cutoff: dt.timedelta) -> str:
    seconds = round(cutoff.total_seconds())
    minutes = 0
    hours = 0

    if seconds > 60:
        minutes, seconds = divmod(seconds, 60)

    if minutes > 60:
        hours, minutes = divmod(minutes, 60)

    text = ""
    for name, value in (("H", hours), ("M", minutes), ("S", seconds)):
        if value > 0:
            text += f"{value}{name}"

    return text


def get_isochrone(parameters: IsochroneParameters, server_url: str) -> IsochroneResult:
    url = parse.urljoin(server_url, ISOCHRONE_API_ROUTE)
    error_message = []

    requester = routing.request(url, parameters.parameters())
    for response in requester:

        if response.status_code == requests.codes.OK:
            if response.text is None:
                # logging.log(f"INNER Retry {response.retry}: {response.message}")
                error_message.append(f"Retry {response.retry}: {response.message}")
                continue

            result = response.text.text
            return response.url, result

        error_message.append(f"Retry {response.retry}: {response.message}")

    # The final error message is duplicated from the generator. Remove if identical
    if error_message[-1] == error_message[-2]:
        del error_message[-1]

    error_message = ", ".join(([msg for msg in error_message]))
    # print("final return", error_message)
    return response.url, error_message


if __name__ == "__main__":
    print("placeholder")    # config = load_config(OUTPUT_DIR)
    # config = load_config("isochrone_tests")

    # config = load_isochrone_config("isochrone_tests")

    # test = config.IsochroneParams

    # cutoffs = [dt.timedelta(time) for time in config]

    # departure_time = dt.datetime(2023, 4, 12, 8, 30)
    # TESTING -- Leave below
    # Sheff:  53.383331, -1.466666
    # Newcastle: 55.020825, -1.652973
    # working example url: http://localhost:8080/otp/traveltime/isochrone?location=%2853.383331%2C+-1.466666%29&time=2023-04-12T10:19:03%2B02:00&modes=WALK,TRANSIT&cutoff=60M
    # params = IsochroneParameters(geometry.Point(53.383331, -1.466666), departure_time, [dt.timedelta(seconds=480), dt.timedelta(seconds=600)], [routing.Mode.WALK])
    # params = IsochroneParameters(location=geometry.Point(53.383331, -1.466666),
    #                              departure_time=dt.datetime(2020, 1, 13, 8, 30),
    #                              cutoff=[
    #                                  dt.timedelta(seconds=120),
    #                                  dt.timedelta(seconds=240),
    #                                  dt.timedelta(seconds=360),
    #                                  dt.timedelta(seconds=480),
    #                                  dt.timedelta(seconds=600)
    #                              ],
    #                              modes=[routing.Mode.WALK, routing.Mode.TRAM],
    #                              )
    # req = requests.Request("GET", parse.urljoin("http://localhost:8080", ISOCHRONE_API_ROUTE), params=params.parameters())
    # prepared = req.prepare()
    #
    # # req url now incorrectly formatted. replace + with T to specify time
    # prepared.url = prepared.url.replace(departure_time.strftime("%Y-%m-%d+%H"),
    #                                     departure_time.strftime("%Y-%m-%dT%H"))
    #
    # print(prepared.url)
