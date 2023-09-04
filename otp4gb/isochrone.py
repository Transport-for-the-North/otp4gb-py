# -*- coding: utf-8 -*-
"""Functionality for requesting isochrones from OTP."""

##### IMPORTS #####
# Standard imports
import dataclasses
import datetime as dt
import logging
from typing import Any
from urllib import parse

# Third party imports
import pydantic
import requests
from shapely import geometry

# Local imports
from otp4gb import routing

##### CONSTANTS #####
LOG = logging.getLogger(__name__)
ISOCHRONE_API_ROUTE = "otp/traveltime/isochrone"


##### CLASSES #####
@dataclasses.dataclass
class IsochroneParameters:
    location: geometry.Point
    departure_time: dt.datetime
    cutoff: list[dt.timedelta]
    modes: list[routing.Mode]

    def parameters(self) -> dict[str, Any]:
        return dict(
            location=[str(i) for i in self.location.coords],
            time=self.departure_time.isoformat(),
            cutoff=[_format_cutoff(i) for i in self.cutoff],
            modes=[i.value for i in self.modes],
        )


class IsochroneResult(pydantic.BaseModel):
    ...


##### FUNCTIONS #####
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


def get_isochrone(server_url: str, parameters: IsochroneParameters) -> IsochroneResult:
    url = parse.urljoin(server_url, ISOCHRONE_API_ROUTE)

    error_message = []

    requester = routing.request(url, parameters.parameters())
    for response in requester:

        if response.status_code == requests.codes.OK:
            if response.text is None:
                error_message.append(f"Retry {response.retry}: {response.message}")
                continue
            result = IsochroneResult.parse_raw(response.text)
            return result

        error_message.append(f"Retry {response.retry}: {response.message}")