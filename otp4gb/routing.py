# -*- coding: utf-8 -*-
"""Module to send requests to the OTP routing API."""

##### IMPORTS #####
from __future__ import annotations
import dataclasses

# Standard imports
import datetime
import enum
import logging
import re
from typing import Any, Optional, Union
from urllib import parse

# Third party imports
import pydantic
import requests
from shapely import geometry

# Local imports

##### CONSTANTS #####
LOG = logging.getLogger(__name__)
ROUTER_API_ROUTE = "otp/routers/default/plan"
REQUEST_TIMEOUT = 200
REQUEST_RETRIES = 5
OTP_ERRORS = {
    "NO_TRANSIT": 406,
    "TOO_CLOSE": 409,
    "TRIP_IMPOSSIBLE": 400,
    "GEOCODE_TO_NOT_FOUND": 450,
}


##### CLASSES #####
class Mode(enum.StrEnum):
    TRANSIT = "TRANSIT"
    BUS = "BUS"
    RAIL = "RAIL"
    TRAM = "TRAM"
    WALK = "WALK"
    BICYCLE = "BICYCLE"

    @staticmethod
    def transit_modes() -> set[Mode]:
        return {Mode.TRANSIT, Mode.BUS, Mode.RAIL, Mode.TRAM}


class RoutePlanParameters(pydantic.BaseModel):
    fromPlace: geometry.Point
    toPlace: geometry.Point
    date: datetime.date
    time: datetime.time
    mode: Union[list[str], str]
    arriveBy: bool = False
    searchWindow: Optional[int] = None
    wheelchair: bool = False
    debugItineraryFilter: bool = False
    showIntermediateStops: bool = False
    maxWalkDistance: float = 1000
    locale: str = "en"

    class Config:
        arbitrary_types_allowed = True

    def params(self) -> dict[str, Any]:
        params: dict[str, Any] = {
            k: str(v) for k, v in self.dict().items() if v is not None
        }

        # Update any parameters which require specific creation
        params.update(
            {
                "fromPlace": [str(i) for i in self.fromPlace.coords],
                "toPlace": [str(i) for i in self.toPlace.coords],
                "time": self.time.strftime("%I:%M%p"),
                "date": self.date.strftime("%m-%d-%Y"),  # date in USA format
                "mode": ",".join([s.upper() for s in self.mode]),
            }
        )
        return params

    @pydantic.validator("fromPlace", "toPlace", pre=True)
    def _validate_point(cls, value) -> geometry.Point:
        if isinstance(value, geometry.Point):
            return value

        if isinstance(value, str):
            match = re.match(
                r"^[(\s]*"  # Optional starting bracket
                r"([-+]?\d+(?:\.\d+)?)[,\s]+"  # 1st int or float with separator
                r"([-+]?\d+(?:\.\d+)?)"  # 2nd number
                r"(?:[,\s]+([-+]?\d+(?:\.\d+)?))?"  # optional 3rd number with separator
                r"[\s)]*$",  # optional ending bracket
                value.strip(),
            )
            if match is None:
                raise ValueError(f"invalid point value string '{value}'")

            value = [n for n in match.groups() if n is not None]

        if isinstance(value, (tuple, list)):
            if len(value) in (2, 3):
                return geometry.Point(*[float(x) for x in value])
            raise ValueError(f"point should have 2 coordinates not {len(value)}")

        raise TypeError(
            f"expected collection or string of coordinates for point not '{value}'"
        )

    @pydantic.validator("date", pre=True)
    def _valid_date(cls, value) -> datetime.date:
        if isinstance(value, datetime.date):
            return value

        if isinstance(value, str):
            return datetime.datetime.strptime(value.strip(), "%m-%d-%Y").date()

        raise TypeError(f"expected date or string not {type(value)}")

    @pydantic.validator("time", pre=True)
    def _valid_time(cls, value) -> datetime.date:
        if isinstance(value, datetime.time):
            return value

        if isinstance(value, str):
            return datetime.datetime.strptime(value.strip(), "%H:%M%p").time()

        raise TypeError(f"expected time or string not {type(value)}")


class Place(pydantic.BaseModel):
    name: str
    id: Optional[str] = None
    zone_system: Optional[str] = None
    lon: float
    lat: float


class Leg(pydantic.BaseModel):
    startTime: datetime.datetime
    endTime: datetime.datetime
    distance: float
    mode: Mode
    transitLeg: bool
    duration: int

    class Config:
        extra = pydantic.Extra.allow


class Itinerary(pydantic.BaseModel):
    duration: int
    startTime: datetime.datetime
    endTime: datetime.datetime
    walkTime: int
    transitTime: int
    waitingTime: int
    walkDistance: float
    walkLimitExceeded: bool
    otp_generalised_cost: int = pydantic.Field(alias="generalizedCost")
    elevationLost: int
    elevationGained: int
    transfers: int
    legs: list[Leg]
    tooSloped: bool
    generalised_cost: Optional[float] = None
    # arrivedAtDestinationWithRentedBicycle: bool

    class Config:
        allow_population_by_field_name = True


class Plan(pydantic.BaseModel):
    date: datetime.datetime
    from_: Place = pydantic.Field(alias="from")
    to: Place
    itineraries: list[Itinerary]

    class Config:
        allow_population_by_field_name = True


class RoutePlanError(pydantic.BaseModel):
    id: int
    msg: str
    message: str
    missing: Optional[list[str]] = None


class RoutePlanResults(pydantic.BaseModel):
    requestParameters: RoutePlanParameters
    plan: Optional[Plan] = None
    error: Optional[RoutePlanError] = None
    debugOutput: Optional[dict] = None
    elevationMetadata: Optional[dict] = None


@dataclasses.dataclass
class _FakeResponse:
    """Storing data when request errors, within `get_route_itineraries`."""

    url: str
    status_code: int
    reason: str


##### FUNCTIONS #####
def get_route_itineraries(
    server_url: str, parameters: RoutePlanParameters
) -> tuple[str, RoutePlanResults]:
    """Request routing from Open Trip Planner server.

    Parameters
    ----------
    server_url : str
        URL for the OTP server.
    parameters : RoutePlanParameters
        Parameters for the route to request.

    Returns
    -------
    str
        Request URL, with parameters.
    RoutePlanResults
        OTP routing response.
    """

    def add_error(message: str) -> None:
        error_message.append(f"Retry {retries}: {message}")

    url = parse.urljoin(server_url, ROUTER_API_ROUTE)

    retries = 0
    error_message = []
    end = False
    while True:
        req = requests.Request("GET", url, params=parameters.params())
        prepared = req.prepare()

        try:
            session = requests.Session()
            response = session.send(prepared, timeout=REQUEST_TIMEOUT)
        except requests.exceptions.RequestException as error:
            msg = f"{error.__class__.__name__}: {error}"
            add_error(msg)
            response = _FakeResponse(url=prepared.url, status_code=-10, reason=msg)

        if response.status_code == requests.codes.OK:
            result = RoutePlanResults.parse_raw(response.text)
            if result.error is None:
                return response.url, result
            if result.error.id in OTP_ERRORS.values():
                return response.url, result

            add_error(
                f"OTP Error {result.error.id}: {result.error.msg} {result.error.message}"
            )

        if end or retries > REQUEST_RETRIES:
            error_message.append("max retries reached")
            result = RoutePlanResults(
                requestParameters=parameters,
                error=RoutePlanError(
                    id=response.status_code,
                    msg=f"Response {response.status_code}: {response.reason}",
                    message="\n".join(error_message),
                ),
            )
            return response.url, result

        retries += 1
