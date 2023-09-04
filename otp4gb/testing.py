# -*- coding: utf-8 -*-
"""
Created on: 29/08/2023
Updated on:

Original author: Ben Taylor
Last update made by:
Other updates made by:

File purpose:

"""
# Built-Ins

# Third Party

# Local Imports
# pylint: disable=import-error,wrong-import-position
# Local imports here
# pylint: enable=import-error,wrong-import-position

# # # CONSTANTS # # #

# # # CLASSES # # #

# # # FUNCTIONS # # #


from pydantic.networks import AnyHttpUrl
from pydantic import BaseModel


class IsUrl(BaseModel):
    url: AnyHttpUrl


valid_url = "http://localhost:8080/otp/traveltime/isochrone?location=%2853.383331%2C+-1.466666%29&time=2023-04-12T10:19:03%2B02:00&modes=WALK,TRANSIT&cutoff=60M"
valid_url_2 = "http://www.google.com"
valid_url_3 = "http://www.exampleurl.com"

invalid_url = "http://http://localhost:8080/otp/traveltime/isochrone?location=%2853.383331%2C+-1.466666%29&time=2023-04-12T10:19:03%2B02:00&modes=WALK,TRANSIT&cutoff=60M"
invalid_url_2 = "http://localhost:8080:8080/ors/traveltime"
invalid_url_3 = "http://localhost:8080:20330/ors/traveltime"

urls = [
    valid_url,
    valid_url_2,
    valid_url_3,
    invalid_url,
    invalid_url_2,
    invalid_url_3,
]

for url in urls:
    try:
        IsUrl(url=url)
    except ValueError as e:
        print("ERROR: {}\n\n {}".format(url, e))

#print("should be valid", IsUrl(url=valid_url))
#print("should be invalid", IsUrl(url=invalid_url))
