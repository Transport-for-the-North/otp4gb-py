# -*- coding: utf-8 -*-
"""Scratch script."""

##### IMPORTS #####
from __future__ import annotations

import collections
import enum
import logging
import pathlib
import re

import pandas as pd
from tqdm import tqdm

from otp4gb import cost

##### CONSTANTS #####
LOG = logging.getLogger(__name__)

##### CLASSES #####


##### FUNCTIONS #####
def check_response_data():
    path = pathlib.Path(
        r"C:\WSP_Projects\TfN BSIP\OTP4GB-Py Outputs\BODS GTFS Cheshire West & Chester\costs\TRANSIT_WALK_costs_20190910T0830.csv-response_data.jsonl"
    )
    output_folder = pathlib.Path(".temp/responses_dump")
    output_folder.mkdir(exist_ok=True)

    with open(path, "rt") as responses_file:

        count = 0
        for i, line in enumerate(responses_file, 1):
            results = cost.CostResults.parse_raw(line)

            if results.plan is None:
                continue

            if len(results.plan.itineraries) > 0:
                out_path = output_folder / f"response_dump_line_{i}.json"
                with open(out_path, "wt") as out:
                    out.write(results.json())
                print(f"Written: {out_path}")

                count += 1
                if count >= 10:
                    break


class EmploymentStatus(enum.StrEnum):
    EMPLOYEES = "employees"
    EMPLOYMENT = "employment"

    @staticmethod
    def fuzzy_find(s: str) -> EmploymentStatus:
        CUTOFF = 0.8

        ratios = {}
        for value in EmploymentStatus:
            ratio = Levenshtein.ratio(
                s, value, processor=str.lower, score_cutoff=CUTOFF
            )

            if ratio >= CUTOFF:
                ratios[value] = ratio

        if len(ratios) == 0:
            raise ValueError(f"no matching EmploymentStatus found for {s}")

        return max(ratios, key=ratios.get)


def load_bres_tsv():
    file = pathlib.Path(
        r"C:\Users\ukmjb018\OneDrive - WSP O365\WSP_Projects\TfN BSIP\BRES_industry_employment.tsv"
    )
    columns = {
        "Date": pd.CategoricalDtype([2021]),
        "Employment statu": pd.CategoricalDtype(list(EmploymentStatus)),
        "measure": "category",
        "2011 super output area - middle layer": str,
        "industry": "category",
        "value type": "category",
        "value": int,
    }

    bres = pd.read_csv(file, sep="\t", usecols=columns.keys(), dtype=columns)
    bres["Date"].astype(columns["Date"])


def get_str_size(s: str) -> int:
    return len(s.encode("utf-8"))


#### IMPORTANT #####
def filter_responses_data():
    response_file = pathlib.Path(
        r"I:\open-innovations\Bus TDIP\OTP4GB-Py Outputs\East Riding\costs\AM\BUS_WALK_costs_20190910T0900.csv-response_data.jsonl"
    )

    # PATH TO FILTERED OUTPUTS
    filtered_file = pathlib.Path(".temp/filtered_responses.jsonl")

    print(f"Filtering {filtered_file}")
    # LIST OF OD PAIR ZONE NAMES
    pairs = [
        ("Bassetlaw 001B", "Kingston upon Hull 026A"),
        ("Bassetlaw 001C", "North Lincolnshire 021D"),
        ("Bassetlaw 001B", "North Lincolnshire 009D"),
        ("Bassetlaw 001C", "North Lincolnshire 010A"),
        ("Bassetlaw 001B", "Selby 007A"),
        ("Bassetlaw 001B", "Kingston upon Hull 015A"),
        ("Bassetlaw 001C", "West Lindsey 002C"),
        ("Bassetlaw 001B", "Ryedale 003C"),
        ("Bassetlaw 001B", "Ryedale 003B"),
        ("Bassetlaw 001B", "West Lindsey 004F"),
        ("Bassetlaw 001C", "Doncaster 008E"),
    ]

    # REGEX TO FIND LINE WITH THE OD PAIRS SPECIFIED ABOVE - COMPILE REGEX TO DO SEARCH
    patterns = {(o, d): re.compile(rf'.*"{o}".*"{d}".*', re.I) for o, d in pairs}

    file_size = response_file.stat().st_size
    # OPEN RESPONSES
    with open(response_file, "rt", encoding="UTF-8") as file:
        # OPEN OUTPUTS FILE
        with open(filtered_file, "wt", encoding="UTF-8") as output:
            progress = tqdm(
                desc="Searching results file",
                total=file_size,
                unit="B",
                unit_scale=True,
            )
            # ITERATE OVER EVERY PAIR LINE IN THE FILE
            for line in file:
                progress.update(get_str_size(line))
                # IS THERE ANYTHING LEFT
                if len(patterns) == 0:
                    break

                # results = cost.CostResults.parse_raw(line.strip())

                found = None
                # FOR ALL PATTERNS SEE IF THERE IS A MATCH
                for i, pat in patterns.items():
                    if pat.match(line):
                        found = i

                if found is not None:
                    # IF THERE IS A MATCH, REMOVE FROM SEARCH & ADD TO FILE
                    patterns.pop(found)
                    output.write(line)
                    tqdm.write(
                        f"Found {found[0]} to {found[1]}, {len(patterns)} remaining"
                    )

            progress.close()
    # USE JSON LINES VIEWER VS CODE EXTENSION - {} IN TOP RIGHT CREATES A NICER FORMAT TO VIEW
    # RESULTS ARE IN "PLAN" - IF ITINERARIES IS EMPTY & NO ERROR - THERE ARE NO ROUTES
    # IF PLAN IS NULL & ERROR IS THERE, REPORT OTP ERROR
    # IF GETTING METRICS OF 6 COLS 7
    # ITINERARIES MAY BE EMPTY FOR A NUMBER OF REASONS (MAX WALK DISTANCE, WAIT TIME, ETC.)

    # ONE WAY OF VERIFYING ERRORS, IF I RUN SERVER & NOT PROCESS, WE CAN GO ONTO WEB VIEW OF OTP FOR ANALYSIS

    # COUPLE HUNDRED THOUSAND REQUESTS ISN'T TOO BAD - MATT'S BIGGEST RUN WAS (gb, LAD level)
    # BIGGER TOTAL AREA EACH REQUEST TAKES LONGER (MORE DATA INCLUDED EVERY TIME, LARGE RAM TOO! - WHOLE GB AT LAD USED 1S/REQ ~ 24 HRS )
    # WHEN MATT DID NORTH AS WHOLE, 1/2 S PER REQ, HALF AS LONG, HALF AS BIG AREA ~ 135,000 1/S = 36HRS
    # WHEN LOOKING AT CHESIRE/LIVERPOOL 100/1S PER REQUEST
    # RDP74 NORTH @ MSOA ~ THREE WEEKS (SLOW VM / 4 CORE / )
    # LSOA GB 1.6BILL | NORTH 400M, 1/10S

    # SEE IF PROGRESS BAR AGRESS WITH MY PREDICTION
    # LADs for GB 36Hrs (16Core / 64Gb/ no filter) - maybe MSOA North 1 week
    # Our filtering is required to get LSOAs
    # when run on 32 core machiene, java server could not keep up with number of requests sent from python (limited to ten threads atm)
    # IF I GET ERROR OTP SERVER NOT RESPONSDING reduce thread thing

    # dont need to all be LSOAs - can be multiple typers, a long as it's centroid coordinates
    # Manny CC LSOA 1 - 1:30, can instead reamove some LSOA'S and use a MSOA, or a SINGLE LAD
    # Could just use NoHAM - 2,700 or NoRMS zoning systems

    # if i cant find statistcs of average bus Journey time (5% > 30km, we can then set to 30km - even if there is a route, doesnt matter)
    # Could add check that people within GM don't want to travel, but if you're in cumbira (far away)
    # if when we filter on distance, and return < LSOAs, consider this place as rural so they may be willing to travel 50% further
    # so for these LSOAs set max_filter as 1.5*max_filter - returns a more similar number of requests for every LSOAs if possible
    # In other words, have larger radius's for less dense areas

    # alternatively, if not radius, for every LSOA we only want to consider getting to the nearest 10.
    # Probably just add a check to increase if you dont have many other zones returned

    # if radius filter reutns more than ten - leave at this,
    # if not, increase radious until you have at least 10 other LSOAs returned

    # filter by distance OR filter by number of other LSOA connections OR just do both!

    print(f"Written filtered to: {filtered_file}")


def count_response_types(path: pathlib.Path):

    file_size = path.stat().st_size
    progress = tqdm(
        desc="Searching results file", total=file_size, unit="B", unit_scale=True
    )

    results_split: dict[str, list[cost.CostResults]] = collections.defaultdict(list)

    error_msgs = set()
    otp_errors = set()

    with open(path, "rt", encoding="utf-8") as file:
        for line in file:
            result = cost.CostResults.model_validate_json(line)

            if result.plan is not None:
                if len(result.plan.itineraries) > 0:
                    results_split["has itineraries"].append(result)
                else:
                    results_split["no itineraries"].append(result)

            elif result.error is not None:
                error_msgs.add(result.error.msg)

                for msg in result.error.message.split("\n"):
                    msg = re.sub(r"\s*retry \d:\s*", "", msg, flags=re.I)
                    otp_errors.add(msg)

                results_split["error"].append(result)

            else:
                results_split["unknown"].append(result)

            progress.update(get_str_size(line))

    progress.close()

    print("Error Messages\n" + "-" * 20, *error_msgs, sep="\n - ")
    print("OTP Errors\n" + "-" * 20, *otp_errors, sep="\n - ")

    print("Writing output files")
    for nm, results in results_split.items():
        print(f"Found {len(results):,} for {nm}")
        out_path = path.with_name(path.stem + f"-{nm}.jsonl")

        with open(out_path, "wt", encoding="utf-8") as file:
            file.writelines(i.json() + "\n" for i in results)
        print(f"Written : {out_path}")


def main() -> None:
    # check_response_data()
    # filter_responses_data()
    path = pathlib.Path(".temp/TRANSIT_WALK_costs_20240415T1000-response_data.jsonl")
    count_response_types(path)


##### MAIN #####
if __name__ == "__main__":
    main()
