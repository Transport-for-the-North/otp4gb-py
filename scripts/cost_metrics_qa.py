# -*- coding: utf-8 -*-
"""
Created on Wed Jan  8 13:24:16 2025

@author: Signalis

Script to provide some QA metrics on BSIP2 cost-metrics dataests

"""

# # # # IMPORTS # # # #
# sys
from typing import Tuple
import os
import pathlib

# 3rd party
import pandas as pd
import matplotlib.pyplot as plt


# # # # CONSTANTS # # # #

# Path to cost metrics dataset OR list of paths to be processed iteratively
CM_PATH = pathlib.Path(
    r"T:\4JH\BSIP2 Scheduled costs\TRANSIT_WALK_costs_20240415T1000-metrics.csv"
)

# If you want outputs for multiple cost metrics, CM_PATH should be a list of Paths
# CM_PATH = [
#     pathlib.Path(r"T:\4JH\BSIP2 Scheduled costs\TRANSIT_WALK_costs_20240415T1000-metrics.csv"),
#     pathlib.Path(r"E:\Current Work\ARCHIVED\2023 OTP_Processing\OTP outputs\TRSE OTP Related runs\GM_test\costs\AM\BUS_WALK_costs_20230608T0900-metrics.csv"),
#     pathlib.Path(r"F:\OTP4GB-py\Scheduled Outputs\OTP TT3 BSIP North West - 20240601\costs\AM\TRANSIT_WALK_costs_20240415T0900-metrics.csv"),
# ]

# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ #
#########################################################################################
# THE USER SHOULD ONLY NEED TO UPDATE CM_PATH WITH FULL PATHS TO cost-metrics.csv FILES #
#########################################################################################

# Seconds in a minute
MIN_SECONDS = 60

# Number of bins to use in histogram plots
N_BINS = 150

STATISTICS_OUT_PATH = pathlib.Path(os.path.join(os.getcwd()), "qa outputs")


# # # # CLASSES & FUNCTIONS # # # #


def qa_number_itineraries(cm_data: pd.DataFrame) -> Tuple[pd.DataFrame, dict]:
    """
    Prints statistics from assessed cost metrics dataset. Returns a dataframe
       of valid trips for later assessment
    """

    all_trips = len(cm_data)

    # Split data into: not poss trips | errored trips | valid trips
    # Trip not poss
    zero_itin = cm_data[cm_data["number_itineraries"] == 0].copy()
    # Trip errored
    na_itin = cm_data[cm_data["number_itineraries"].isna()].copy()
    # Valid trips
    real_itin = cm_data[cm_data["number_itineraries"] > 0].copy()

    # Assert that each of the three sub-DFs match the length of the input DF
    assert len(zero_itin) + len(na_itin) + len(real_itin) == len(
        cm_data
    ), "Lengths do not match"

    # Calculate percentages
    trip_pct = round((len(real_itin) / all_trips) * 100, 2)
    no_trip_pct = round((len(zero_itin) / all_trips) * 100, 2)
    err_trip_pct = round((len(na_itin) / all_trips) * 100, 2)

    # Print stats in console for user
    print(f"\nTotal trip requests: {all_trips:,}")
    print(f"Trips with data: {len(real_itin):,} ({trip_pct}% of all requests)")
    print(f"Trips not possible: {len(zero_itin):,} ({no_trip_pct}% of all requests)")
    print(f"Null / Errored trips: {len(na_itin):,} ({err_trip_pct}% of all requests)")

    # Store stats for export later
    stats = {
        "Total trip requests sent": all_trips,
        "Trips with data returned": len(real_itin),
        "Trips not possible returned": len(zero_itin),
        "Null / Errored trips returned": len(na_itin),
        "Trips with data (%)": trip_pct,
        "Trips not possible (%)": no_trip_pct,
        "Null / Errored trips (%)": err_trip_pct,
    }

    return real_itin, stats


def qa_valid_trips(trip_data: pd.DataFrame) -> dict:
    """
    Assess dataframe of returned trips from OTP. Prints and stores various trip statistics
       to be exported later
    """
    mean_mean_duration_mins = round(trip_data["mean_duration"].mean() / MIN_SECONDS, 2)
    mode_mean_duration_mins = round(trip_data["mean_duration"].mode() / MIN_SECONDS, 2)
    max_mean_duration_mins = round(trip_data["mean_duration"].max() / MIN_SECONDS, 2)
    min_mean_duration_mins = round(trip_data["mean_duration"].min() / MIN_SECONDS, 2)

    # Sometimes, multiple modes are returned
    if len(mode_mean_duration_mins) == 1:
        mode_mean_duration_mins = mode_mean_duration_mins.iloc[0]
    else:
        # More than one mode returned
        print("Averaging observed modes into one")
        mode_mean_duration_mins = mode_mean_duration_mins.mean()

    # Print stats in console for user
    print(f"\nFrom {len(trip_data):,} valid trips returned:")
    print(f"Mean trip durations: {mean_mean_duration_mins:,} minutes")
    print(f"Mode trip durations: {mode_mean_duration_mins:,} minutes")
    print(f"Max trip duration: {max_mean_duration_mins:,} minutes")
    print(f"Min trip duration: {min_mean_duration_mins:,} minutes")

    # Store stats for export later
    stats = {
        "Mean trip duration minutes": mean_mean_duration_mins,
        "Mode trip duration minutes": mode_mean_duration_mins,
        "Max trip duration minutes": max_mean_duration_mins,
        "Min trip duration minutes": min_mean_duration_mins,
    }

    return stats


def stats_dict_to_df(
    stats: dict,
    out_path: pathlib.Path,
    filename: str,
) -> None:
    """
    Converts statistics dictionary to pd.DataFrame and exports the dataframe
       as .csv to out_path / filename
    """
    save_filename = f"{filename}_OTP_cost_metrics_qa_stats.csv"
    print()  # newline in terminal

    if not os.path.exists(out_path / filename):
        os.makedirs((out_path / filename), exist_ok=True)
        print(f"Created output dir: {out_path}")

    # Convert dict to dataframe - orienting dict keys to index of dataframe rather than columns
    stats_df = pd.DataFrame.from_dict(
        stats, orient="index", columns=[f"{CM_PATH.name}"]
    )

    # Export
    stats_df.to_csv((out_path / filename) / save_filename)

    print(f"Exported statistics to: {(out_path / filename) / save_filename}")


def plot_mean_duration_distribution(
    data: pd.DataFrame,
    out_path: pathlib.Path,
    filename: str,
) -> None:
    """
    Plot the distribution of mean durations and export the plot
    """
    print("\nCreating JT distribution plot")

    # Subset data & convert seconds to mins
    mean_duration = data["mean_duration"] / MIN_SECONDS

    # Create the plot
    plt.figure(figsize=(10, 6))
    plt.hist(mean_duration, bins=N_BINS, edgecolor="k", alpha=0.7)
    plt.title(f"Distribution of Mean Trip Durations for {filename}")
    plt.xlabel("Mean Trip Duration (minutes)")
    plt.ylabel("Frequency")

    # Save the plot as a PNG file
    plot_filename = f"{filename}_mean_duration_distribution.png"
    plt.savefig((out_path / filename) / plot_filename)
    plt.close()

    print(f"Plot exported: {(out_path / filename) / plot_filename}")


def main():
    """main"""
    # Load in data
    print(f"\nReading costs: {CM_PATH}")
    data = pd.read_csv(CM_PATH)
    print("Costs read")

    # Assess the types of trips returned
    valid_trips, itinerary_stats = qa_number_itineraries(cm_data=data)

    # Get metrics from returned valid trips
    trip_stats = qa_valid_trips(trip_data=valid_trips)

    # Combine all statistic dictionaries into a combined dict before turning into a DataFrame
    combined_stats = itinerary_stats
    combined_stats.update(trip_stats)

    # Convert statistics to pd.DataFrame and export as csv
    stats_dict_to_df(
        stats=combined_stats,
        out_path=STATISTICS_OUT_PATH,
        filename=CM_PATH.stem,
    )

    # Plot and export distribution of observed trips
    plot_mean_duration_distribution(
        data=valid_trips, out_path=STATISTICS_OUT_PATH, filename=CM_PATH.stem
    )

    print(f"\nProcess complete for {CM_PATH.name}")
    print("_" * 75)


# # # # PROCESS # # # #

if __name__ == "__main__":

    # If a single costs pathlib.Path is provided - just run main for that
    if isinstance(CM_PATH, pathlib.Path):
        main()

    # If path has been provided as a raw string, convert to pathlib.Path
    elif isinstance(CM_PATH, str):
        CM_PATH = pathlib.Path(CM_PATH)
        main()

    # Otherwise, if a list of paths are provided, run main on all paths.
    elif isinstance(CM_PATH, list):
        ALL_PATHS = CM_PATH
        print(f"Detected {len(CM_PATH):,} cost files. Processing iteratively.")

        for CM_PATH in ALL_PATHS:
            if not isinstance(CM_PATH, pathlib.Path):
                CM_PATH = pathlib.Path(CM_PATH)
            main()

    print("\n\nScript complete")
