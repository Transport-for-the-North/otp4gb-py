# -*- coding: utf-8 -*-
"""Script to provide some QA metrics on BSIP2 cost-metrics datasets."""

# # # # IMPORTS # # # #
# sys
from typing import Tuple
import os
import pathlib

# 3rd party
import pandas as pd
import matplotlib.pyplot as plt
import sqlalchemy

# # # # CONSTANTS # # # #

# Should QA be done on a dataset uploaded to the TAME DB, or a local file?
# True - pulls data from TAME DB.
# False - reads data from CM_PATH, as a single path or list of paths.
DB_QA = True

# What run_id do you want to QA?
DB_RUN_ID = 121  # only needed if `DB_QA` is True
DB_NAME = "prod"
DB_SCHEMA = "bus_data"
DB_TABLE = "cost_metrics"
DB_ENGINE = ("pg8000",)  # or: psycopg2 | pygresql

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

# Store DB details into memory, only if we need them.
if DB_QA:
    # DB details should never be stored in a script
    DB_USERNAME = os.getenv("DB_USERNAME")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")


# # # # CLASSES & FUNCTIONS # # # #


def create_sql_query(
    schema: str,
    table: str,
    run_id: int,
) -> str:
    """Creates SQL query to pull data from cost_metrics"""
    return f"SELECT * FROM {schema}.{table} WHERE run_id = {str(run_id)}"


def pull_down_cost_metrics(
    db_conn_url: str,
    run_id: int,
    schema: str,
    table: str,
) -> pd.DataFrame | None:
    """
    Pulls down DB `table` data for specific `run_id` as pd.DataFrame
    """

    try:
        # Create the engine
        print("Connecting to DB to pull table data...")
        engine = sqlalchemy.create_engine(db_conn_url)

        # Test the connection
        with engine.connect() as conn:
            print("Connection successful")

        # Formulate query to select data from the passed table
        query = create_sql_query(
            schema=schema,
            table=table,
            run_id=run_id,
        )

        # Open connection and retrieve data
        print("Retrieving data from DB")
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
            print(f"{schema}.{table} read from {DB_NAME} with:\n{query}")

        return df

    except Exception as e:
        print(f"Error: {e}")
        return None


def create_db_url(
    username: str,
    password: str,
    host: str,
    port: str,
    database_name: str,
    engine: str,
) -> str:
    """
    Creates database connection url as string.

    Format: postgresql+psycopg2://username:password@host:port/database_name
    """

    url_fmt = (
        "postgresql+{engine}://{username}:{password}@{host}:{port}/{database_name}"
    )

    return url_fmt.format(
        username=username,
        password=password,
        host=host,
        port=port,
        database_name=database_name,
        engine=engine,
    )


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

    if DB_QA:
        filename = filename.strip(".csv")

    save_filename = f"{filename}_OTP_cost_metrics_qa_stats.csv"
    print()  # newline in terminal

    if not os.path.exists(out_path / filename):
        os.makedirs((out_path / filename), exist_ok=True)
        print(f"Created output dir: {out_path}")

    # Convert dict to dataframe - orienting dict keys to index of dataframe rather than columns
    stats_df = pd.DataFrame.from_dict(stats, orient="index", columns=[f"{filename}"])

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
    filename = filename.strip(".csv")  # try remove .csv - present if DB QA
    plot_filename = f"{filename}_mean_duration_distribution.png"
    plt.savefig((out_path / filename) / plot_filename)
    plt.close()

    print(f"Plot exported: {(out_path / filename) / plot_filename}")


def main(costs_data: pd.DataFrame) -> None:
    """main"""

    # Assess the types of trips returned
    valid_trips, itinerary_stats = qa_number_itineraries(cm_data=data)

    # Get metrics from returned valid trips
    trip_stats = qa_valid_trips(trip_data=valid_trips)

    # Combine all statistic dictionaries into a combined dict before turning into a DataFrame
    combined_stats = itinerary_stats
    combined_stats.update(trip_stats)

    # Convert statistics to pd.DataFrame and export as csv
    if DB_QA:
        filename = f"DB_QA_{DB_SCHEMA}.{DB_TABLE}_RUN_ID_{DB_RUN_ID}.csv"
    else:
        filename = CM_PATH.stem

    stats_dict_to_df(
        stats=combined_stats,
        out_path=STATISTICS_OUT_PATH,
        filename=filename,
    )

    # Plot and export distribution of observed trips
    plot_mean_duration_distribution(
        data=valid_trips, out_path=STATISTICS_OUT_PATH, filename=filename
    )

    print(f"\nProcess complete for {filename}")
    print("_" * 75)


# # # # PROCESS # # # #

if __name__ == "__main__":
    if DB_QA:
        # Create DB connection URL
        conn_url = create_db_url(
            username=DB_USERNAME,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            database_name=DB_NAME,
            engine="pg8000",
        )
        print(f"Using DB url: {conn_url}")

        # Download the dataset
        data = pull_down_cost_metrics(
            db_conn_url=conn_url,
            run_id=DB_RUN_ID,
            schema=DB_SCHEMA,
            table=DB_TABLE,
        )
        print("Data downloaded")

        # Run QA
        main(costs_data=data)

    else:  # Must be reading files locally from CM_PATH
        if not CM_PATH:
            raise ValueError(
                "`CM_PATH` is empty. Did you mean to perform QA on DB datasets?"
            )

        # If a single costs pathlib.Path is provided - just run main for that
        if isinstance(CM_PATH, pathlib.Path):
            # Load in data
            print(f"\nReading costs: {CM_PATH}")
            data = pd.read_csv(CM_PATH)
            print("Costs read")

            main(costs_data=data)

        # If path has been provided as a raw string, convert to pathlib.Path
        elif isinstance(CM_PATH, str):
            CM_PATH = pathlib.Path(CM_PATH)

            # Load in data
            print(f"\nReading costs: {CM_PATH}")
            data = pd.read_csv(CM_PATH)
            print("Costs read")

            main(costs_data=data)

        # Otherwise, if a list of paths are provided, run main on all paths.
        elif isinstance(CM_PATH, list):
            ALL_PATHS = CM_PATH
            print(f"Detected {len(CM_PATH):,} cost files. Processing iteratively.")

            for CM_PATH in ALL_PATHS:
                if not isinstance(CM_PATH, pathlib.Path):
                    CM_PATH = pathlib.Path(CM_PATH)

                # Load in data
                print(f"\nReading costs: {CM_PATH}")
                data = pd.read_csv(CM_PATH)
                print("Costs read")

                main(costs_data=data)

    print("\n\nScript complete")
