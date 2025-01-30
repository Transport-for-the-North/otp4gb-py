# -*- coding: utf-8 -*-
"""Script to provide some QA metrics on BSIP2 cost-metrics datasets."""

# # # # IMPORTS # # # #
# sys
from typing import List, Tuple, Generator
import os
import pathlib
import json
import argparse

# 3rd party
import pandas as pd
import matplotlib.pyplot as plt
import sqlalchemy

# # # # CONSTANTS # # # #

_ENV_VARS = [
    "DB_USERNAME",
    "DB_PASSWORD",
    "DB_HOST",
    "DB_PORT",
]

DB_DRIVERNAME = "postgresql+pg8000"  # or: psycopg2 | pygresql

# Seconds in a minute
MIN_SECONDS = 60

# Number of bins to use in histogram plots
N_BINS = 150

STATISTICS_OUT_PATH = pathlib.Path(os.path.join(os.getcwd()), "qa outputs")

STR2BOOL_TRUE = [
    "true",
    "yes",
    "t",
    "y",
    "1",
]
STR2BOOL_FALSE = [
    "false",
    "no",
    "f",
    "n",
    "0",
]

# # # # CLASSES & FUNCTIONS # # # #


def str2bool(string):
    """
    Appropriately convert a string to boolean
    """

    if isinstance(string, bool):
        return string

    if string.strip().lower() in STR2BOOL_TRUE:
        return True
    if string.strip().lower() in STR2BOOL_FALSE:
        return False


def parse_args():
    """
    Parses command line arguments into Python variables
    """
    parser = argparse.ArgumentParser(
        description="Perform QA metrics on BSIP2 cost-metrics datasets"
    )

    # Argument to pass QA source - DB or local files
    parser.add_argument(
        "--db_qa",
        "--db",
        help="Should the QA be performed on a dataset that is in the TAME DB",
        type=str2bool,
    )

    # Argument to pass specific run_id to pull from bus_data.cost_metrics
    parser.add_argument(
        "--db_run_id",
        "--run_id",
        help="Specific run_id to pull from whole cost_metrics table",
        type=int,
    )

    # Argument for DB name
    parser.add_argument(
        "--db_name",
        help="What database should be read from i.e. 'prod' (default)",
        default="prod",
        type=str,
    )

    # DB Schema name
    parser.add_argument(
        "--db_schema",
        help="What database schema should be read i.e. 'bus_data' (default)",
        default="bus_data",
        type=str,
    )

    # DB Table name
    parser.add_argument(
        "--db_table",
        help="What database table should be read i.e. `cost_metrics` (default)",
        default="cost_metrics",
        type=str,
    )

    # list of local cost-metrics paths
    parser.add_argument(
        "--local_paths",
        help="json.dumps() of a List of pathlib.Path objects to "
        "be read in and QA'd by the process",
        type=str,
    )

    return parser.parse_args()


def validate_parsed_args(args):
    """
    Validates args passed via argparse
    """
    print(args)

    # If we're NOT doing DB QA, only need variable all_paths
    if not args.db_qa:
        if not args.local_paths:
            raise ValueError("--local_paths must be provided if --db_qa is False")
    # Must be doing DB QA
    else:
        if not args.db_run_id:
            raise ValueError("--db_run_id is required when --db_qa is True")
        if not args.db_name:
            raise ValueError("--db_name is required when --db_qa is True")
        if not args.db_schema:
            raise ValueError("--db_schema is required when --db_qa is True")
        if not args.db_table:
            raise ValueError("--db_table is required when --db_qa is True")


def check_paths(paths: List[pathlib.Path]) -> None:
    """
    Checks provided paths exist
    """

    broken = []
    for _path in paths:
        _path = pathlib.Path(_path)

        if not _path.is_file():
            broken.append(str(_path))

    if broken:
        raise ValueError(f"Files do not exist: {' '.join(broken)}")


def generate_paths(paths) -> Generator[pathlib.Path, None, None]:
    """
    Generator function to yield each path in the list of paths.
    """
    for _path in paths:
        yield pathlib.Path(_path)


def validate_env_vars(env_vars: List[str]) -> None:
    """
    `getenv` by default returns None if the env variable isn't set
    """
    missing = []
    for env_var in env_vars:
        if os.getenv(env_var) is None:
            missing.append(env_var)

    if missing:
        raise ValueError(
            f"Missing environment variables: {' '.join(missing)} "
            f"Please go and set these."
        )


def create_sql_query(schema: str, table: str, run_id: int, engine) -> str:
    """Creates SQL query to pull data from cost_metrics using sqlalchemy"""

    metadata = sqlalchemy.MetaData(schema=schema)

    cost_metrics_table = sqlalchemy.Table(table, metadata, autoload_with=engine)

    return sqlalchemy.select(cost_metrics_table).where(
        cost_metrics_table.c.run_id == run_id
    )


def pull_down_cost_metrics(
    db_conn_url: str,
    run_id: int,
    schema: str,
    table: str,
) -> pd.DataFrame | None:
    """
    Pulls down DB `table` data for specific `run_id` as pd.DataFrame
    """

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
        engine=engine,
    )

    # Open connection and retrieve data
    print("Retrieving data from DB")
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
        print(f"{schema}.{table} read with:\n{query}")

    return df


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
    plot_filename = f"{filename}_mean_duration_distribution.png"
    plt.savefig((out_path / filename) / plot_filename)
    plt.close()

    print(f"Plot exported: {(out_path / filename) / plot_filename}")


def main(costs_data: pd.DataFrame, costs_path: pathlib.Path) -> None:
    """main"""

    # Assess the types of trips returned
    valid_trips, itinerary_stats = qa_number_itineraries(cm_data=data)

    # Get metrics from returned valid trips
    trip_stats = qa_valid_trips(trip_data=valid_trips)

    # Combine all statistic dictionaries into a combined dict before turning into a DataFrame
    combined_stats = itinerary_stats
    combined_stats.update(trip_stats)

    filename = costs_path.stem

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


def _run() -> Generator[pd.DataFrame, None, None]:
    """
    Generator run function to call main with either local data, or DB data.

    main() accepts a pd.DataFrame dataset, this function should create that
        pd.DataFrame from a relevant source, and return it for main()
    """

    args = parse_args()
    validate_parsed_args(args)

    DB_QA = bool(args.db_qa)

    # If pulling from the database
    if DB_QA:
        DB_NAME = args.db_name
        DB_SCHEMA = args.db_schema
        DB_TABLE = args.db_table
        DB_RUN_ID = args.db_run_id

        # DB details should never be stored in a script
        DB_USERNAME = os.getenv("DB_USERNAME")
        DB_PASSWORD = os.getenv("DB_PASSWORD")
        DB_HOST = os.getenv("DB_HOST")
        DB_PORT = os.getenv("DB_PORT")

        # Check required environment variables have been set correctly
        validate_env_vars(env_vars=_ENV_VARS)

        conn_url = sqlalchemy.engine.URL.create(
            drivername=DB_DRIVERNAME,
            username=DB_USERNAME,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
        )

        print(f"Using DB url: {conn_url}")

        # Download the dataset
        _data = pull_down_cost_metrics(
            db_conn_url=conn_url,
            run_id=DB_RUN_ID,
            schema=DB_SCHEMA,
            table=DB_TABLE,
        )
        print("Data downloaded from database")

        fake_db_path = pathlib.Path(f"DB_QA_{DB_SCHEMA}-{DB_TABLE}_RUN_ID_{DB_RUN_ID}")

        yield _data, fake_db_path

    # Must be reading files locally from CM_PATH
    else:
        CM_PATHS = json.loads(args.local_paths)

        # convert CM_PATHS to list of paths
        if isinstance(CM_PATHS, (pathlib.Path, str)):
            all_paths = [CM_PATHS]
        else:
            # Assume CM_PATH is a list or iterable
            all_paths = CM_PATHS

        # Checks that all paths passed return True to path.is_file()
        check_paths(all_paths)

        print(f"Detected {len(all_paths):,} cost files. Processing iteratively.")

        for _path in generate_paths(all_paths):
            # Load in data
            print(f"\nReading costs: {_path}")
            _data = pd.read_csv(_path, low_memory=False)
            print("Costs read")

            yield _data, _path


# # # # PROCESS # # # #

if __name__ == "__main__":
    # Retrieve relevant datasets
    for data, path in _run():
        if isinstance(data, pd.DataFrame):
            # Run QA on DataFrame from TAME database
            main(costs_data=data, costs_path=path)

    print("\n\nScript complete")
