# -*- coding: utf-8 -*-
"""
Created on: 27/01/2025
Updated on:

Original author: Your Name
Last update made by:
Other updates made by:

File purpose: Script to test the new QA metrics script.
"""

# # # # IMPORTS # # # #
import subprocess
import threading
import pathlib
import json


# # # # CONSTANTS # # # #
SCRIPT_NAME = "cost_metrics_qa.py"  # Script to perform QA

#  1: True, 0: False
DB_QA = 1  # Perform QA on a TAME DB cost metrics dataset?
DB_RUN_ID = 121  # run_id to filter out from cost_metrics if doing DB QA

LOCAL_PATHS = [
    pathlib.Path(r"T:\4JH\BSIP2 Scheduled costs\TRANSIT_WALK_costs_20240415T1000-metrics.csv"),
    pathlib.Path(r"E:\Current Work\ARCHIVED\2023 OTP_Processing\OTP outputs\TRSE OTP Related runs\GM_test\costs\AM\BUS_WALK_costs_20230608T0900-metrics.csv"),
    pathlib.Path(r"F:\OTP4GB-py\Scheduled Outputs\OTP TT3 BSIP North West - 20240601\costs\AM\TRANSIT_WALK_costs_20240415T0900-metrics.csv"),
]
# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ #
#########################################################################################
# THE USER SHOULD ONLY NEED TO UPDATE CM_PATHS WITH FULL PATHS TO cost-metrics.csv FILES #

# Should the test functions be run?
TEST_FUNCTIONALITY = True

# # # # FUNCTIONS # # # #


def test_db_qa():
    """
    Call the cost_metrics_qa process using DB dataset
    """
    # Test with DB_QA = True
    command = [
        "python",
        SCRIPT_NAME,
        "--db_run_id", str(DB_RUN_ID),
        "--db_qa", "True",
    ]

    # Execute the command
    print(f"\nRunning command:\n'{' '.join(command)}'\n\n")
    _run_command(command)
    print(f"\nScript {SCRIPT_NAME} complete with DB_QA=True\n\n\n")


def test_local_qa():
    """
    Call the cost_metrics_qa process using local files
    """

    # Windows paths are not JSON serializable - convert to str
    local_paths = [str(path) for path in LOCAL_PATHS]

    # Test with DB_QA = False
    command = [
        "python",
        SCRIPT_NAME,
        "--db_qa", "false",
        "--local_paths", json.dumps(local_paths),
    ]

    # Execute the command
    print(f"Running command:\n'{' '.join(command)}'")
    _run_command(command)
    print(f"\nScript {SCRIPT_NAME} complete with DB_QA=False\n\n\n")


def _run_command(command):
    """
    Execute a command with subprocess and print all output messages to console in real-time.
    """

    def _stream_output(pipe, prefix):
        """Streams outputs from subprocess"""
        for line in iter(pipe.readline, ''):
            print(f"{prefix}: {line}", end='')

    # Execute the command and capture the output in real-time
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, universal_newlines=True)

    # Create threads to read stdout and stderr
    stdout_thread = threading.Thread(target=_stream_output, args=(process.stdout, "Output"))
    stderr_thread = threading.Thread(target=_stream_output, args=(process.stderr, "Error"))

    # Start the threads
    stdout_thread.start()
    stderr_thread.start()

    # Wait for the process to complete
    process.wait()

    # Wait for the threads to complete
    stdout_thread.join()
    stderr_thread.join()


def main():
    """
    Main function to test the new script.
    """

    if TEST_FUNCTIONALITY:
        test_db_qa()
        test_local_qa()
        quit()

    # Convert provided paths to str - not json serializable
    local_paths = [str(path) for path in LOCAL_PATHS]

    command = [
        "python",
        SCRIPT_NAME,
        "--db_qa", DB_QA,
        "--local_paths", json.dumps(local_paths),
    ]

    # Execute the command
    print(f"Running command:\n'{' '.join(command)}'")
    _run_command(command)
    print(f"\nScript {SCRIPT_NAME} complete\n")


if __name__ == "__main__":
    main()
    print("\n\nProcess complete.")
