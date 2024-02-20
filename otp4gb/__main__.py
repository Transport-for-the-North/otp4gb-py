# -*- coding: utf-8 -*-
"""Python package for running OpenTripPlanner v2."""

##### IMPORTS #####

import argparse
import datetime as dt
import logging
import pathlib

from caf.toolkit import log_helpers

from otp4gb import otp

##### CONSTANTS #####

LOG = logging.getLogger(__name__)


##### CLASSES & FUNCTIONS #####


def create_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        __package__,
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    subparsers = parser.add_subparsers(
        title="Sub-Commands",
        description="List of available sub-commands for "
        "running the different OTP processes",
    )

    process_parser = subparsers.add_parser(
        "process",
        help="perform routing analysis with OpenTripPlanner",
        description="Prepare the OTP graph (if it doesn't already exist) and "
        "perform routing analysis and output cost metrics based on config "
        "file in given folder",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    process_parser.add_argument(
        "-b", "--bounds", help="name of bounds defined in bounds.yml for the filter"
    )
    process_parser.add_argument(
        "-d",
        "--date",
        type=dt.date.fromisoformat,
        help="date for routing graph preparation",
    )
    process_parser.add_argument(
        "-F",
        "--force",
        action="store_true",
        help="force recreation of the OTP graph file",
    )
    process_parser.add_argument(
        "-s",
        "--save_parameters",
        action="store_true",
        help="save build parameters to JSON lines files and exit",
    )
    process_parser.add_argument(
        "-p",
        "--prepare",
        action="store_true",
        help="prepare the OTP graph and GTFS files without running routing analysis",
    )

    server_parser = subparsers.add_parser(
        "server",
        help="run OTP server for existing inputs",
        description="Run the OTP server for an existing set of OTP inputs, "
        "inputs can be created prior using 'process' command with "
        "'--prepare' flag.",
    )

    # Arguments available for both sub-commands
    for p in (server_parser, process_parser):
        p.add_argument(
            "folder",
            type=pathlib.Path,
            help="folder containing config file and OTP graphs",
        )

    process_parser.set_defaults(func=otp.run_process)
    server_parser.set_defaults(func=otp.run_server)

    return parser


def main() -> None:
    parser = create_argument_parser()
    args = parser.parse_args()

    # TODO(MB) Define package version somewhere
    details = log_helpers.ToolDetails(__package__, "0.1.0")

    output_folder: pathlib.Path = args.folder
    log_file = output_folder / f"logs/otp4gb-{dt.date.today():%Y%m%d}.log"
    log_file.parent.mkdir(exist_ok=True)

    with log_helpers.LogHelper(__package__, tool_details=details, log_file=log_file):
        args.func(**vars(args))


if __name__ == "__main__":
    main()
