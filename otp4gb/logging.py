"""
Logging module for OTP4GB-py

TODO(MB) Should be replaced with caf.toolkit logging when available.
"""
import logging
import os
import pathlib
import sys

from otp4gb.config import LOG_DIR

FORMAT_STRING = (
    "%(asctime)s.%(msecs)03d:%(levelname)s:%(processName)s:%(module)s:%(message)s"
)

logging.basicConfig(format=FORMAT_STRING, datefmt="%H:%M:%S")

flat_formatter = logging.Formatter(FORMAT_STRING, "%H:%M:%S")

stderr_handler = logging.StreamHandler(stream=sys.stderr)
stderr_handler.setFormatter(flat_formatter)


def file_handler_factory(filename, dir=LOG_DIR, level=logging.INFO):
    if not os.path.exists(dir):
        os.makedirs(dir)
    handler = logging.FileHandler(os.path.join(dir, filename), mode="w")
    handler.setFormatter(flat_formatter)
    handler.setLevel(level)
    return handler


def get_logger(*args, **kwargs):
    logger = logging.getLogger(*args, **kwargs)
    return logger


# TODO(MB) Replace with logging module from caf.toolkit once released
def initialise_logger(name: str, log_file: pathlib.Path, reset_handlers: bool = True) -> None:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if reset_handlers:
        logger.handlers.clear()

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(logging.Formatter("[{levelname}] {message}", style="{"))
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter(
            "{asctime} [{name:>20.20}] [{levelname:^8.8}] {message}", style="{"
        )
    )
    logger.addHandler(file_handler)

    logger.debug("Initialised logger")
