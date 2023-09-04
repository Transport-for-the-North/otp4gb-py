import logging
import os
import sys

from otp4gb.otp import Server, CheckUrl
from otp4gb.config import load_config


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


def main():
    try:
        opt_base_folder = os.path.abspath(sys.argv[1])
    except IndexError:
        logger.error("No path provided")

    # Load hostname & port from config
    config = load_config(opt_base_folder)

    # Start OTP Server
    server = Server(opt_base_folder, hostname=config.hostname, port=config.port)
    server.start()

    input("\n\nPress any key to stop server...\n\n")

    server.stop()


if __name__ == "__main__":
    main()
