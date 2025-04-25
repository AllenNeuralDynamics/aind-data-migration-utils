"""Utility functions"""

import logging
from datetime import datetime
from pathlib import Path


def setup_logger(logfile_path: Path):
    """Setup logging for migration scripts"""

    # make the log directory if it's missing
    logfile_path.mkdir(parents=True, exist_ok=True)

    # set up logger with given file path
    log_file_name = logfile_path / ("log_" + datetime.now().strftime("%Y%m%d_%H%M%S") + ".log")
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Remove all handlers associated with the logger object
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        handler.close()

    # create file handler which logs even debug messages
    fh = logging.FileHandler(log_file_name)
    fh.setLevel(logging.DEBUG)

    # create console handler, can set the level to info or warning if desired
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create formatter and add it to the handlers
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)

    # add the handlers to logger
    logger.addHandler(ch)
    logger.addHandler(fh)
