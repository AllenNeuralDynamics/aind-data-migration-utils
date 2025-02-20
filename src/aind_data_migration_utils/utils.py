import logging
from datetime import datetime
import zipfile
import os
from pathlib import Path


def setup_logger(logfile_path: Path):
    """Setup logging for migration scripts"""
    
    # make the log directory if it's missing
    logfile_path.mkdir(parents=True, exist_ok=True)

    # set up logger with given file path
    log_file_name = logfile_path / ("log_" + datetime.now().strftime("%Y%m%d_%H%M%S") + ".log")
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # create file handler which logs even debug messages
    fh = logging.FileHandler(log_file_name)
    fh.setLevel(logging.DEBUG)

    # create console handler, can set the level to info or warning if desired
    # You can remove the console handler if you don't want to see these messages in the
    # notebook.
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)

    # add the handlers to logger
    logger.addHandler(ch)
    logger.addHandler(fh)


def create_output_zip(run_type: str, log_dir: Path) -> str:
    """Create a zip file containing logs and failed records CSV, then clean up unzipped files"""
    zip_filename = f"{run_type.lower()}_run.zip"

    # Get all log files in the log directory
    log_files = [f for f in os.listdir(log_dir) if f.endswith('.log')]

    with zipfile.ZipFile(zip_filename, 'w') as zipf:
        # Add log files
        for log_file in log_files:
            zipf.write(log_dir / log_file, log_file)

    return zip_filename
