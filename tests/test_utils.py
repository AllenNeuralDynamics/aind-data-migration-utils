import unittest
import logging
from pathlib import Path
from aind_data_migration_utils.utils import setup_logger, create_output_zip
import os
import zipfile


class TestUtils(unittest.TestCase):

    def setUp(self):
        self.log_dir = Path("test_logs")
        self.log_dir.mkdir(exist_ok=True)

    def tearDown(self):
        for log_file in self.log_dir.glob("*.log"):
            log_file.unlink()
        self.log_dir.rmdir()
        if os.path.exists("test_run.zip"):
            os.remove("test_run.zip")

    def test_setup_logger_creates_log_file(self):
        setup_logger(str(self.log_dir))
        log_files = list(self.log_dir.glob("*.log"))
        self.assertTrue(len(log_files) > 0, "No log file created")
        self.assertTrue(log_files[0].name.startswith("log_"), "Log file name does not start with 'log_'")

    def test_logger_writes_to_log_file(self):
        setup_logger(str(self.log_dir))
        logger = logging.getLogger()
        test_message = "This is a test log message"
        logger.info(test_message)

        log_files = list(self.log_dir.glob("*.log"))
        with open(log_files[0], 'r') as log_file:
            log_content = log_file.read()
            self.assertIn(test_message, log_content, "Log message not found in log file")

    def test_create_output_zip_creates_zip_file(self):
        setup_logger(str(self.log_dir))
        logger = logging.getLogger()
        logger.info("Test log message")

        zip_filename = create_output_zip("test", self.log_dir)
        self.assertTrue(os.path.exists(zip_filename), "Zip file was not created")

    def test_create_output_zip_contains_log_files(self):
        setup_logger(str(self.log_dir))
        logger = logging.getLogger()
        logger.info("Test log message")

        zip_filename = create_output_zip("test", self.log_dir)
        with zipfile.ZipFile(zip_filename, 'r') as zipf:
            log_files = [f for f in zipf.namelist() if f.endswith('.log')]
            self.assertTrue(len(log_files) > 0, "No log files found in zip archive")

if __name__ == '__main__':
    unittest.main()