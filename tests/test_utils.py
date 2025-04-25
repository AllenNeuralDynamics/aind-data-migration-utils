"""Tests for the utility functions in aind_data_migration_utils.utils"""

import unittest
import logging
from pathlib import Path
from aind_data_migration_utils.utils import setup_logger


class TestUtils(unittest.TestCase):
    """Test the utility functions"""

    def setUp(self):
        """Set up the test environment"""
        self.log_dir = Path("test_logs")
        self.output_path = Path("test_output")
        self.log_dir.mkdir(exist_ok=True)
        self.output_path.mkdir(exist_ok=True)

    def tearDown(self):
        """Clean up the test environment"""
        for log_file in self.log_dir.glob("*.log"):
            log_file.unlink()
        self.log_dir.rmdir()
        self.output_path.rmdir()

    def test_setup_logger_creates_log_file(self):
        """Test that setup_logger creates a log file"""
        setup_logger(self.log_dir)
        log_files = list(self.log_dir.glob("*.log"))
        self.assertTrue(len(log_files) > 0, "No log file created")
        self.assertTrue(log_files[0].name.startswith("log_"), "Log file name does not start with 'log_'")

    def test_logger_writes_to_log_file(self):
        """Test that the logger writes to the log file"""
        setup_logger(self.log_dir)
        logger = logging.getLogger()
        test_message = "This is a test log message"
        logger.info(test_message)

        log_files = list(self.log_dir.glob("*.log"))
        with open(log_files[0], "r") as log_file:
            log_content = log_file.read()
            self.assertIn(test_message, log_content, "Log message not found in log file")


if __name__ == "__main__":
    unittest.main()
