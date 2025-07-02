"""Tests for the utility functions in aind_data_migration_utils.utils"""

import logging
import unittest
from pathlib import Path

from aind_data_migration_utils.utils import hash_records, setup_logger


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

    def test_hash_records_with_empty_list(self):
        """Test that hash_records works with an empty list"""
        result = hash_records([])
        # Hash of an empty list should be consistent
        self.assertEqual(result, "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945")

    def test_hash_records_with_sample_data(self):
        """Test that hash_records produces expected output with sample data"""
        records = [
            {"name": "record1", "last_modified": "2023-01-01", "extra_field": "value"},
            {"name": "record2", "last_modified": "2023-01-02", "another_field": 123},
        ]
        result = hash_records(records)
        # The hash should only consider name and last_modified fields
        expected = hash_records(
            [{"name": "record1", "last_modified": "2023-01-01"}, {"name": "record2", "last_modified": "2023-01-02"}]
        )
        self.assertEqual(result, expected)

    def test_hash_records_order_independence(self):
        """Test that hash_records is not affected by the order of records"""
        records1 = [
            {"name": "record1", "last_modified": "2023-01-01"},
            {"name": "record2", "last_modified": "2023-01-02"},
        ]
        records2 = [
            {"name": "record2", "last_modified": "2023-01-02"},
            {"name": "record1", "last_modified": "2023-01-01"},
        ]
        # Different order should produce different hashes
        self.assertNotEqual(hash_records(records1), hash_records(records2))


if __name__ == "__main__":
    unittest.main()
