"""Tests for the Migrator class in the migrate module."""

import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock
from aind_data_migration_utils.migrate import Migrator
import json
from unittest.mock import patch, MagicMock, mock_open
from hashlib import sha256


class TestMigrator(unittest.TestCase):
    """Tests for the Migrator class"""

    @patch("aind_data_migration_utils.migrate.setup_logger")
    @patch("aind_data_migration_utils.migrate.MetadataDbClient")
    def test_migrator_initialization(self, MockMetadataDbClient, mock_setup_logger):
        """Test the initialization of the Migrator class"""
        query = {"field": "value"}
        migration_callback = MagicMock()
        files = ["file1", "file2"]
        prod = True
        path = "test_path"

        migrator = Migrator(query=query, migration_callback=migration_callback, files=files, prod=prod, path=path)

        self.assertEqual(migrator.query, query)
        self.assertEqual(migrator.migration_callback, migration_callback)
        self.assertEqual(migrator.files, files)
        self.assertTrue(migrator.dry_run_complete is False)
        self.assertEqual(migrator.original_records, [])
        self.assertEqual(migrator.results, [])
        self.assertEqual(migrator.output_dir, Path(path))
        self.assertTrue(migrator.output_dir.exists())
        self.assertEqual(migrator.log_dir, migrator.output_dir / "logs")
        mock_setup_logger.assert_called_once_with(migrator.log_dir)
        MockMetadataDbClient.assert_called_once_with(
            host="api.allenneuraldynamics.org", database="metadata_index", collection="data_assets"
        )

    @patch("aind_data_migration_utils.migrate.setup_logger")
    @patch("aind_data_migration_utils.migrate.MetadataDbClient")
    def test_migrator_initialization_non_prod(self, MockMetadataDbClient, mock_setup_logger):
        """Test the initialization of the Migrator class in non-prod mode"""
        query = {"field": "value"}
        migration_callback = MagicMock()
        files = ["file1", "file2"]
        prod = False
        path = "test_path"

        migrator = Migrator(query, migration_callback, files, prod, path=path)

        self.assertEqual(migrator.query, query)
        self.assertEqual(migrator.migration_callback, migration_callback)
        self.assertEqual(migrator.files, files)
        self.assertTrue(migrator.dry_run_complete is False)
        self.assertEqual(migrator.original_records, [])
        self.assertEqual(migrator.results, [])
        self.assertEqual(migrator.output_dir, Path(path))
        self.assertTrue(migrator.output_dir.exists())
        self.assertEqual(migrator.log_dir, migrator.output_dir / "logs")
        mock_setup_logger.assert_called_once_with(migrator.log_dir)
        MockMetadataDbClient.assert_called_once_with(
            host="api.allenneuraldynamics-test.org", database="test", collection="data_assets"
        )

    @patch("aind_data_migration_utils.migrate.setup_logger")
    @patch("aind_data_migration_utils.migrate.MetadataDbClient")
    def test_run_dry_run(self, MockMetadataDbClient, mock_setup_logger):
        """Test the run method with a dry run"""
        query = {"field": "value"}
        migration_callback = MagicMock()
        migrator = Migrator(query, migration_callback, prod=True, path="test_path", test_mode=True)

        migrator._setup = MagicMock()
        migrator._migrate = MagicMock()
        migrator._upsert = MagicMock()
        migrator._teardown = MagicMock()

        migrator.run(full_run=False)

        migrator._setup.assert_called_once()
        migrator._migrate.assert_called_once()
        migrator._upsert.assert_called_once()
        migrator._teardown.assert_called_once()

    @patch("aind_data_migration_utils.migrate.setup_logger")
    @patch("aind_data_migration_utils.migrate.MetadataDbClient")
    def test_run_full_run_without_dry_run(self, MockMetadataDbClient, mock_setup_logger):
        """Test the run method with a full run without a dry run"""
        query = {"field": "value"}
        migration_callback = MagicMock()
        migrator = Migrator(query, migration_callback, prod=True, path="test_path", test_mode=False)

        with self.assertRaises(ValueError):
            migrator.run(full_run=True)

    @patch("aind_data_migration_utils.migrate.setup_logger")
    @patch("aind_data_migration_utils.migrate.MetadataDbClient")
    def test_revert(self, MockMetadataDbClient, mock_setup_logger):
        """Test the revert method"""
        query = {"field": "value"}
        migration_callback = MagicMock()
        migrator = Migrator(query, migration_callback, prod=True, path="test_path")

        migrator.original_records = [{"_id": "123"}]
        migrator.client.upsert_one_docdb_record = MagicMock()

        migrator.revert()

        migrator.client.upsert_one_docdb_record.assert_called_once_with({"_id": "123"})

    @patch("aind_data_migration_utils.migrate.setup_logger")
    @patch("aind_data_migration_utils.migrate.MetadataDbClient")
    def test_revert_no_original_records(self, MockMetadataDbClient, mock_setup_logger):
        """Test the revert method with no original records"""
        query = {"field": "value"}
        migration_callback = MagicMock()
        migrator = Migrator(query, migration_callback, prod=True, path="test_path")

        with self.assertRaises(ValueError):
            migrator.revert()

    @patch("aind_data_migration_utils.migrate.setup_logger")
    @patch("aind_data_migration_utils.migrate.MetadataDbClient")
    def test_setup_with_files(self, MockMetadataDbClient, mock_setup_logger):
        """Test the _setup method with files"""
        query = {"field": "value"}
        migration_callback = MagicMock()
        files = ["file1", "file2"]
        migrator = Migrator(query, migration_callback, files, prod=True, path="test_path")
        migrator.test_mode = False

        migrator.client.retrieve_docdb_records = MagicMock(return_value=[{"_id": "123"}])

        migrator._setup()

        migrator.client.retrieve_docdb_records.assert_called_once_with(
            filter_query=query, projection={"file1": 1, "file2": 1, "_id": 1, "name": 1, "location": 1}, limit=0
        )

    @patch("aind_data_migration_utils.migrate.setup_logger")
    @patch("aind_data_migration_utils.migrate.MetadataDbClient")
    def test_setup_without_files(self, MockMetadataDbClient, mock_setup_logger):
        """Test the _setup method without files"""
        query = {"field": "value"}
        migration_callback = MagicMock()
        migrator = Migrator(query, migration_callback, prod=True, path="test_path")
        migrator.test_mode = False

        migrator.client.retrieve_docdb_records = MagicMock(return_value=[{"_id": "123"}])

        migrator._setup()

        migrator.client.retrieve_docdb_records.assert_called_once_with(filter_query=query, projection=None, limit=0)

    @patch("aind_data_migration_utils.migrate.setup_logger")
    @patch("aind_data_migration_utils.migrate.MetadataDbClient")
    def test_migrate(self, MockMetadataDbClient, mock_setup_logger):
        """Test the _migrate method"""
        query = {"field": "value"}
        migration_callback = MagicMock(return_value={"_id": "123", "name": "new_name"})
        migrator = Migrator(query, migration_callback, prod=True, path="test_path")

        migrator.original_records = [{"_id": "123", "name": "old_name"}]

        migrator._migrate()

        self.assertEqual(migrator.migrated_records, [{"_id": "123", "name": "new_name"}])

    @patch("aind_data_migration_utils.migrate.setup_logger")
    @patch("aind_data_migration_utils.migrate.MetadataDbClient")
    def test_migrate_with_error(self, MockMetadataDbClient, mock_setup_logger):
        """Test the _migrate method with an error"""
        query = {"field": "value"}
        migration_callback = MagicMock(side_effect=Exception("Migration error"))
        migrator = Migrator(query, migration_callback, prod=True, path="test_path")

        migrator.original_records = [{"_id": "123", "name": "old_name"}]

        migrator._migrate()

        self.assertEqual(migrator.results, [{"_id": "123", "status": "failed", "notes": "Migration error"}])

    @patch("aind_data_migration_utils.migrate.setup_logger")
    @patch("aind_data_migration_utils.migrate.MetadataDbClient")
    def test_upsert_full_run(self, MockMetadataDbClient, mock_setup_logger):
        """Test the _upsert method with a full run"""
        query = {"field": "value"}
        migration_callback = MagicMock()
        migrator = Migrator(query, migration_callback, prod=True, path="test_path")

        migrator.migrated_records = [{"_id": "123", "name": "new_name"}]
        migrator.full_run = True
        migrator.client.upsert_one_docdb_record = MagicMock(return_value=MagicMock(status_code=200))

        migrator._upsert()

        migrator.client.upsert_one_docdb_record.assert_called_once_with({"_id": "123", "name": "new_name"})
        self.assertEqual(migrator.results, [{"_id": "123", "status": "success", "notes": ""}])

    @patch("aind_data_migration_utils.migrate.setup_logger")
    @patch("aind_data_migration_utils.migrate.MetadataDbClient")
    def test_upsert_dry_run(self, MockMetadataDbClient, mock_setup_logger):
        """Test the _upsert method with a dry run"""
        query = {"field": "value"}
        migration_callback = MagicMock()
        migrator = Migrator(query, migration_callback, prod=True, path="test_path")

        migrator.migrated_records = [{"_id": "123", "name": "new_name"}]
        migrator.full_run = False

        migrator._upsert()

        self.assertEqual(migrator.results, [{"_id": "123", "status": "dry_run", "notes": ""}])

    @patch("aind_data_migration_utils.migrate.setup_logger")
    @patch("aind_data_migration_utils.migrate.MetadataDbClient")
    def test_teardown(self, MockMetadataDbClient, mock_setup_logger):
        """Test the _teardown method"""
        query = {"field": "value"}
        migration_callback = MagicMock()
        migrator = Migrator(query, migration_callback, prod=True, path="test_path")

        migrator.results = [{"_id": "123", "status": "success", "notes": ""}]
        migrator.full_run = True
        migrator.log_dir = Path("test_path/logs")
        migrator.output_dir = Path("test_path")

        with patch("pandas.DataFrame.to_csv") as mock_to_csv:
            migrator._teardown()

            mock_to_csv.assert_called_once_with(migrator.output_dir / "results.csv", index=False)

    @patch("aind_data_migration_utils.migrate.setup_logger")
    @patch("aind_data_migration_utils.migrate.MetadataDbClient")
    def test_upsert_full_run_with_error(self, MockMetadataDbClient, mock_setup_logger):
        """Test the _upsert method with an error"""
        query = {"field": "value"}
        migration_callback = MagicMock()
        migrator = Migrator(query, migration_callback, prod=True, path="test_path")

        migrator.migrated_records = [{"_id": "123", "name": "new_name"}]
        migrator.full_run = True
        mock_response = MagicMock(status_code=500, text="Internal Server Error")
        migrator.client.upsert_one_docdb_record = MagicMock(return_value=mock_response)

        migrator._upsert()

        migrator.client.upsert_one_docdb_record.assert_called_once_with({"_id": "123", "name": "new_name"})
        self.assertEqual(migrator.results, [{"_id": "123", "status": "failed", "notes": "Internal Server Error"}])

    @patch("aind_data_migration_utils.migrate.setup_logger")
    @patch("aind_data_migration_utils.migrate.MetadataDbClient")
    def test_upsert_dry_run_with_multiple_records(self, MockMetadataDbClient, mock_setup_logger):
        """Test the _upsert method with multiple records"""
        query = {"field": "value"}
        migration_callback = MagicMock()
        migrator = Migrator(query, migration_callback, prod=True, path="test_path")

        migrator.migrated_records = [{"_id": "123", "name": "new_name1"}, {"_id": "456", "name": "new_name2"}]
        migrator.full_run = False

        migrator._upsert()

        self.assertEqual(
            migrator.results,
            [{"_id": "123", "status": "dry_run", "notes": ""}, {"_id": "456", "status": "dry_run", "notes": ""}],
        )

    @patch("aind_data_migration_utils.migrate.setup_logger")
    @patch("aind_data_migration_utils.migrate.MetadataDbClient")
    def test_teardown_full_run(self, MockMetadataDbClient, mock_setup_logger):
        """Test the _teardown method with a full"""
        query = {"field": "value"}
        migration_callback = MagicMock()
        migrator = Migrator(query, migration_callback, prod=True, path="test_path")

        migrator.results = [
            {"_id": "123", "status": "success", "notes": ""},
            {"_id": "456", "status": "failed", "notes": "Error"},
        ]
        migrator.full_run = True
        migrator.log_dir = Path("test_path/logs")
        migrator.output_dir = Path("test_path")

        with patch("pandas.DataFrame.to_csv") as mock_to_csv:
            migrator._teardown()

            mock_to_csv.assert_called_once_with(migrator.output_dir / "results.csv", index=False)
            self.assertTrue(migrator.dry_run_complete is False)

    @patch("aind_data_migration_utils.migrate.setup_logger")
    @patch("aind_data_migration_utils.migrate.MetadataDbClient")
    def test_teardown_dry_run(self, MockMetadataDbClient, mock_setup_logger):
        """Test the _teardown method with a dry run"""
        query = {"field": "value"}
        migration_callback = MagicMock()
        migrator = Migrator(query, migration_callback, prod=True, path="test_path")

        migrator.results = [
            {"_id": "123", "status": "dry_run", "notes": ""},
            {"_id": "456", "status": "dry_run", "notes": ""},
        ]
        migrator.full_run = False
        migrator.log_dir = Path("test_path/logs")
        migrator.output_dir = Path("test_path")

        with patch("pandas.DataFrame.to_csv") as mock_to_csv:
            migrator._teardown()

            mock_to_csv.assert_called_once_with(migrator.output_dir / "results.csv", index=False)
            self.assertTrue(migrator.dry_run_complete is True)

    @patch("aind_data_migration_utils.migrate.setup_logger")
    @patch("aind_data_migration_utils.migrate.MetadataDbClient")
    def test_dry_file_path(self, MockMetadataDbClient, mock_setup_logger):
        """Test the _dry_file_path method"""
        query = {"field": "value"}
        migration_callback = MagicMock()
        migrator = Migrator(query, migration_callback, prod=True, path="test_path")

        path = migrator._dry_file_path()

        self.assertEqual(path, Path("test_path") / "dry_run_hash.txt")

    @patch("aind_data_migration_utils.migrate.setup_logger")
    @patch("aind_data_migration_utils.migrate.MetadataDbClient")
    @patch("aind_data_migration_utils.migrate.hash_records")
    def test_hash(self, mock_hash_records, MockMetadataDbClient, mock_setup_logger):
        """Test the _hash method"""
        query = {"field": "value"}
        migration_callback = MagicMock()
        migrator = Migrator(query, migration_callback, prod=True, path="test_path")
        migrator.original_records = [{"_id": "123"}]
        mock_hash_records.return_value = "hash123"

        result = migrator._hash()

        mock_hash_records.assert_called_once_with(migrator.original_records)
        self.assertEqual(result, "hash123")

    @patch("aind_data_migration_utils.migrate.setup_logger")
    @patch("aind_data_migration_utils.migrate.MetadataDbClient")
    @patch("pathlib.Path.exists")
    @patch("builtins.open", new_callable=mock_open, read_data="hash123")
    def test_read_dry_file_exists_and_matches(self, mock_file, mock_exists, MockMetadataDbClient, mock_setup_logger):
        """Test the _read_dry_file method when file exists and hashes match"""
        query = {"field": "value"}
        migration_callback = MagicMock()
        migrator = Migrator(query, migration_callback, prod=True, path="test_path")
        mock_exists.return_value = True
        migrator._hash = MagicMock(return_value="hash123")

        result = migrator._read_dry_file()

        self.assertTrue(result)
        mock_file.assert_called_once_with(migrator._dry_file_path(), "r")

    @patch("aind_data_migration_utils.migrate.setup_logger")
    @patch("aind_data_migration_utils.migrate.MetadataDbClient")
    @patch("pathlib.Path.exists")
    @patch("builtins.open", new_callable=mock_open, read_data="hash456")
    def test_read_dry_file_exists_but_does_not_match(
        self, mock_file, mock_exists, MockMetadataDbClient, mock_setup_logger
    ):
        """Test the _read_dry_file method when file exists but hashes don't match"""
        query = {"field": "value"}
        migration_callback = MagicMock()
        migrator = Migrator(query, migration_callback, prod=True, path="test_path")
        mock_exists.return_value = True
        migrator._hash = MagicMock(return_value="hash123")

        result = migrator._read_dry_file()

        self.assertFalse(result)
        mock_file.assert_called_once_with(migrator._dry_file_path(), "r")

    @patch("aind_data_migration_utils.migrate.setup_logger")
    @patch("aind_data_migration_utils.migrate.MetadataDbClient")
    @patch("pathlib.Path.exists")
    @patch("builtins.print")
    def test_read_dry_file_does_not_exist(self, mock_print, mock_exists, MockMetadataDbClient, mock_setup_logger):
        """Test the _read_dry_file method when file doesn't exist"""
        query = {"field": "value"}
        migration_callback = MagicMock()
        migrator = Migrator(query, migration_callback, prod=True, path="test_path")
        mock_exists.return_value = False

        result = migrator._read_dry_file()

        self.assertFalse(result)
        mock_print.assert_called_once()

    @patch("aind_data_migration_utils.migrate.setup_logger")
    @patch("aind_data_migration_utils.migrate.MetadataDbClient")
    @patch("builtins.open", new_callable=mock_open)
    @patch("logging.info")
    def test_write_dry_file(self, mock_log_info, mock_file, MockMetadataDbClient, mock_setup_logger):
        """Test the _write_dry_file method"""
        query = {"field": "value"}
        migration_callback = MagicMock()
        migrator = Migrator(query, migration_callback, prod=True, path="test_path")
        migrator._hash = MagicMock(return_value="hash123")

        migrator._write_dry_file()

        mock_file.assert_called_once_with(migrator._dry_file_path(), "w")
        mock_file().write.assert_called_once_with("hash123")
        mock_log_info.assert_called_once()
