"""Migration script wrapper"""

import logging
from pathlib import Path
from typing import Callable, List

import pandas as pd
import requests
from aind_data_access_api.document_db import MetadataDbClient

from aind_data_migration_utils.utils import hash_records, setup_logger

ALWAYS_KEEP_FIELDS = ["name", "location"]


class Migrator:
    """Migrator class"""

    def __init__(
        self,
        query: dict,
        migration_callback: Callable,
        files: List[str] = [],
        prod: bool = True,
        test_mode: bool = False,
        path=".",
    ):
        """Set up a migration script

        Parameters
        ----------
        query: dict
            MongoDB query to filter the records to migrate
        migration_callback : Callable
            Function that takes a metadata core file dict and returns the modified dict
        files : List[str], optional
            List of metadata files to include in the migration, by default all files
        prod : bool, optional
            Whether to run in the production docdb, by default True
        path : str, optional
            Path to subfolder where output files will be stored, by default "."
        """

        self.output_dir = Path(path)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir = self.output_dir / "logs"
        setup_logger(self.log_dir)

        self.test_mode = test_mode

        self.prod = prod

        self.query = query
        self.migration_callback = migration_callback

        self.files = files

        self.dry_run_complete = False

        self.original_records = []
        self.results = []

        # Initialize the client
        self._check_and_establish_client()

    def _check_and_establish_client(self):
        """Check and establish database client connection if needed"""
        # Test existing connection with a simple query, recreate if it fails
        if hasattr(self, "client") and self.client is not None:
            try:
                self.client.retrieve_docdb_records(filter_query={"_id": "test"}, limit=1)
                return  # Connection is good
            except requests.exceptions.RequestException:
                pass  # Connection failed, will recreate below

        # Create new client connection
        self.client = MetadataDbClient(
            host=("api.allenneuraldynamics.org" if self.prod else "api.allenneuraldynamics-test.org"),
            database="metadata_index",
            collection="data_assets",
        )

    def run(self, full_run: bool = False):
        """Run the migration"""

        self._setup()
        self.full_run = full_run

        if full_run:
            self.dry_run_complete = self._read_dry_file()

            if not self.dry_run_complete:
                logging.error("Dry run not completed. Cannot proceed with full run.")
                raise ValueError("Full run requested but dry run has not been completed.")
            logging.info(f"Confirmed dry run is complete by comparing hash file: {self.dry_run_complete}")

        logging.info(f"Starting migration with query: {self.query}")
        logging.info(f"This is a {'full' if full_run else 'dry'} run.")
        logging.info(f"Pushing migration to {self.client.host}")

        self._migrate()
        self._upsert()
        self._teardown()

    def revert(self):
        """Revert a migration"""

        if not self.original_records:
            raise ValueError("No original records to revert to.")

        # Ensure client connection is active
        self._check_and_establish_client()

        for record in self.original_records:
            logging.info(f"Reverting record {record['name']}")

            self.client.upsert_one_docdb_record(record)

    def _setup(self):
        """Setup the migration"""

        if self.files:
            projection = {file: 1 for file in self.files}
            for field in ALWAYS_KEEP_FIELDS:
                projection[field] = 1
        else:
            projection = None

        self.original_records = self.client.retrieve_docdb_records(
            filter_query=self.query,
            projection=projection,
            limit=1 if self.test_mode else 0,
        )

        logging.info(f"Retrieved {len(self.original_records)} records")

    def _migrate(self):
        """Migrate the data"""

        self.migrated_records = []

        for record in self.original_records:
            try:
                self.migrated_records.append(self.migration_callback(record))
            except Exception as e:
                logging.error(f"Error migrating record {record['name']}: {e}")
                self.results.append(
                    {
                        "name": record["name"],
                        "status": "failed",
                        "notes": str(e),
                    }
                )

    def _upsert(self):
        """Upsert the data"""

        # Ensure client connection is active before upserting
        if self.full_run:
            self._check_and_establish_client()

        for record in self.migrated_records:

            if self.full_run:
                response = self.client.upsert_one_docdb_record(record)

                if response.status_code == 200:
                    logging.info(f"Record {record['name']} migrated successfully")
                    self.results.append(
                        {
                            "name": record["name"],
                            "status": "success",
                            "notes": "",
                        }
                    )
                else:
                    logging.info(f"Record {record['name']} upsert error: {response.text}")
                    self.results.append(
                        {
                            "name": record["name"],
                            "status": "failed",
                            "notes": response.text,
                        }
                    )
            else:
                logging.info(f"Dry run: Record {record['name']} would be migrated")
                self.results.append(
                    {
                        "name": record["name"],
                        "status": "dry_run",
                        "notes": "",
                    }
                )

    def _teardown(self):  # pragma: no cover
        """Teardown the migration"""

        if self.full_run:
            logging.info(
                f"Migration succeeded for {len([r for r in self.results if r['status'] == 'success'])} records"
            )
            logging.info(f"Migration failed for {len([r for r in self.results if r['status'] == 'failed'])} records")
        else:
            logging.info("Dry run complete.")
            self.dry_run_complete = True
            self._write_dry_file()

        df = pd.DataFrame(self.results)
        df.to_csv(self.output_dir / "results.csv", index=False)

        logging.info(f"Migration complete. Results saved to {self.output_dir}")

    def _dry_file_path(self):
        """Get the path to the dry run file"""
        return self.output_dir / "dry_run_hash.txt"

    def _hash(self):
        """Hash the records"""
        return hash_records(self.original_records)

    def _read_dry_file(self):
        """Read the dry run file to check if the dry run has been completed"""
        dry_file = self._dry_file_path()
        logging.info(f"Reading dry run file {dry_file}")

        if not dry_file.exists():
            logging.info(f"Dry run file {dry_file} does not exist.")
            return False

        with open(dry_file, "r") as f:
            hash_lines = f.read().strip().split("\n")

        current_hash = self._hash()
        logging.info(f"Hash data read from dry run file: {hash_lines}")
        logging.info(f"Hash data for current run: {current_hash}")
        return current_hash in hash_lines

    def _write_dry_file(self):
        """Write a hashed file indicating that the dry run has been completed"""
        dry_file = self._dry_file_path()

        hash_data = self._hash()

        with open(dry_file, "a") as f:
            f.write(str(hash_data) + "\n")
        logging.info(f"Hash data for dry run appended to {dry_file}")
