""" Migration script wrapper """
from pathlib import Path
from typing import List, Callable
import logging
import pandas as pd

from aind_data_access_api.document_db import MetadataDbClient
from aind_data_migration_utils.utils import setup_logger, create_output_zip

ALWAYS_KEEP_FIELDS = ["_id", "name", "location"]


class Migrator():
    """ Migrator class """

    def __init__(self,
                 query: dict,
                 migration_callback: Callable,
                 files: List[str] = [],
                 prod: bool = True,
                 path="."):
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

        self.prod = prod
        self.client = MetadataDbClient(
            host="api.allenneuraldynamics.org" if prod else "api.allenneuraldynamics-test.org",
            database="metadata_index" if self.prod else "test",
            collection="data_assets",
        )

        self.query = query
        self.migration_callback = migration_callback

        self.files = files

        self.dry_run_complete = False

        self.original_records = []
        self.results = []

    def run(self, full_run: bool = False, test_mode: bool = False):
        """ Run the migration """

        self.full_run = full_run
        self.test_mode = test_mode
        if full_run and not self.dry_run_complete:
            raise ValueError("Full run requested but dry run has not been completed.")

        logging.info(f"Starting migration with query: {self.query}")
        logging.info(f"This is a {'full' if full_run else 'dry'} run.")
        logging.info(f"Pushing migration to {self.client.host}")

        self._setup()
        self._migrate()
        self._upsert()
        self._teardown()

    def revert(self):
        """ Revert a migration """

        if not self.original_records:
            raise ValueError("No original records to revert to.")

        for record in self.original_records:
            logging.info(f"Reverting record {record['_id']}")

            self.client.upsert_one_docdb_record(
                record
            )

    def _setup(self):
        """ Setup the migration """

        if self.files:
            projection = {
                file: 1 for file in self.files
            }
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
        """ Migrate the data """

        self.migrated_records = []

        for record in self.original_records:
            try:
                self.migrated_records.append(
                    self.migration_callback(record)
                )
            except Exception as e:
                logging.error(f"Error migrating record {record['_id']}: {e}")
                self.results.append({
                    "_id": record["_id"],
                    "status": "failed",
                    "notes": str(e),
                })

    def _upsert(self):
        """ Upsert the data """

        for record in self.migrated_records:

            if self.full_run:
                response = self.client.upsert_one_docdb_record(
                    record
                )

                if response.status_code == 200:
                    logging.info(f"Record {record['_id']} migrated successfully")
                    self.results.append({
                        "_id": record["_id"],
                        "status": "success",
                        "notes": "",
                    })
                else:
                    logging.info(f"Record {record['_id']} upsert error: {response.text}")
                    self.results.append({
                        "_id": record["_id"],
                        "status": "failed",
                        "notes": response.text,
                    })
            else:
                logging.info(f"Dry run: Record {record['_id']} would be migrated")
                self.results.append({
                    "_id": record["_id"],
                    "status": "dry_run",
                    "notes": "",
                })

    def _teardown(self):
        """ Teardown the migration """

        zip_file = create_output_zip("full" if self.full_run else "dry", self.log_dir, self.output_dir)

        logging.info(f"Migration succeeded for {len([r for r in self.results if r['status'] == 'success'])} records")
        logging.info(f"Migration failed for {len([r for r in self.results if r['status'] == 'failed'])} records")

        df = pd.DataFrame(self.results)
        df.to_csv(self.output_dir / "results.csv", index=False)

        logging.info(f"Migration complete. Output zip file: {zip_file}")
