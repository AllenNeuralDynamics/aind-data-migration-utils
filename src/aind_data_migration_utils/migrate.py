""" Migration script wrapper """
from pathlib import Path
from typing import List
import logging
import pandas as pd

from aind_data_access_api.document_db import MetadataDbClient
from aind_data_migration_utils.utils import setup_logger, create_output_zip


class Migrator():
    """ Migrator class """

    def __init__(self,
                 query: dict,
                 migration_callback: callable,
                 files: List[str] = [],
                 full_run=False,
                 prod: bool = True,
                 path="."):
        """Set up a migration script

        Parameters
        ----------
        query: dict
            MongoDB query to filter the records to migrate
        migration_callback : Callable
            Function that takes a metadata core file dict and returns the modified dict
        prod : bool, optional
            Whether to run in production mode, by default True
        path : str, optional
            Path to subfolder where output files will be stored, by default "."
        """

        self.output_dir = Path(path)
        self.log_dir = self.output_dir / "logs"
        setup_logger(self.log_dir)

        self.client = MetadataDbClient(
            host="api.allenneuraldynamics.org" if prod else "api.allenneuraldynamics-test.org",
            database="metadata_index",
            collection="data_assets",
        )

        self.query = query
        self.migration_callback = migration_callback

        self.files = files
        self.full_run = full_run

        self.dry_run_complete = False

        self.original_records = []
        self.results = []

    def run(self, full_run: bool = False):
        """ Run the migration """

        if full_run and not self.dry_run_complete:
            raise ValueError("Full run requested but dry run has not been completed.")

        logging.info(f"Starting migration with query: {self.query}")
        logging.info(f"This is a {'full' if full_run else 'dry'} run.")

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
            projection["_id"] = 1
        else:
            projection = None

        self.original_records = self.client.retrive_docdb_records(
            filter_query=self.query,
            projection=projection,
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

    def _teardown(self):
        """ Teardown the migration """

        zip_file = create_output_zip("full" if self.full_run else "dry", self.log_dir)

        df = pd.DataFrame(self.results)
        df.to_csv(self.output_dir / "results.csv", index=False)

        logging.info(f"Migration complete. Output zip file: {zip_file}")
