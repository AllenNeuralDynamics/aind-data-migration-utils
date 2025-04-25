"""Test file for checking that hashes work properly"""
from aind_data_migration_utils.migrate import Migrator
from datetime import datetime
import argparse
import logging

dev_name = "ecephys_655019_2000-01-01_02-02-15"

query = {
    "name": dev_name,
}


def update_label(record):
    """Update the label of the record"""
    record["label"] = f"{datetime.now()}"
    logging.info(f"Updated {record['name']} label to {record['label']}")
    return record


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--full-run",
        action=argparse.BooleanOptionalAction,
        required=False,
        default=False,
    )
    parser.add_argument(
        "--test-mode",
        action=argparse.BooleanOptionalAction,
        required=False,
        default=False,
    )
    args = parser.parse_args()

    migrator = Migrator(
        query=query,
        migration_callback=update_label,
        files=[],
        test_mode=args.test_mode,
        prod=False,
    )

    migrator.run(full_run=args.full_run)
