# aind-data-migration-utils

[![License](https://img.shields.io/badge/license-MIT-brightgreen)](LICENSE)
![Code Style](https://img.shields.io/badge/code%20style-black-black)
[![semantic-release: angular](https://img.shields.io/badge/semantic--release-angular-e10079?logo=semantic-release)](https://github.com/semantic-release/semantic-release)
![Interrogate](https://img.shields.io/badge/interrogate-100.0%25-brightgreen)
![Coverage](https://img.shields.io/badge/coverage-100%25-brightgreen?logo=codecov)
![Python](https://img.shields.io/badge/python->=3.10-blue?logo=python)

## Installation

```bash
pip install aind-data-migration-utils
```

## Usage

To use the `Migrator` object, you need to create a DocDB query and a callback. The callback should take a full metadata record as input and return the same metadata record, with any modifications you need to make. Note that you will only have access to core metadata files that you specifically request using `Migrator(files: List[str])`.

There are two main arguments that control the `Migrator` class and how it runs:

- `Migrator(test_mode: bool)` controls whether or not to run the migrator over all records or just a single record. This is useful when you are running a large migration and want to modify just a single file in production.
- `.run(full_run: bool)` whether to actually modify records on the DocDB server

Running a dry run stores a hash that tracks what the dry run was completed on. You cannot run a full run until a hash for that dry run is completed.

The full process of running a migration is:

1. Define your query and callback, make sure to use logging to clearly explain what happened to each record and use the `files` parameter to limit your request to just the core files you are modifying.
2. Run you dry run, the hash file should get generated so that you can run your full run.
3. Open your PR and get confirmation that your code works properly.
4. Run your full run.
5. Merge the PR.

If your code modifies large numbers of records, split step 4 into three partial steps: (a) re-run the dry run with the `--test` flag to modify only a single record, (b) run the full run with the `--test` flag and check using `metadata-portal.allenneuraldynamics.org/view?name=<your-asset-name>` that the record was modified properly, (c) re-run the full dry and full runs.

## Example

```python
from aind_data_migration_utils.migrate import Migrator
import argparse
import logging

# Create a docdb query
query = {
    "_id": {"_id": "your-id-to-fix"}
}

def your_callback(record: dict) -> dict:
    """ Make changes to a record """

    # For example, convert a subject ID that wasn't a string to a string
    if not isinstance(record["subject"]["subject_id"], str):
        original_type = type(record["subject"]["subject_id"])
        record["subject"]["subject_id"] = str(record["subject"]["subject_id"])
        logging.info(f"Modified type of subject_id field for record {record["name"]} from {original_type} to str)")
    
    # Note: raising Exceptions inside a callback will log errors in the results.csv file

    return record


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--full-run", action=argparse.BooleanOptionalAction, required=False, default=False)
    parser.add_argument("--test", action=argparse.BooleanOptionalAction, required=False, default=False)
    args = parser.parse_args()

    migrator = Migrator(
        query=query,
        migration_callback=your_callback,
        test_mode=args.test,
        files=["subject"],
        prod=True,
    )
    migrator.run(full_run=args.full_run)
```

Call your code to run the dry run

```bash
python run.py
```

Pass the `--full-run` argument to push changes to DocDB.
