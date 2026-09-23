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

There are three main arguments that control the `Migrator` class and how it runs:

- `Migrator(test_mode: bool)` controls whether or not to run the migrator over all records or just a single record. This is useful when you are running a large migration and want to modify just a single file in production.
- `Migrator(version: str)` controls which DocDB database the migrator reads and writes: `"v1"` (the default) or `"v2"`. See [Choosing a database version](#choosing-a-database-version-v1-vs-v2) below.
- `.run(full_run: bool)` whether to actually modify records on the DocDB server

Running a dry run stores a hash that tracks what the dry run was completed on. You cannot run a full run until a hash for that dry run is completed.

The full process of running a migration is:

1. Define your query and callback, make sure to use logging to clearly explain what happened to each record and use the `files` parameter to limit your request to just the core files you are modifying.
2. Run you dry run, the hash file should get generated so that you can run your full run.
3. Open your PR and get confirmation that your code works properly.
4. Run your full run.
5. Merge the PR.

If your code modifies large numbers of records, split step 4 into three partial steps: (a) re-run the dry run with the `--test` flag to modify only a single record, (b) run the full run with the `--test` flag and check using `metadata-portal.allenneuraldynamics.org/view?name=<your-asset-name>` that the record was modified properly, (c) re-run the full dry and full runs.

## Choosing a database version (v1 vs v2)

The `version` argument selects which DocDB database the migrator targets. `"v1"` and `"v2"` are separate databases: the same asset has different `_id`s in each (if it exists in both at all), and the records follow different schemas (v1 holds legacy aind-data-schema `<2.0` records; v2 holds aind-data-schema `>=2.0` records).

Which one to target depends on where the record lives:

- **Legacy records (exist in v1):** target `version="v1"`. The v1 -> v2 sync in [aind-metadata-upgrader](https://github.com/AllenNeuralDynamics/aind-metadata-upgrader) regenerates a record's v2 copy from v1 whenever the v1 record changes or a new upgrader version is released, so v1 is the source of truth: a fix applied only to the v2 copy will be silently overwritten by the next sync. If the fix needs to appear in v2 immediately (rather than after the next sync), you can additionally apply it to the v2 record, using identical content so the eventual re-sync has no effect.
- **Records created after the v2 transition (exist only in v2):** target `version="v2"`. The sync only iterates v1 records and will never touch these.

When in doubt, query both databases for your target records before writing the migration to confirm where they exist.

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
        version="v1",  # target the v1 database ("v2" for post-transition records), see README
    )
    migrator.run(full_run=args.full_run)
```

Call your code to run the dry run. You can run multiple dry runs as needed.

```bash
python run.py
```

After completing a dry run for your specific query, pass the `--full-run` argument to push changes to DocDB.
