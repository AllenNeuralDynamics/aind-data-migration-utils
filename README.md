# aind-data-migration-utils

[![License](https://img.shields.io/badge/license-MIT-brightgreen)](LICENSE)
![Code Style](https://img.shields.io/badge/code%20style-black-black)
[![semantic-release: angular](https://img.shields.io/badge/semantic--release-angular-e10079?logo=semantic-release)](https://github.com/semantic-release/semantic-release)
![Interrogate](https://img.shields.io/badge/interrogate-33.3%25-red)
![Coverage](https://img.shields.io/badge/coverage-100%25-brightgreen?logo=codecov)
![Python](https://img.shields.io/badge/python->=3.10-blue?logo=python)

## Installation

```bash
pip install aind-data-migration-utils
```

## Usage

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
        record["subject"]["subject_id"] = str(record["subject"]["subject_id"])

    return record


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--full-run", action=argparse.BooleanOptionalAction, required=False, default=False)
    parser.add_argument("--test", action=argparse.BooleanOptionalAction, required=False, default=False)
    args = parser.parse_args()

    migrator = Migrator(
        query=query,
        migration_callback=your_callback,
        files=["subject"],
    )
    migrator.run(full_run=args.full_run, test_mode=args.test)
```

Call your code to run the dry run

```bash
python run.py
```

Pass the `--full-run` argument to push changes to DocDB.
