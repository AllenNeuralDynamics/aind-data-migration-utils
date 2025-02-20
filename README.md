# aind-data-migration-utils

[![License](https://img.shields.io/badge/license-MIT-brightgreen)](LICENSE)
![Code Style](https://img.shields.io/badge/code%20style-black-black)
[![semantic-release: angular](https://img.shields.io/badge/semantic--release-angular-e10079?logo=semantic-release)](https://github.com/semantic-release/semantic-release)
![Interrogate](https://img.shields.io/badge/interrogate-33.3%25-red)
![Coverage](https://img.shields.io/badge/coverage-100%25-brightgreen?logo=codecov)
![Python](https://img.shields.io/badge/python->=3.10-blue?logo=python)


## Usage
 - To use this template, click the green `Use this template` button and `Create new repository`.
 - After github initially creates the new repository, please wait an extra minute for the initialization scripts to finish organizing the repo.
 - To enable the automatic semantic version increments: in the repository go to `Settings` and `Collaborators and teams`. Click the green `Add people` button. Add `svc-aindscicomp` as an admin. Modify the file in `.github/workflows/tag_and_publish.yml` and remove the if statement in line 65. The semantic version will now be incremented every time a code is committed into the main branch.
 - To publish to PyPI, enable semantic versioning and uncomment the publish block in `.github/workflows/tag_and_publish.yml`. The code will now be published to PyPI every time the code is committed into the main branch.
 - The `.github/workflows/test_and_lint.yml` file will run automated tests and style checks every time a Pull Request is opened. If the checks are undesired, the `test_and_lint.yml` can be deleted. The strictness of the code coverage level, etc., can be modified by altering the configurations in the `pyproject.toml` file and the `.flake8` file.

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

    # For example, fix a subject_id
    record["subject"]["subject_id"] = "724910"

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
