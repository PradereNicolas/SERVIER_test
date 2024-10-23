# Dataplatform

The idea of this dataplatform is to provide a set of pipelines to enhance data from raw area to business area.

It is made from 4 areas:

- **RAW**:
  - _DATA_: Storage area for potentially multiple data sources with multiple data format
- **REFINED**:
  - _DATA_: Homogeneous data storage without altering input data schema
  - _JOBS_: Simple pipelines to convert data to the target type with light technical controls
- **OPTIMIZED**:
  - _DATA_: Company data model
  - _JOBS_: Retreive data from multiple sources in order to create an optimized company data model
- **BUSINESS**:
  - _DATA_: Potentially non normalized data tables
  - _JOBS_: Create functionnal tables thats aims to answer to specific business cases

All data tables (except for the raw area) also have a reject table associated that stores all the records that can not be inserted in functionnal tables because of quality controls (either technical controls or functionnal controls implemented in the jobs)

## Installation

To be able to run all following commands, please use a unix environment or a WSL in VS code.

This dataplatform uses poetry as dependency manager. In order to be able to run the main function, dependencies must by downladed by the following command:

```bash
pip install poetry
poetry shell
poetry install
```

These commands will automatically create the virtual environment and activate it.

Then, all the workflows can be executed by:

```bash
python3 main.py
```

## Tests

The tests can be launched with the following command:

```bash
PYTHONPATH=$(pwd) poetry run pytest
```

## Documentation

### Technical

The technical documentation can be automatically generated with the following commands:

```bash
cd documentation/technical/
sphinx-apidoc -o . ../../areas
sphinx-build -d _build/doctrees . _build/html
```

It creates some html page that describe the behavior of each module of the dataplatform in the _documentation/technical/\_build/htlm/_ folder. (Open index.html to get the root page)

### Functionnal

The functionnal documentation can be automatically generated with the following command (at root directory):

```bash
PYTHONPATH=$(pwd) python3 documentation/functional/schema.py
```

This script create a chart with the data model, which job populates which tables and where do they get data.
The chart can be found at _documentation/functional/schema.png_
