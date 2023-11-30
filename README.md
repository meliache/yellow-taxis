# Yellow Taxis

![PyTest](https://github.com/meliache/yellow-taxis/actions/workflows/pytest.yml/badge.svg)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/release/python-311/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![pdm-managed](https://img.shields.io/badge/pdm-managed-blueviolet)](https://pdm-project.org)

Analysis of average travel times of _Yellow Taxis_ travel times in New York City.

Data source: [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## TODO's

- [X] Please write a Python program that calculates the average trip length of all Yellow Taxis for a month.
- [ ] Extend this to a data pipeline that can ingest new data and calculates the 45 day rolling average trip length.
- [ ] Make sure your program could be run in a production setup.

## Installation

First clone the repository and enter the root directory:

``` shell
git clone https://github.com/meliache/yellow-taxis
```

Create a virtual environment with Python 3.11 and then install the project using `pip` or [pdm](https://github.com/pdm-project/pdm), the package manager used for development of this project:

``` shell
pip install --editable .
pdm install # alternative
```

This is sufficient for running the pipeline.

### Developer install

Additionally this project has optional dependencies for developing, testing and for using Jupyter:

``` shell
pip install --editable '.[dev,test,jupyter]' # choose what you need
pip install --editable '.[complete]' # install all
pdm install --dev --group dev --group test --group jupyter # alternative
```

Further, if you want to contribute, please install [pre-commit](https://pre-commit.com/):

``` shell
pre-commit install
```

But to be less bothered by pre-commit's errors I recommend setting up your editor/IDE to auto-format the project with [ruff](https://docs.astral.sh/ruff/formatter/), [isort](https://pycqa.github.io/isort/) and [docformatter](https://docformatter.readthedocs.io/en/latest).

### Running the pipeline

TODO

### Non-Python dependencies

- [curl](https://curl.se): The `curl` commandline-tool is currently used for downloading the datasets. It be available on all desktop operating systems and should pre-installed on Windows and Mac, but if not it's available for almost any architecture.

## Author

[Michael Eliachevitch](mailto:m.eliachevitch@posteo.de "Email-Address")

<!-- Local Variables: -->
<!-- mode: gfm -->
<!-- End: -->
