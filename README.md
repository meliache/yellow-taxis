# Yellow Taxis

![Pytest](https://github.com/meliache/yellow-taxis/actions/workflows/pytest.yml/badge.svg)
[![Codecov](https://codecov.io/gh/meliache/yellow-taxis/graph/badge.svg?token=QB6OA6CPVT)](https://codecov.io/gh/meliache/yellow-taxis)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/release/python-311/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![pdm-managed](https://img.shields.io/badge/pdm-managed-blueviolet)](https://pdm-project.org)

Analysis of average travel times of _Yellow Taxis_ in New York City.

Data source: [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## Tasks

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

### Non-Python dependencies

- [curl](https://curl.se): The `curl` commandline-tool is currently used for downloading the datasets. It is available for all desktop operating systems and comes pre-installed on Windows and MacOS.

### Developer installation

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

For creating the pipeline I have used [Luigi](https://github.com/spotify/luigi), which I have been familiar with from my academic work. I consciously avoided using [b2luigi](https://github.com/nils-braun/b2luigi), a Luigi helper package for working with batch systems as are common in Physics, because it's a bit niche and plain Luigi might be more familiar, plus the batch systems used at a company are likely to be very different than in physics and would first need to be adapted for b2luigi.

In short, Luigi defines workflows using classes implementing `luigi.Task`. Completeness is defined by the `Task.output()` method and requirements (other tasks) by the `Task.requires()` method. The actual work is done in the `Task.run()` method. Refer to the [Luigi docs](https://luigi.readthedocs.io/en/stable/index.html) for more information.

#### Local pipeline

The tasks defining the pipeline are found in [`src/yellow_taxis/tasks/`](https://github.com/meliache/yellow-taxis/tree/main/src/yellow_taxis/tasks). You can custom trigger individual tasks by using the `luigi` (or `pdm run luigi`) command line tool, which will also run all their dependencies. This method allows setting the task parameters, scheduler and number of workers on the command line


``` shell
# get average for a single month
luigi --module yellow_taxis.tasks.averaging_tasks MonthlyAveragesTask \
  --result-dir /path/to/results --year 2023 --month 8 --local-scheduler --workers 1

# get all averages
luigi --module yellow_taxis.tasks.averaging_tasks AggregateAveragesTask \
  --result-dir /path/to/results --local-scheduler --workers 1
```


However, the task modules are also executable scripts and their main function can be run direcly, e.g.

``` shell
./averaging_tasks.py
```

 By default, it runs the jobs locally with a single worker. You can increase the number of parallel jobs by changing the `luigi.build(…, workers=<num workers>)` in the main function at the bottom of each script. Each worker might require up to couple of GB of memory, so only increase this for local tasks if you have sufficient memory (or configure resources as described below).

If you installed the project via PDM, you can also run the pdm commands

``` shell
pdm run average-locally  # will also run download tasks as dependencies
pdm run download-locally  # only trigger download tasks
```

##### Central scheduler
To get visualization of the pipeline in a web interface, use the [luigi central scheduler](https://luigi.readthedocs.io/en/stable/central_scheduler.html). Here's a simple example usage:

``` shell
luigid --port 8887 # in a separate terminal or use `--background`
luigi --module yellow_taxis.tasks.averaging_tasks AggregateAveragesTask \
  --result-dir /path/to/results --scheduler-port 8887 --workers 1
```



#### Configuring Luigi and managing resources

You can configure luigi and its scheduler using a `luigi.cfg` or `luigi.toml` file as described in the [Luigi configuration docs](https://luigi.readthedocs.io/en/stable/configuration.html).

This is for example useful for limiting resource usage such a CPU's, memory, number of parallel downloads, number of parallel batch submissions etc.
Under the [`[resources]`](https://luigi.readthedocs.io/en/stable/configuration.html?highlight=resources#resources) section you can
configure resources. The resource usage of each task is determined by the dictionary under its `Task.resources` attribute. For example add

``` toml
[resources]
downloads = 4 # parallel downloads # TODO: not implemented yet
memory = 16000 # memory usage estimate in mb # TODO: not implemented yet
cpus = 8 # number CPU cores to use # TODO: not implemented yet
```

#### # Running as docker container

TODO

### Deploy to batch systems

TODO

## Author

[Michael Eliachevitch](mailto:m.eliachevitch@posteo.de "Email-Address")

<!-- Local Variables: -->
<!-- mode: gfm -->
<!-- End: -->
