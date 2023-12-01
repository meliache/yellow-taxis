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
- [X] Make sure your program could be run in a production setup.

## Installation

First clone the repository and enter the root directory:

``` shell
git clone https://github.com/meliache/yellow-taxis
cd yellow-taxis
```

Then install the project using `pip` or [pdm](https://github.com/pdm-project/pdm), the package manager used for development of this project:

``` shell
# PDM:
curl -sSL https://pdm-project.org/install-pdm.py | python3 -  # install PDM
pdm install --production

# Alternative, pure pip
pip install --editable .
```

I recommend using virtual environment, either via `python -m venv` or `pdm venv create`.

### Developer installation

Additionally this project has optional dependencies for developing, testing and for using Jupyter. Without using `--production`, pdm will install all optional dependencies, but you can select which you want with

``` shell
pdm install --dev --group dev --group test --group jupyter $alterna
```
With pip you can do the same via

``` shell
pip install --editable '.[dev,test,jupyter]' # choose what you need
pip install --editable '.[complete]' # install all

```

Further, if you want to contribute, please enable [pre-commit](https://pre-commit.com/), which is in the `test` dependencies:

``` shell
pdm pre-commit install
```

But to be less bothered by pre-commit's errors I recommend setting up your editor/IDE to auto-format the project with [ruff](https://docs.astral.sh/ruff/formatter/), [isort](https://pycqa.github.io/isort/) and [docformatter](https://docformatter.readthedocs.io/en/latest).

### Non-Python dependencies

- [curl](https://curl.se): The `curl` commandline-tool is currently used for downloading the datasets. It is available for all desktop operating systems and comes pre-installed on Windows and MacOS.

### Running the pipeline

For creating the pipeline I have used [Luigi](https://github.com/spotify/luigi), which I have been familiar with from my academic work. Luigi defines workflows using classes implementing `luigi.Task`. Completeness is defined by the `Task.output()` method which return one or more `Luigi.Target` instances. Requirements (other tasks) are defined by the `Task.requires()` method. The actual work is done in the `Task.run()` method. Refer to the [Luigi docs](https://luigi.readthedocs.io/en/stable/index.html) for more information.

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
pdm run luigid --port 8887 # in a separate terminal or use `--background`
pdm run luigi --module yellow_taxis.tasks.averaging_tasks AggregateAveragesTask \
  --result-dir /path/to/results --scheduler-port 8887 --workers 1
```

![Task Hierarchy Visualizer of the Luigi central scheduler](https://raw.githubusercontent.com/meliache/yellow-taxis/main/screenshots/Luigi%20Task%20Visualiser.webp)

#### Configuring Luigi and managing resources

You can configure luigi and its scheduler using a `luigi.cfg` or `luigi.toml` file as described in the [Luigi configuration docs](https://luigi.readthedocs.io/en/stable/configuration.html).

This is for example useful for limiting resource usage such a CPU's, memory, number of parallel downloads, number of parallel batch submissions etc.
Under the [`[resources]`](https://luigi.readthedocs.io/en/stable/configuration.html?highlight=resources#resources) section you can
configure resources. The resource usage of each task is determined by the dictionary under its `Task.resources` attribute. For example add

``` toml
[resources]
downloads = 4 # max parallel downloads
memory = 16000 # memory usage estimate in MB
cpus = 8 # number CPU cores to use
```

#### # Running as docker container

The workflows can also be deployed in a docker file:
``` shell
docker build . -t yellow-taxis
```

Then, commands can be run in the image via
``` shell
docker run --rm yellow-taxis pdm run luigi \
  --module yellow_taxis.tasks.averaging_tasks MonthlyAveragesTask \
  --result-dir /data/results --year 2023 --month 8 --local-scheduler --workers 1
```

By default it runs the `luigid` central scheduler on port 8887.

Note that the dockerized luigi is not aware of the host `luigi.cfg`/`luigi.toml` configuration files in `~/.config/luigi`. The docker image should be built with a custom config file, which can be achieved by placing one in the repository root before building it.

### Deploy to batch systems

With this pipeline the entire yellow-taxi dataset can be analyzed locally on an average notebook, though due to memory constraints only with few parallel workers, which might take a long time for all the historical data and might not scale well for future datasets. Therefore, the computation can be delegated to some kind of batch system.


#### Using b2luigi

[b2luigi](https://b2luigi.readthedocs.io/en/stable) is a Luigi extension created to deploy tasks to batch system workers with minimal batch-specific changes to the tasks themselves, just by changing the runner. It was written by Physicists (including me) with their needs and constraints in mind. But currently only the [LSF](https://www.ibm.com/de-de/products/hpc-workload-management) and [HTCondor](HTCondor) batch systems are fully implemented. It is a drop-in replacement, so if one of those is batch systems is available, one can change the tasks to run on it by simply by

``` python
import b2luigi as luigi
b2luigi.set_setting("batch_system", "htcondor")  # or "lsf"
```
or setting a `--batch` command line parameter. For more see the [b2luigi batch manual](https://b2luigi.readthedocs.io/en/stable/usage/batch.html).

Other more batch systems can be added by implementing [`b2luigi.batch.processes.BatchProcess`](https://b2luigi.readthedocs.io/en/stable/usage/batch.html#add-your-own-batch-system).

#### Using `luigi.contrib` packages to run on AWS and other commercial batch systems

The [luigi.contrib](https://luigi.readthedocs.io/en/stable/api/luigi.contrib.html) repository already contains several tasks for running on commercially popular batch systems. But they need to be inherited from and usually overwrite the task's `run` method, meaning that the pipeline will need to be adapted to a specific batch.

For example, for running on *AWS*, the [`luigi.contrib.batch.BatchTask`](https://luigi.readthedocs.io/en/stable/api/luigi.contrib.batch.html) exists. It can only run docker containers via job definitions, but we can do that via the dockerfile provided in this repository.

I'll sketch out my idea for a possible implementation: We could make all our tasks implement `luigi.contrib.batch.BatchTask`. But we can override its `run` method to react to a luigi Parameter (which can be provided on the command line) and if it is given, just do the calculation locally instead of submitting an AWS job. The `job_definition` method then could be implemented that the tasks submits itself as a docker command, but with that parameter to do the calculation locally.

##### Scaling file storage

Currently the whole dataset below 30 GB large, which might be challenging on a personal computer/notebook with limited space, but a factor 100 more could easily fit onto a not too expensive hard drive, so realistically speaking file storage should be not a big issue for this coding challenge. But in a many other realistic scenarios using a large, scalable, reliable distributed file/storage system could be used, like HDFS or S3.

They could be used with `luigi.LocalTarget` by just mounting the filesystems over network, e.g. via [`rclone` mount](https://rclone.org/overview), But for those cases it might be better using specific targes like [HdfsTarget](https://luigi.readthedocs.io/en/stable/api/luigi.contrib.hdfs.target.html#module-luigi.contrib.hdfs.target) or [S3Target](https://luigi.readthedocs.io/en/stable/api/luigi.contrib.s3.html).

## Author

[Michael Eliachevitch](mailto:m.eliachevitch@posteo.de "Email-Address")

<!-- Local Variables: -->
<!-- mode: gfm -->
<!-- End: -->
