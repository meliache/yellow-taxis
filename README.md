# Yellow Taxis

![Pytest](https://github.com/meliache/yellow-taxis/actions/workflows/pytest.yml/badge.svg)
[![Codecov](https://codecov.io/gh/meliache/yellow-taxis/graph/badge.svg?token=QB6OA6CPVT)](https://codecov.io/gh/meliache/yellow-taxis)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/release/python-311/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![pdm-managed](https://img.shields.io/badge/pdm-managed-blueviolet)](https://pdm-project.org)

Analysis of average travel times of _Yellow Taxis_ in New York City.

Data source: [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## Table of contents

<!-- toc -->

- [Installation](#installation)
  * [Developer installation](#developer-installation)
  * [Non-Python dependencies](#non-python-dependencies)
- [Running the pipeline](#running-the-pipeline)
  * [Local pipeline](#local-pipeline)
  * [Central scheduler](#central-scheduler)
  * [Configuring default parameters](#configuring-default-parameters)
  * [Configuring Luigi and managing resources](#configuring-luigi-and-managing-resources)
  * [Running as docker container](#running-as-docker-container)
- [Deploy to batch systems](#deploy-to-batch-systems)
  * [Using b2luigi](#using-b2luigi)
  * [Using `luigi.contrib` packages to run on AWS and other commercial batch systems](#using-luigicontrib-packages-to-run-on-aws-and-other-commercial-batch-systems)
  * [Scaling file storage](#scaling-file-storage)
- [Example plots](#example-plots)
- [Author](#author)

<!-- tocstop -->

## Installation

First clone the repository and enter the root directory:

``` shell
git clone https://github.com/meliache/yellow-taxis
cd yellow-taxis
```

Then install the project using `pip` or [PDM](https://github.com/pdm-project/pdm), the package manager used for development of this project:

``` shell
# PDM:
curl -sSL https://pdm-project.org/install-pdm.py | python3 -  # install PDM
pdm install --production

# Alternative, pure pip
pip install --editable .
```

I recommend using virtual environment, either via `python -m venv` or `pdm venv create`.

### Developer installation

Additionally this project has optional dependencies for developing, testing and for using Jupyter. Without using `--production`, PDM will install all optional dependencies, but you can select which you want with

``` shell
pdm install --dev \
	--group dev \
	--group test \
	--group jupyter \
	--group batch
```
With pip you can do the same via

``` shell
pip install --editable '.[dev,test,jupyter,batch,jupyter]' # choose what you need
pip install --editable '.[complete]' # install all

```

Further, if you want to contribute, please enable [pre-commit](https://pre-commit.com/), which is in the `test` dependencies:

``` shell
pdm pre-commit install
```

To fix style and errors before being bothered by pre-commit, I recommend setting up your editor/IDE to auto-format the project with [ruff](https://docs.astral.sh/ruff/formatter/), [isort](https://pycqa.github.io/isort/) and [docformatter](https://docformatter.readthedocs.io/en/latest).

### Non-Python dependencies

- [curl](https://curl.se): The `curl` command-line-tool is currently used for downloading the datasets. It is available for all desktop operating systems and comes pre-installed on Windows and macOS.

## Running the pipeline

For creating the pipeline I have used [Luigi](https://github.com/spotify/luigi), which I have been familiar with from my academic work. Luigi defines workflows using classes implementing `luigi.Task`. Completeness is defined by the `Task.output()` method which return one or more `Luigi.Target` instances. Requirements (other tasks) are defined by the `Task.requires()` method. The actual work is done in the `Task.run()` method. Refer to the [Luigi docs](https://luigi.readthedocs.io/en/stable/index.html) for more information.

### Local pipeline

The tasks defining the pipeline are found in [`src/yellow_taxis/tasks/`](https://github.com/meliache/yellow-taxis/tree/main/src/yellow_taxis/tasks). You can custom trigger individual tasks by using the `luigi` (or `pdm run luigi`) command line tool, which will also run all their dependencies. This method allows setting the task parameters, scheduler and number of workers on the command line


``` shell
# run entire pipeline with all tasks
pdm run luigi --module yellow_taxis.tasks.run_all MainTask \
	--local-scheduler --workers 1

# pipeline for all monthly averages
pdm run luigi --module yellow_taxis.tasks.monthly_averages AggregateMonthlyAveragesTask \
   --local-scheduler --workers 1

# pipeline for all rolling averages
pdm run luigi --module yellow_taxis.tasks.rolling_averages AggregateRollingAveragesTask \
   --local-scheduler --workers 1

# get rolling averages for a single month (takes into account the previous 2 months in the avg.)
pdm run luigi --module yellow_taxis.tasks.rolling_averages RollingAveragesTask \
   --year 2023 --month 8 --local-scheduler --workers 1


# get monthly averages for a single month
pdm run luigi --module yellow_taxis.tasks.monthly_averages MonthlyAveragesTask \
   --year 2023 --month 8 --local-scheduler --workers 1
```


However, the task modules are also executable scripts and their main function can be run directly, e.g.

``` shell
./src/yellow_taxis/tasks/run_all.py
```

 By default, it runs the jobs locally with a single worker. You can increase the number of parallel jobs by changing the `luigi.build(…, workers=<num workers>)` in the main function at the bottom of each script. Each worker might require up to couple of GB of memory, so only increase this for local tasks if you have sufficient memory (or configure resources as described below).

If you installed the project via PDM, you can also run the PDM commands

``` shell
pdm run run-all-locally  # run all tasks
pdm run monthly-average-locally  # calculate all running averages
pdm run rolling-average-locally  # calculate all monthly averages
pdm run download-locally  # (not really needed, downloads triggered automatically by other tasks)
```

### Central scheduler
To get visualization of the pipeline in a web interface, use the [luigi central scheduler](https://luigi.readthedocs.io/en/stable/central_scheduler.html). Here's a simple example usage:

``` shell
pdm run luigid --port 8887 # in a separate terminal or use `--background`
pdm run luigi --module yellow_taxis.tasks.monthly_averages AggregateMonthlyAveragesTask \
   --scheduler-port 8887 --workers 1
```

![Task Hierarchy Visualizer of the Luigi central scheduler](https://raw.githubusercontent.com/meliache/yellow-taxis/main/images/screenshots/Luigi%20Task%20Visualiser%20Rolling.webp)

### Configuring default parameters

The luigi parameters for all tasks can set in the task constructors, or when using the `luigi` command using command line flags. However, parameters which are share across tasks can be configured using the [`config.toml`](https://github.com/meliache/yellow-taxis/tree/main/config.toml) in the repository root.

All downloads and task outputs will be saved in `result_dir`. Please set it to the absolute path on a device where you have sufficient storage space or to some kind of mounted network storage.

### Configuring Luigi and managing resources

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

### Running as docker container

The workflows can also be deployed in a docker file:
``` shell
docker build . -t yellow-taxis
```

Then, commands can be run in the image, e.g.
``` shell
docker run --rm yellow-taxis pdm run luigi \
  --module yellow_taxis.tasks.monthly_averages AggregateMonthlyAveragesTask \
  --result-dir /data/results --local-scheduler --workers 1
```

By default it runs the `luigid` central scheduler on port 8887.

Note that the dockerized luigi is not aware of the host `luigi.cfg`/`luigi.toml` configuration files in `~/.config/luigi`. The docker image should be built with a custom config file, which can be achieved by placing one in the repository root before building it.

## Deploy to batch systems

With this pipeline the entire yellow-taxi dataset can be analyzed locally on an average notebook, though due to memory constraints only with few parallel workers, which might take a long time for all the historical data and might not scale well for future datasets. Therefore, the computation can be delegated to some kind of batch system.


### Using b2luigi

[b2luigi](https://b2luigi.readthedocs.io/en/stable) is a Luigi extension created to deploy tasks to batch system workers with minimal batch-specific changes to the tasks themselves, just by changing the runner. It was written by Physicists (including me) with their needs and constraints in mind. But currently only the [LSF](https://www.ibm.com/de-de/products/hpc-workload-management) and [HTCondor](HTCondor) batch systems are fully implemented. It is a drop-in replacement, so if one of those is batch systems is available, one can change the tasks to run on it by simply by

``` python
import b2luigi as luigi
b2luigi.set_setting("batch_system", "htcondor")  # or "lsf"
```
or setting a `--batch` command line parameter. For more see the [b2luigi batch manual](https://b2luigi.readthedocs.io/en/stable/usage/batch.html).

Other more batch systems can be added by implementing [`b2luigi.batch.processes.BatchProcess`](https://b2luigi.readthedocs.io/en/stable/usage/batch.html#add-your-own-batch-system).

### Using `luigi.contrib` packages to run on AWS and other commercial batch systems

The [luigi.contrib](https://luigi.readthedocs.io/en/stable/api/luigi.contrib.html) repository already contains several tasks for running on commercially popular batch systems. But they need to be inherited from and usually overwrite the task's `run` method, meaning that the pipeline will need to be adapted to a specific batch.

For example, for running on *AWS*, the [`luigi.contrib.batch.BatchTask`](https://luigi.readthedocs.io/en/stable/api/luigi.contrib.batch.html) exists. It can only run docker containers via job definitions, but we can do that via the dockerfile provided in this repository.

I'll sketch out my idea for a possible implementation: We could make all our tasks implement `luigi.contrib.batch.BatchTask`. But we can override its `run` method to react to a luigi Parameter (which can be provided on the command line) and if it is given, just do the calculation locally instead of submitting an AWS job. The `job_definition` method then could be implemented that the tasks submits itself as a docker command, but with that parameter to do the calculation locally.

### Scaling file storage

Currently the whole dataset below 30 GB large, which might be challenging on a personal computer/notebook with limited space, but a factor 100 more could easily fit onto a not too expensive hard drive, so realistically speaking file storage should be not a big issue for this coding challenge. But in a many other realistic scenarios using a large, scaleable, reliable distributed file/storage system could be used, like HDFS or S3.

They could be used with `luigi.LocalTarget` by just mounting the file systems over network, e.g. via [`rclone` mount](https://rclone.org/overview), But for those cases it might be better using specific targets like [HdfsTarget](https://luigi.readthedocs.io/en/stable/api/luigi.contrib.hdfs.target.html#module-luigi.contrib.hdfs.target) or [S3Target](https://luigi.readthedocs.io/en/stable/api/luigi.contrib.s3.html).

## Example plots

The creation of plots is not part of the implemented pipeline and of this Python package.
They have been created with the [result_visualization.ipynb](https://github.com/meliache/yellow-taxis/blob/main/notebooks/result_visualization.ipynb) notebook in the repository, which is meant as an example how to analyzed the results from the data pipeline.
The plots had been useful to discover issues with the data such as outliers and wrong dates.

Monthly averages             |  Rolling averages
:-------------------------:|:-------------------------:
![Monthly averages](https://raw.githubusercontent.com/meliache/yellow-taxis/main/images/example_plots/trip_lenghts_monthly_averages.webp)  |  ![Rolling averages](https://raw.githubusercontent.com/meliache/yellow-taxis/main/images/example_plots/trip_lenghts_rolling_averages.webp)


## Author

[Michael Eliachevitch](mailto:m.eliachevitch@posteo.de "Email-Address")

<!-- Local Variables: -->
<!-- mode: gfm -->
<!-- End: -->
