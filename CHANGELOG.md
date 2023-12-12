# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Fix old task API of `month` and `year` parameters still used in example notebook rather than new `month_date` parameters.

## [0.3.1] - 2023-12-11

### Fixed

* Fix Pandas query on `datetime.date`, convert it to `pandas.Timestamp` first.
- Fix old API still used in a test

**Full Changelog**: https://github.com/meliache/yellow-taxis/compare/v0.3.0...v0.3.1

## [0.3.0] - 2023-12-09

### Changed

* Use `luigi.MonthParameter` to encode months in my tasks instead of separate year and month `IntParameter`s. This changes how dates are supplied in the command line and also how the dates are interpolated in the output directory structure.

* Use `datetime.date` as much as possible in my utility functions. Changed therefore the API of functions that either took separate year and month integers or pandas timestamps. I tried to move away from pandas timestamps away as much as possible, to allow using a different dataframe implementation in the future.


**Full Changelog**: https://github.com/meliache/yellow-taxis/compare/v0.2.1...v0.3.0

## [0.2.1] - 2023-12-09

### Fixed

* Fix final aggregating tasks (which create merged parquet files) not re-running when new datasets become available. This error occurred because the date of the latest dataset was not encoded in their output, so when new source datasets become available these tasks would keep being "done" until their outputs are deleted. Fixed that by encoding the latest dataset update date in their output. For this, added a `last_month` parameter to them and added tests to make sure the task is re-run when the parameters changes.


**Full Changelog**: https://github.com/meliache/yellow-taxis/compare/v0.2.0...v0.2.1

## [0.2.0] - 2023-12-07

### Added

First fully working and tested pipeline. Submitted solution to coding challenge.
