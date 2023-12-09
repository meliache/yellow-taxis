# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.1] - 2023-12-09

### Fixed

* Fix final aggregating tasks (which create merged parquet files) not re-running when new datasets become available. This error occurred because the date of the latest dataset was not encoded in their output, so when new source datasets become available these tasks would keep being "done" until their outputs are deleted. Fixed that by encoding the latest dataset update date in their output. For this, added a `last_month` parameter to them and added tests to make sure the task is re-run when the parameters changes.


**Full Changelog**: https://github.com/meliache/yellow-taxis/compare/v0.2.0...v0.2.1

## [0.2.0] - 2023-12-07

### Added

First fully working and tested pipeline. Submitted solution to coding challenge.
