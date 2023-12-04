"""Utility functions for use in Luigi tasks."""

import abc
from os import PathLike
from pathlib import Path

import luigi

from yellow_taxis.settings import get_settings


class TaxiBaseTask(luigi.Task, abc.ABC):
    """Base class for Tasks in Yellow Taxi data pipeline.

    Mostly as helper for automatizing output handing. It provides a ``Task.output``
    by using the required ``output_base_name`` property, the ``result_dir`` and
    encoding all other Parameter values as directories.
    """

    # introduce luigi parameters with settings common to all tasks

    default_result_dir: str | None = get_settings().get("result_dir")
    if default_result_dir:
        default_result_dir = Path(default_result_dir).expanduser()
        if not default_result_dir.is_absolute():
            raise ValueError(
                "Relative path names for ``result_dir`` setting are not allowed!"
            )

    result_dir = luigi.PathParameter(
        default=default_result_dir,
        absolute=True,
        significant=False,
        description="Root directory under which to store downloaded files.",
    )

    default_max_duration: str | None = get_settings().get("max_duration")
    max_duration = luigi.IntParameter(
        description="Reject trips with duration longer than this. In seconds.",
        default=default_max_duration,  # 4h
    )

    default_distance_duration: int | None = get_settings().get("max_distance")
    max_distance = luigi.IntParameter(
        description="Reject trips with distance longer than this.",
        default=default_max_duration,
    )

    @property
    @abc.abstractmethod
    def output_base_name(self) -> PathLike:
        """Base file name for the output."""
        raise NotImplementedError(
            "Add an ``output_base_name`` static property to the the class."
        )

    def output(self):
        """Luigi target for the generated output path.

        The returned target defines the completeness of the task.
        See ``luigi.Task.output``.
        """
        return luigi.LocalTarget(self.get_output_path())

    def get_output_path(self, mkdir: bool = True) -> Path:
        """Output file name constructed from ``output_base_name`` and luigi Parameters.

        Output path will have structure::

            self.result_dir/param1=value1/self.param2=value2/…/self.output_base_name

        Parameters created with ``significant=False`` will not be encoded in the output.

        :param mkdir: If ``True`` create missing parent directories of returned output.
        :return: Final file system output path.
        """
        output_dir = Path(self.result_dir)
        for param_name, param in self.get_params():
            param_value = self.param_kwargs[param_name]
            param_value_serialized = param.serialize(param_value)

            # don't encode insignificant parameters in output
            if not param.significant:
                continue

            # Raise errors if parameter contains e.g. slashes which result in it being
            # interpreted as a directory
            if param_value_serialized != Path(param_value_serialized).name:
                raise ValueError(
                    f"Invalid characters in {param_value}."
                    " Parameter cannot be interpreted as a file base name."
                )

            subdir_name = f"{param_name}={param_value}"
            output_dir = output_dir / subdir_name

        if mkdir:
            output_dir.mkdir(parents=True, exist_ok=True)

        return output_dir / self.output_base_name
