"""Utility functions for use in Luigi tasks."""

import abc
from os import PathLike
from pathlib import Path

import luigi

from yellow_taxis.settings import get_result_dir, get_settings


class ManagedOutputTask(luigi.Task, abc.ABC):
    """Abstract task class for managing task outputs.

    It provides a ``Task.output`` by using the required ``output_base_name``
    class attribute, the ``result_dir`` parameter and
    encoding all other Parameter values as directories.
    """

    result_dir = luigi.PathParameter(
        default=get_result_dir(),
        absolute=True,
        significant=False,
        description="Root directory under which to store downloaded files.",
    )

    @property
    @abc.abstractmethod
    def output_base_name(self) -> PathLike:
        """Base file name for the output."""
        pass

    def output(self):
        """Luigi target for the generated output path.

        The returned target defines the completeness of the task.
        See ``luigi.Task.output``.
        """
        return luigi.LocalTarget(self.get_output_path())

    def get_output_path(self, mkdir: bool = True) -> Path:
        """Generated output file path for given set of parameters.

        Output path will have structure::

            self.result_dir/param1=value1/self.param2=value2/…/self.output_base_name

        Parameters created with ``significant=False`` will not be encoded in the output.

        :param mkdir: If ``True`` create missing parent directories of returned output.
        :return: Final file system output path.
        """
        output_dir = Path(self.result_dir).expanduser().absolute()

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


class TaxiBaseTask(ManagedOutputTask):
    """Base class for Tasks in Yellow Taxi data pipeline.

    Mostly for collecting parameters common to all tasks.
    """

    # introduce luigi parameters with settings common to all tasks

    default_max_duration: str | None = get_settings().get("max_duration")
    max_duration = luigi.IntParameter(
        description="Reject trips with duration longer than this. In seconds.",
        default=default_max_duration,  # 4h
    )

    default_max_distance: int | None = get_settings().get("max_distance")
    max_distance = luigi.IntParameter(
        description="Reject trips with distance in Miles longer than this",
        default=default_max_distance,
    )
