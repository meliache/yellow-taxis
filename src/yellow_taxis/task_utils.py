"""Utility functions for use in Luigi tasks."""

import abc
from os import PathLike
from pathlib import Path

import luigi


class TaxiBaseTask(luigi.Task, abc.ABC):
    result_dir = luigi.PathParameter(
        absolute=True,
        significant=False,
        description="Root directory under which to store downloaded files.",
    )

    @property
    @abc.abstractmethod
    def output_base_name(self) -> PathLike:
        """Base file name for the output."""
        raise NotImplementedError(
            "Add an ``output_base_name`` static property to the the class."
        )

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

    def output(self):
        return luigi.LocalTarget(self.get_output_path())
