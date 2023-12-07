import tempfile
from pathlib import Path

import luigi
import pytest
from yellow_taxis.task_utils import ManagedOutputTask


class TestManagedOutputTask:
    def test_output_basename_not_implemented(self) -> None:
        with pytest.raises(TypeError):

            class _TestClass(ManagedOutputTask):
                pass

            _TestClass()

    def test_output_basename_implemented(self) -> None:
        class _TestClass(ManagedOutputTask):
            output_base_name = Path("test")
            pass

        test_instance = _TestClass()
        assert test_instance.output_base_name == Path("test")

    def test_get_output_path_correct_path(self) -> None:
        class _TestClass(ManagedOutputTask):
            output_base_name = Path("output")
            param_a = luigi.Parameter()
            param_b = luigi.IntParameter()

        with tempfile.TemporaryDirectory() as tmpdirname:
            test_class = _TestClass(
                param_a="a",
                param_b=42,
                result_dir=tmpdirname,
            )
            output_path = test_class.get_output_path(mkdir=False)
            expected_output_path = (
                Path(tmpdirname) / "param_a=a" / "param_b=42" / "output"
            )
            assert output_path == expected_output_path

    def test_get_output_path_mkdir(self) -> None:
        class _TestClass(ManagedOutputTask):
            output_base_name = Path("output")
            param_a = luigi.Parameter()
            param_b = luigi.IntParameter()

        with tempfile.TemporaryDirectory() as tmpdirname:
            test_class = _TestClass(
                param_a="a",
                param_b=42,
                result_dir=tmpdirname,
            )
            path = test_class.get_output_path(mkdir=True)
            assert path.parent.is_dir()

    def test_get_output_path_not_mkdir(self) -> None:
        class _TestClass(ManagedOutputTask):
            output_base_name = Path("output")
            param_a = luigi.Parameter()
            param_b = luigi.IntParameter()

        with tempfile.TemporaryDirectory() as tmpdirname:
            test_class = _TestClass(
                param_a="a",
                param_b=42,
                result_dir=tmpdirname,
            )
            path = test_class.get_output_path(mkdir=False)
            assert not path.parent.is_dir()
