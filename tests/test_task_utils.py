import tempfile
from pathlib import Path

from yellow_taxis import task_utils


class TestYearMonthResultDir:
    def year_month_result_dir_pad_month(self):
        generated: Path = task_utils.year_month_result_dir(
            result_dir="/data",
            year=2023,
            month=9,
            make_parents=False,
        )
        assert generated.as_posix() == "/data/2023/09/"

    def year_month_result_dir_dont_pad_two_digit_month(self):
        generated: Path = task_utils.year_month_result_dir(
            result_dir="/data",
            year=2023,
            month=12,
            make_parents=False,
        )
        assert generated.as_posix() == "/data/2023/12/"

    def year_month_result_dir_make_parents_by_default(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            generated: Path = task_utils.year_month_result_dir(
                result_dir=Path(tmpdirname) / "data/subdir/",
                year=2023,
                month=12,
            )
            assert generated.is_dir()

    def year_month_result_dir_not_make_parents(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            generated: Path = task_utils.year_month_result_dir(
                result_dir=Path(tmpdirname) / "data/subdir/",
                year=2023,
                month=12,
                make_parents=False,
            )
            assert not generated.is_dir()
