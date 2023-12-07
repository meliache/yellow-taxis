from pathlib import Path
from unittest.mock import mock_open, patch

import pytest
from yellow_taxis.settings import get_result_dir, get_settings


class TestSettings:
    test_conf_file_contets = b"""\
result_dir = "/path/to/data/"

# reject trip times with duration longer than this as outliers (in seconds)
max_duration = 7200  # 2h

# Reject trip distances with duration longer than this as outliers (in Miles)
max_distance = 800

# window length for rolling average in days
rolling_window = 40
"""

    def test_get_settings(self) -> None:
        m_open = mock_open(read_data=self.test_conf_file_contets)
        with patch("yellow_taxis.settings.open", m_open):
            settings = get_settings()

        expected_settings = {
            "result_dir": "/path/to/data/",
            "max_duration": 7200,
            "max_distance": 800,
            "rolling_window": 40,
        }
        assert settings == expected_settings

    def test_get_result_dir(self) -> None:
        m_open = mock_open(read_data=self.test_conf_file_contets)
        with patch("yellow_taxis.settings.open", m_open):
            result_dir = get_result_dir()
        assert result_dir == Path("/path/to/data/")

    def test_get_result_dir_expands_user_tilde(self) -> None:
        test_conf_file_contets = b"""\
result_dir = "~/data"
"""

        m_open = mock_open(read_data=test_conf_file_contets)
        with patch("yellow_taxis.settings.open", m_open):
            result_dir = get_result_dir()
        assert result_dir == Path.home() / "data"

    def test_get_result_raises_on_relative_path(self) -> None:
        test_conf_file_contets = b"""\
result_dir = "data"
"""

        m_open = mock_open(read_data=test_conf_file_contets)
        with patch("yellow_taxis.settings.open", m_open), pytest.raises(ValueError):
            get_result_dir()

    def test_get_result_dir_is_none_if_not_set(self) -> None:
        test_conf_file_contets = b""

        m_open = mock_open(read_data=test_conf_file_contets)
        with patch("yellow_taxis.settings.open", m_open):
            assert get_result_dir() is None
