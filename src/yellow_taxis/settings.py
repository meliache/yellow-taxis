"""Module to read the settings from the ``config.toml`` file."""

import tomllib
from pathlib import Path
from typing import Any

REPO_ROOT = (Path(__file__).parent.parent.parent).absolute()
SETTINGS_FILE_PATH = REPO_ROOT / "config.toml"


def get_settings() -> dict[str, Any]:
    """Get settings dictionary from ``config.toml`` file in repo root."""
    with open(SETTINGS_FILE_PATH, "rb") as settings_file:
        return tomllib.load(settings_file)


def get_result_dir() -> Path | None:
    """Return default ``result_dir`` from settings if provided or ``None``
    otherwise."""
    result_dir: str | None = get_settings().get("result_dir")
    if result_dir:
        if not isinstance(result_dir, str):
            return TypeError(f"{result_dir=} is not a string!")
        result_dir = Path(result_dir).expanduser()
        if not result_dir.is_absolute():
            raise ValueError(
                "Relative path names for ``result_dir`` setting are not allowed!"
            )
    return result_dir
