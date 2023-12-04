# TODO implement some settings system to set result directory etc.
from pathlib import Path
from typing import Any

import tomli

REPO_ROOT = (Path(__file__).parent.parent.parent).absolute()
SETTINGS_FILE_PATH = REPO_ROOT / "config.toml"


def get_settings() -> dict[str, Any]:
    """Get settings dictionary from ``config.toml`` file in repo root."""
    with open(SETTINGS_FILE_PATH, "rb") as settings_file:
        return tomli.load(settings_file)
