from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class AppConfig:
    app_name: str = "YouTube Data API v3 収集・統計ツール"
    data_root: Path = Path("data")
    outputs_root: Path = Path("outputs")

    # Safety limits (can be overridden in UI)
    max_channels_per_run: int = 200
    max_videos_per_run: int = 20000


CFG = AppConfig()
