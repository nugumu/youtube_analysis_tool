from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .utils import ensure_dir, safe_filename


@dataclass
class ArtifactPaths:
    base_dir: Path

    def child(
        self,
        name: str,
    ) -> Path:
        return self.base_dir / safe_filename(name)


def ensure_output_dir(
    base: Path,
    project_slug: str,
) -> ArtifactPaths:
    out = ensure_dir(base / project_slug)
    return ArtifactPaths(base_dir=out)


def save_df_csv(
    df: pd.DataFrame,
    out_dir: ArtifactPaths,
    filename: str,
) -> Path:
    p = out_dir.child(filename)
    if not p.name.endswith(".csv"):
        p = p.with_suffix(".csv")
    df.to_csv(p, index=False)
    return p


def save_bytes(
    out_dir: ArtifactPaths,
    filename: str,
    content: bytes,
) -> Path:
    p = out_dir.child(filename)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(content)
    return p
