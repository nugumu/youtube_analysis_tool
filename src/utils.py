from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional, Tuple, List

from dateutil import parser as dtparser


CHANNEL_ID_RE = re.compile(r"\bUC[0-9A-Za-z_-]{20,}\b")
VIDEO_ID_RE = re.compile(r"\b[0-9A-Za-z_-]{11}\b")

CHANNEL_URL_RE = re.compile(r"https?://(?:www\.)?youtube\.com/channel/(UC[0-9A-Za-z_-]{20,})")

# Handles / custom URLs are tricky without search. We still detect them.
HANDLE_URL_RE = re.compile(r"https?://(?:www\.)?youtube\.com/@([0-9A-Za-z_.-]{1,})")
CUSTOM_URL_RE = re.compile(r"https?://(?:www\.)?youtube\.com/(c|user)/([0-9A-Za-z_.-]{1,})")

VIDEO_URL_RE = re.compile(r"https?://(?:www\.)?youtube\.com/watch\?v=([0-9A-Za-z_-]{11})")
SHORTS_URL_RE = re.compile(r"https?://(?:www\.)?youtube\.com/shorts/([0-9A-Za-z_-]{11})")
YOUTU_BE_RE = re.compile(r"https?://youtu\.be/([0-9A-Za-z_-]{11})")


def slugify(
    value: str,
    max_len: int = 50,
) -> str:
    value = unicodedata.normalize("NFKC", value).strip().lower()
    value = re.sub(r"[^a-z0-9\-_]+", "-", value)
    value = re.sub(r"-+", "-", value).strip("-")
    if not value:
        value = "project"
    return value[:max_len]


def ensure_dir(
    p: Path,
) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p


def safe_filename(
    name: str,
    max_len: int = 80,
) -> str:
    name = unicodedata.normalize("NFKC", name)
    name = re.sub(r"[\\/:*?\"<>|]+", "_", name)
    name = name.strip().strip(".")
    if not name:
        name = "artifact"
    return name[:max_len]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_rfc3339(
    dt: str,
) -> Optional[datetime]:
    try:
        return dtparser.isoparse(dt)
    except Exception:
        return None


def to_date_str(
    dt: Optional[datetime],
) -> Optional[str]:
    if dt is None:
        return None
    return dt.date().isoformat()


def parse_iso8601_duration_to_seconds(dur: str) -> Optional[int]:
    """Parse a subset of ISO8601 durations returned by YouTube (e.g., PT1H2M3S)."""
    if not dur or not dur.startswith("P"):
        return None
    # Only supports time part PT... (YouTube durations are typically PT...)
    m = re.match(r"P(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)", dur)
    if not m:
        return None
    h = int(m.group(1) or 0)
    mi = int(m.group(2) or 0)
    s = int(m.group(3) or 0)
    return h * 3600 + mi * 60 + s


@dataclass
class ParsedInputs:
    channel_ids: List[str]
    video_ids: List[str]
    handles: List[str]
    custom_usernames: List[Tuple[str, str]]  # (kind, name)


def parse_inputs(
    lines: Iterable[str],
) -> ParsedInputs:
    channel_ids: List[str] = []
    video_ids: List[str] = []
    handles: List[str] = []
    custom_usernames: List[Tuple[str, str]] = []

    for raw in lines:
        s = (raw or "").strip()
        if not s:
            continue

        # channel url
        m = CHANNEL_URL_RE.search(s)
        if m:
            channel_ids.append(m.group(1))
            continue

        # channel id in text
        m = CHANNEL_ID_RE.search(s)
        if m:
            channel_ids.append(m.group(0))
            continue

        # video URLs
        for rx in [VIDEO_URL_RE, SHORTS_URL_RE, YOUTU_BE_RE]:
            m = rx.search(s)
            if m:
                video_ids.append(m.group(1))
                break
        else:
            # raw video id
            if VIDEO_ID_RE.fullmatch(s):
                video_ids.append(s)
                continue

            # handles / custom URLs (need search.list or other methods)
            m = HANDLE_URL_RE.search(s)
            if m:
                handles.append(m.group(1))
                continue
            m = CUSTOM_URL_RE.search(s)
            if m:
                custom_usernames.append((m.group(1), m.group(2)))
                continue

    # de-dupe while preserving order
    def dedupe(
        seq: List[str],
    ) -> List[str]:
        out = []
        seen = set()
        for x in seq:
            if x not in seen:
                out.append(x)
                seen.add(x)
        return out

    return ParsedInputs(
        channel_ids=dedupe(channel_ids),
        video_ids=dedupe(video_ids),
        handles=dedupe(handles),
        custom_usernames=list(dict.fromkeys(custom_usernames)),
    )


def human_int(
    x: Optional[int],
) -> str:
    if x is None:
        return ""
    try:
        n = int(x)
    except Exception:
        return str(x)
    if n < 1000:
        return str(n)
    for unit, div in [("K", 1_000), ("M", 1_000_000), ("B", 1_000_000_000)]:
        if n < div * 1000:
            return f"{n/div:.1f}{unit}"
    return f"{n/1_000_000_000_000:.1f}T"
