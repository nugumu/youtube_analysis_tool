from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from .utils import utc_now_iso, parse_iso8601_duration_to_seconds


SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS meta (
    k TEXT PRIMARY KEY,
    v TEXT
);

CREATE TABLE IF NOT EXISTS channels (
    channel_id TEXT PRIMARY KEY,
    title TEXT,
    description TEXT,
    published_at TEXT,
    country TEXT,
    custom_url TEXT,
    uploads_playlist_id TEXT,
    view_count INTEGER,
    subscriber_count INTEGER,
    video_count INTEGER,
    fetched_at TEXT
);

CREATE TABLE IF NOT EXISTS channel_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id TEXT NOT NULL,
    captured_at TEXT NOT NULL,
    view_count INTEGER,
    subscriber_count INTEGER,
    video_count INTEGER,
    FOREIGN KEY(channel_id) REFERENCES channels(channel_id)
);

CREATE INDEX IF NOT EXISTS idx_channel_snapshots_channel_time
    ON channel_snapshots(channel_id, captured_at);

CREATE TABLE IF NOT EXISTS videos (
    video_id TEXT PRIMARY KEY,
    channel_id TEXT,
    title TEXT,
    description TEXT,
    published_at TEXT,
    duration_seconds INTEGER,
    category_id TEXT,
    tags_json TEXT,
    default_language TEXT,
    live_broadcast_content TEXT,
    scheduled_start_time TEXT,
    actual_start_time TEXT,
    actual_end_time TEXT,
    view_count INTEGER,
    like_count INTEGER,
    comment_count INTEGER,
    fetched_at TEXT,
    FOREIGN KEY(channel_id) REFERENCES channels(channel_id)
);

CREATE INDEX IF NOT EXISTS idx_videos_channel_published
    ON videos(channel_id, published_at);

CREATE TABLE IF NOT EXISTS video_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    video_id TEXT NOT NULL,
    captured_at TEXT NOT NULL,
    view_count INTEGER,
    like_count INTEGER,
    comment_count INTEGER,
    FOREIGN KEY(video_id) REFERENCES videos(video_id)
);

CREATE INDEX IF NOT EXISTS idx_video_snapshots_video_time
    ON video_snapshots(video_id, captured_at);

CREATE TABLE IF NOT EXISTS comment_threads (
    thread_id TEXT PRIMARY KEY,
    video_id TEXT NOT NULL,
    top_comment_id TEXT,
    total_reply_count INTEGER,
    fetched_at TEXT,
    FOREIGN KEY(video_id) REFERENCES videos(video_id)
);

CREATE INDEX IF NOT EXISTS idx_comment_threads_video
    ON comment_threads(video_id);

CREATE TABLE IF NOT EXISTS comments (
    comment_id TEXT PRIMARY KEY,
    video_id TEXT NOT NULL,
    author_channel_id TEXT,
    author_display_name TEXT,
    text TEXT,
    published_at TEXT,
    updated_at TEXT,
    like_count INTEGER,
    fetched_at TEXT,
    FOREIGN KEY(video_id) REFERENCES videos(video_id)
);

CREATE INDEX IF NOT EXISTS idx_comments_video_time
    ON comments(video_id, published_at);

CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_type TEXT,
    started_at TEXT,
    ended_at TEXT,
    status TEXT,
    details_json TEXT
);

-- For keyword-based collection tracking
CREATE TABLE IF NOT EXISTS search_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    q TEXT NOT NULL,
    order_by TEXT,
    mode TEXT,
    filters_json TEXT,
    collected_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_search_runs_q_time
    ON search_runs(q, collected_at);

CREATE TABLE IF NOT EXISTS search_run_videos (
    run_id INTEGER NOT NULL,
    video_id TEXT NOT NULL,
    PRIMARY KEY(run_id, video_id),
    FOREIGN KEY(run_id) REFERENCES search_runs(id),
    FOREIGN KEY(video_id) REFERENCES videos(video_id)
);

CREATE INDEX IF NOT EXISTS idx_search_run_videos_video
    ON search_run_videos(video_id);
"""


@dataclass
class JobRecord:
    id: int
    job_type: str
    started_at: str
    ended_at: Optional[str]
    status: str
    details_json: Optional[str]


def connect(
    db_path: Path,
) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn




def ensure_columns(
    conn: sqlite3.Connection,
    table: str,
    cols: Dict[str, str],
) -> None:
    """Add missing columns (SQLite migration-lite)."""
    existing = {r["name"] for r in conn.execute(f"pragma table_info({table})").fetchall()}
    for name, coltype in cols.items():
        if name in existing:
            continue
        conn.execute(f"alter table {table} add column {name} {coltype}")


def init_db(
    db_path: Path,
) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA_SQL)

        # Migration-lite: add new columns if the user upgrades the tool
        ensure_columns(conn, "videos", {
            "live_broadcast_content": "TEXT",
            "scheduled_start_time": "TEXT",
            "actual_start_time": "TEXT",
            "actual_end_time": "TEXT",
        })

        conn.commit()
    finally:
        conn.close()


def upsert_channels(
    conn: sqlite3.Connection,
    items: List[Dict[str, Any]],
) -> int:
    now = utc_now_iso()
    rows: List[Tuple] = []
    for it in items:
        cid = it.get("id")
        sn = it.get("snippet", {})
        st = it.get("statistics", {})
        cd = it.get("contentDetails", {})
        related = cd.get("relatedPlaylists", {}) if isinstance(cd, dict) else {}
        uploads = related.get("uploads")
        rows.append(
            (
                cid,
                sn.get("title"),
                sn.get("description"),
                sn.get("publishedAt"),
                sn.get("country"),
                sn.get("customUrl"),
                uploads,
                _to_int(st.get("viewCount")),
                _to_int(st.get("subscriberCount")),
                _to_int(st.get("videoCount")),
                now,
            )
        )

    conn.executemany(
        """
        insert into channels(
            channel_id,
            title,
            description,
            published_at,
            country,
            custom_url,
            uploads_playlist_id,
            view_count,
            subscriber_count,
            video_count,
            fetched_at
        ) values(?,?,?,?,?,?,?,?,?,?,?)
        on conflict(channel_id) do update set
            title=excluded.title,
            description=excluded.description,
            published_at=excluded.published_at,
            country=excluded.country,
            custom_url=excluded.custom_url,
            uploads_playlist_id=COALESCE(excluded.uploads_playlist_id, channels.uploads_playlist_id),
            view_count=excluded.view_count,
            subscriber_count=excluded.subscriber_count,
            video_count=excluded.video_count,
            fetched_at=excluded.fetched_at
        """,
        rows,
    )
    return len(rows)


def upsert_videos(
    conn: sqlite3.Connection,
    items: List[Dict[str, Any]],
) -> int:
    now = utc_now_iso()
    rows: List[Tuple] = []
    for it in items:
        vid = it.get("id")
        sn = it.get("snippet", {})
        ct = it.get("contentDetails", {})
        st = it.get("statistics", {})
        duration = parse_iso8601_duration_to_seconds(ct.get("duration", ""))
        tags = sn.get("tags")
        rows.append(
            (
                vid,
                sn.get("channelId"),
                sn.get("title"),
                sn.get("description"),
                sn.get("publishedAt"),
                duration,
                sn.get("categoryId"),
                json.dumps(tags, ensure_ascii=False) if isinstance(tags, list) else None,
                sn.get("defaultLanguage"),
                sn.get("liveBroadcastContent"),
                (it.get("liveStreamingDetails") or {}).get("scheduledStartTime"),
                (it.get("liveStreamingDetails") or {}).get("actualStartTime"),
                (it.get("liveStreamingDetails") or {}).get("actualEndTime"),
                _to_int(st.get("viewCount")),
                _to_int(st.get("likeCount")),
                _to_int(st.get("commentCount")),
                now,
            )
        )

    conn.executemany(
        """
        insert into videos(
            video_id,
            channel_id,
            title,
            description,
            published_at,
            duration_seconds,
            category_id,
            tags_json,
            default_language,
            live_broadcast_content,
            scheduled_start_time,
            actual_start_time,
            actual_end_time,
            view_count,
            like_count,
            comment_count,
            fetched_at
        ) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        on conflict(video_id) do update set
            channel_id=excluded.channel_id,
            title=excluded.title,
            description=excluded.description,
            published_at=excluded.published_at,
            duration_seconds=excluded.duration_seconds,
            category_id=excluded.category_id,
            tags_json=excluded.tags_json,
            default_language=excluded.default_language,
            live_broadcast_content=excluded.live_broadcast_content,
            scheduled_start_time=excluded.scheduled_start_time,
            actual_start_time=excluded.actual_start_time,
            actual_end_time=excluded.actual_end_time,
            view_count=excluded.view_count,
            like_count=excluded.like_count,
            comment_count=excluded.comment_count,
            fetched_at=excluded.fetched_at
        """,
        rows,
    )
    return len(rows)


def insert_channel_snapshots(
    conn: sqlite3.Connection,
    channel_ids: List[str],
) -> int:
    """Capture current stats from channels table into channel_snapshots."""
    now = utc_now_iso()
    q = """
    select
        channel_id,
        view_count,
        subscriber_count,
        video_count
    from channels
    where channel_id in ({})
    """.format(
        ",".join(["?"] * len(channel_ids))
    )
    rows = conn.execute(q, channel_ids).fetchall()
    out_rows = [(r["channel_id"], now, r["view_count"], r["subscriber_count"], r["video_count"]) for r in rows]
    conn.executemany(
        """
        insert into channel_snapshots(
            channel_id,
            captured_at,
            view_count,
            subscriber_count,
            video_count
        ) values(?,?,?,?,?)
        """,
        out_rows,
    )
    return len(out_rows)


def insert_video_snapshots(
    conn: sqlite3.Connection,
    video_ids: List[str],
) -> int:
    now = utc_now_iso()
    q = """
    select
        video_id,
        view_count,
        like_count,
        comment_count
    from videos
    where video_id in ({})
    """.format(
        ",".join(["?"] * len(video_ids))
    )
    rows = conn.execute(q, video_ids).fetchall()
    out_rows = [(r["video_id"], now, r["view_count"], r["like_count"], r["comment_count"]) for r in rows]
    conn.executemany(
        """
        insert into video_snapshots(
            video_id,
            captured_at,
            view_count,
            like_count,
            comment_count
        ) values(?,?,?,?,?)
        """,
        out_rows,
    )
    return len(out_rows)


def upsert_comment_threads_and_comments(
    conn: sqlite3.Connection,
    video_id: str,
    threads: List[Dict[str, Any]],
) -> Tuple[int, int]:
    now = utc_now_iso()

    thread_rows: List[Tuple] = []
    comment_rows: List[Tuple] = []

    for th in threads:
        thread_id = th.get("id")
        sn = th.get("snippet", {})
        top = sn.get("topLevelComment", {})
        top_sn = top.get("snippet", {}) if isinstance(top, dict) else {}
        top_comment_id = top.get("id") if isinstance(top, dict) else None

        thread_rows.append(
            (
                thread_id,
                video_id,
                top_comment_id,
                _to_int(sn.get("totalReplyCount")),
                now,
            )
        )

        author_cid = None
        author = top_sn.get("authorChannelId")
        if isinstance(author, dict):
            author_cid = author.get("value")

        comment_rows.append(
            (
                top_comment_id,
                video_id,
                author_cid,
                top_sn.get("authorDisplayName"),
                top_sn.get("textDisplay") or top_sn.get("textOriginal"),
                top_sn.get("publishedAt"),
                top_sn.get("updatedAt"),
                _to_int(top_sn.get("likeCount")),
                now,
            )
        )

    conn.executemany(
        """
        insert into comment_threads(
            thread_id,
            video_id,
            top_comment_id,
            total_reply_count,
            fetched_at
        ) values(?,?,?,?,?)
        on conflict(thread_id) do update set
            top_comment_id=excluded.top_comment_id,
            total_reply_count=excluded.total_reply_count,
            fetched_at=excluded.fetched_at
        """,
        thread_rows,
    )

    # top-level comments
    conn.executemany(
        """
        insert into comments(
            comment_id,
            video_id,
            author_channel_id,
            author_display_name,
            text,
            published_at,
            updated_at,
            like_count,
            fetched_at
        ) values(?,?,?,?,?,?,?,?,?)
        on conflict(comment_id) do update set
            author_channel_id=excluded.author_channel_id,
            author_display_name=excluded.author_display_name,
            text=excluded.text,
            published_at=excluded.published_at,
            updated_at=excluded.updated_at,
            like_count=excluded.like_count,
            fetched_at=excluded.fetched_at
        """,
        comment_rows,
    )

    return len(thread_rows), len(comment_rows)


def get_known_video_ids(
    conn: sqlite3.Connection,
    channel_id: Optional[str] = None,
) -> set:
    if channel_id:
        rows = conn.execute(
            """
            select
                video_id
            from videos
            where channel_id=?
            """,
            (channel_id,)
        ).fetchall()
    else:
        rows = conn.execute(
            """
            select
                video_id
            from videos
            """
        ).fetchall()
    return {r[0] for r in rows}


def get_channel_uploads_playlist_id(
    conn: sqlite3.Connection,
    channel_id: str,
) -> Optional[str]:
    r = conn.execute(
        """
        select
            uploads_playlist_id
        from channels
        where channel_id=?
        """,
        (channel_id,)
    ).fetchone()
    if not r:
        return None
    return r[0]


def create_job(
    conn: sqlite3.Connection,
    job_type: str,
    details: Dict[str, Any],
) -> int:
    now = utc_now_iso()
    cur = conn.execute(
        """
        insert into jobs(
            job_type,
            started_at,
            status,
            details_json
        ) values(?,?,?,?)
        """,
        (job_type, now, "running", json.dumps(details, ensure_ascii=False)),
    )
    return int(cur.lastrowid)


def finish_job(
    conn: sqlite3.Connection,
    job_id: int,
    status: str,
    details: Dict[str, Any],
) -> None:
    now = utc_now_iso()
    conn.execute(
        "update jobs set ended_at=?, status=?, details_json=? where id=?",
        (now, status, json.dumps(details, ensure_ascii=False), job_id),
    )


def list_jobs(
    conn: sqlite3.Connection,
    limit: int = 50,
) -> List[JobRecord]:
    rows = conn.execute(
        """
        select
            id,
            job_type,
            started_at,
            ended_at,
            status,
            details_json
        from jobs
        order by id desc
        limit ?
        """,
        (limit,),
    ).fetchall()
    out: List[JobRecord] = []
    for r in rows:
        out.append(JobRecord(
            id=int(r["id"]),
            job_type=r["job_type"],
            started_at=r["started_at"],
            ended_at=r["ended_at"],
            status=r["status"],
            details_json=r["details_json"],
        ))
    return out


def create_search_run(
    conn: sqlite3.Connection,
    *,
    q: str,
    order_by: str,
    mode: str,
    filters: Dict[str, Any],
) -> int:
    now = utc_now_iso()
    cur = conn.execute(
        """
        insert into search_runs(
            q,
            order_by,
            mode,
            filters_json,
            collected_at
        ) values(?,?,?,?,?)
        """,
        (q, order_by, mode, json.dumps(filters, ensure_ascii=False), now),
    )
    return int(cur.lastrowid)


def insert_search_run_videos(
    conn: sqlite3.Connection,
    run_id: int,
    video_ids: List[str],
) -> int:
    if not video_ids:
        return 0
    rows = [(run_id, vid) for vid in video_ids]
    conn.executemany(
        """
        insert or ignore into search_run_videos(
            run_id,
            video_id
        ) values(?,?)
        """,
        rows,
    )
    return len(rows)


def _to_int(
    x: Any,
) -> Optional[int]:
    if x is None:
        return None
    try:
        return int(x)
    except Exception:
        return None
