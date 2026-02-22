from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import List, Optional, Tuple

import pandas as pd
import sqlite3


def add_broadcast_kind(df: pd.DataFrame) -> pd.DataFrame:
    """配信状態を推定して broadcast_kind 列を追加する。

    - live_broadcast_content: snippet.liveBroadcastContent (none/live/upcoming)
    - actual_start_time / actual_end_time: liveStreamingDetails の実績（存在すればライブ由来とみなす）

    Notes:
        旧DB（列がNULL）だと判定不可になります。動画統計を更新すると埋まります。
    """

    if df.empty:
        return df

    def _kind(row):
        lbc = row.get("live_broadcast_content")
        if lbc == "live":
            return "ライブ配信中"
        if lbc == "upcoming":
            return "予約/配信予定"

        ast = row.get("actual_start_time")
        aen = row.get("actual_end_time")
        if pd.notna(ast) or pd.notna(aen):
            return "ライブアーカイブ"

        if pd.isna(lbc):
            return "判定不可"
        return "通常動画"

    out = df.copy()
    out["broadcast_kind"] = out.apply(_kind, axis=1)
    return out


@dataclass
class VideoFilters:
    channel_ids: Optional[List[str]] = None
    date_from: Optional[str] = None  # YYYY-MM-DD
    date_to: Optional[str] = None  # YYYY-MM-DD
    title_contains: Optional[str] = None
    min_views: Optional[int] = None
    max_views: Optional[int] = None
    min_duration_sec: Optional[int] = None
    max_duration_sec: Optional[int] = None
    include_shorts: Optional[bool] = (
        None  # None=all, True=only shorts, False=exclude shorts
    )
    broadcast_kinds: Optional[List[str]] = (
        None  # ["通常動画", "ライブアーカイブ", "ライブ配信中", "予約/配信予定", "判定不可"]
    )


@dataclass
class CommentFilters:
    video_ids: Optional[List[str]] = None
    channel_ids: Optional[List[str]] = None  # filter via join videos
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    text_contains: Optional[str] = None
    broadcast_kinds: Optional[List[str]] = None


def load_channels_df(conn: sqlite3.Connection) -> pd.DataFrame:
    df = pd.read_sql_query(
        "select * from channels order by (subscriber_count is null), subscriber_count desc",
        conn,
    )
    return df


def load_videos_df(conn: sqlite3.Connection, f: VideoFilters) -> pd.DataFrame:
    where = []
    params: List = []

    if f.channel_ids:
        where.append(
            "v.channel_id in ({})".format(",".join(["?"] * len(f.channel_ids)))
        )
        params.extend(f.channel_ids)

    if f.date_from:
        where.append("substr(v.published_at,1,10) >= ?")
        params.append(f.date_from)
    if f.date_to:
        where.append("substr(v.published_at,1,10) <= ?")
        params.append(f.date_to)

    if f.title_contains:
        where.append("v.title like ?")
        params.append(f"%{f.title_contains}%")

    if f.min_views is not None:
        where.append("coalesce(v.view_count,0) >= ?")
        params.append(int(f.min_views))
    if f.max_views is not None:
        where.append("coalesce(v.view_count,0) <= ?")
        params.append(int(f.max_views))

    if f.min_duration_sec is not None:
        where.append("coalesce(v.duration_seconds,0) >= ?")
        params.append(int(f.min_duration_sec))
    if f.max_duration_sec is not None:
        where.append("coalesce(v.duration_seconds,0) <= ?")
        params.append(int(f.max_duration_sec))

    sql = """
    SELECT v.*, c.title AS channel_title
    FROM videos v
    LEFT JOIN channels c ON c.channel_id = v.channel_id
    """.strip()
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY v.published_at DESC"

    df = pd.read_sql_query(sql, conn, params=params)

    if df.empty:
        return df

    df["published_date"] = pd.to_datetime(df["published_at"], errors="coerce").dt.date
    df["duration_min"] = (df["duration_seconds"].fillna(0) / 60.0).round(2)
    df["is_short"] = df["duration_seconds"].fillna(0).astype(int) <= 60

    df = add_broadcast_kind(df)
    if f.broadcast_kinds:
        df = df[df["broadcast_kind"].isin(list(f.broadcast_kinds))]

    if f.include_shorts is True:
        df = df[df["is_short"]]
    elif f.include_shorts is False:
        df = df[~df["is_short"]]

    return df


def load_comments_df(conn: sqlite3.Connection, f: CommentFilters) -> pd.DataFrame:
    where = []
    params: List = []

    if f.channel_ids:
        where.append(
            "v.channel_id in ({})".format(",".join(["?"] * len(f.channel_ids)))
        )
        params.extend(f.channel_ids)

    if f.video_ids:
        where.append("c.video_id in ({})".format(",".join(["?"] * len(f.video_ids))))
        params.extend(f.video_ids)

    if f.date_from:
        where.append("substr(c.published_at,1,10) >= ?")
        params.append(f.date_from)
    if f.date_to:
        where.append("substr(c.published_at,1,10) <= ?")
        params.append(f.date_to)

    if f.text_contains:
        where.append("c.text like ?")
        params.append(f"%{f.text_contains}%")

    sql = """
    select
        c.*
        ,v.channel_id
        ,v.title as video_title
        ,ch.title as channel_title
        ,v.live_broadcast_content
        ,v.scheduled_start_time
        ,v.actual_start_time
        ,v.actual_end_time
    from comments c
    join videos v
        on v.video_id = c.video_id
    left join channels ch
        on ch.channel_id = v.channel_id
    """.strip()
    if where:
        sql += " where " + " and ".join(where)
    sql += " order by c.published_at desc"

    df = pd.read_sql_query(sql, conn, params=params)
    if not df.empty:
        df["published_date"] = pd.to_datetime(
            df["published_at"], errors="coerce"
        ).dt.date
        df = add_broadcast_kind(df)
        if f.broadcast_kinds:
            df = df[df["broadcast_kind"].isin(list(f.broadcast_kinds))]
    return df


def summarize_by_day(
    df_videos: pd.DataFrame, metric: str = "view_count"
) -> pd.DataFrame:
    if df_videos.empty:
        return pd.DataFrame()
    tmp = df_videos.copy()
    tmp["day"] = pd.to_datetime(tmp["published_at"], errors="coerce").dt.date
    out = (
        tmp.groupby("day")
        .agg(videos=("video_id", "count"), views=(metric, "sum"))
        .reset_index()
        .sort_values("day")
    )
    return out


def load_channel_snapshots_df(
    conn: sqlite3.Connection, limit: int = 1000
) -> pd.DataFrame:
    """チャンネル名を結合したチャンネルスナップショット。"""
    sql = f"""
    select
        cs.id
        ,cs.captured_at
        ,cs.channel_id
        ,ch.title as channel_title
        ,cs.view_count
        ,cs.subscriber_count
        ,cs.video_count
    from channel_snapshots cs
    left join channels ch
        on ch.channel_id = cs.channel_id
    order by cs.captured_at desc
    limit {int(limit)}
    """.strip()
    return pd.read_sql_query(sql, conn)


def load_video_snapshots_df(
    conn: sqlite3.Connection, limit: int = 1000
) -> pd.DataFrame:
    """動画タイトル/チャンネル名を結合した動画スナップショット。"""
    sql = f"""
    select
        vs.id
        ,vs.captured_at
        ,vs.video_id
        ,v.title as video_title
        ,v.channel_id
        ,ch.title as channel_title
        ,vs.view_count
        ,vs.like_count
        ,vs.comment_count
    from video_snapshots vs
    left join videos v
        on v.video_id = vs.video_id
    left join channels ch
        on ch.channel_id = v.channel_id
    order by vs.captured_at desc
    limit {int(limit)}
    """.strip()
    return pd.read_sql_query(sql, conn)
