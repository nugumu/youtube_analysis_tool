from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import sqlite3

from .youtube_api import YouTubeAPI, fetch_channels, fetch_videos
from . import storage


@dataclass
class CollectResult:
    channels_upserted: int = 0
    video_ids_found: int = 0
    videos_upserted: int = 0
    new_videos: int = 0
    comments_threads_upserted: int = 0
    comments_upserted: int = 0
    collected_video_ids: List[str] = None
    errors: List[str] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.collected_video_ids is None:
            self.collected_video_ids = []




def _broadcast_kind_from_item(it: Dict[str, Any]) -> str:
    sn = it.get("snippet") or {}
    lbc = sn.get("liveBroadcastContent")
    if lbc == "live":
        return "ライブ配信中"
    if lbc == "upcoming":
        return "予約/配信予定"

    ls = it.get("liveStreamingDetails") or {}
    if ls.get("actualStartTime") or ls.get("actualEndTime"):
        return "ライブアーカイブ"

    if lbc is None:
        return "判定不可"
    return "通常動画"


def _filter_video_items(items: List[Dict[str, Any]], include_kinds: Optional[List[str]]) -> List[Dict[str, Any]]:
    if not items or not include_kinds:
        return items
    inc = set(include_kinds)
    return [it for it in items if _broadcast_kind_from_item(it) in inc]


def update_channels(api: YouTubeAPI, conn: sqlite3.Connection, channel_ids: List[str]) -> Tuple[int, List[str]]:
    items, errors = fetch_channels(api, channel_ids, part="snippet,statistics,contentDetails")
    n = storage.upsert_channels(conn, items)
    return n, errors


def fetch_channel_video_ids(
    api: YouTubeAPI,
    conn: sqlite3.Connection,
    channel_id: str,
    mode: str = "diff",
    max_videos: int = 20000,
) -> Tuple[List[str], List[str]]:
    """Return video IDs from the channel uploads playlist.

    mode:
        - 'diff': stop when we see a known video_id (fast incremental)
        - 'full': traverse all pages up to max_videos
    """
    errors: List[str] = []

    uploads = storage.get_channel_uploads_playlist_id(conn, channel_id)
    if not uploads:
        # try refresh channel details (contentDetails)
        items, errs = fetch_channels(api, [channel_id], part="contentDetails")
        errors.extend(errs)
        if items:
            storage.upsert_channels(conn, items)
            uploads = storage.get_channel_uploads_playlist_id(conn, channel_id)

    if not uploads:
        errors.append(f"uploads playlist id not found for {channel_id}")
        return [], errors

    known = storage.get_known_video_ids(conn, channel_id=channel_id)

    out: List[str] = []
    page_token: Optional[str] = None
    seen_existing = False

    while True:
        res = api.playlist_items_list(
            part="contentDetails",
            playlist_id=uploads,
            max_results=50,
            page_token=page_token,
        )
        if "error" in res:
            errors.append(str(res["error"]))
            break

        items = res.get("items", [])
        if not items:
            break

        for it in items:
            cd = it.get("contentDetails", {})
            vid = cd.get("videoId")
            if not vid:
                continue
            if mode == "diff" and vid in known:
                seen_existing = True
                break
            out.append(vid)
            if len(out) >= max_videos:
                break

        if seen_existing or len(out) >= max_videos:
            break

        page_token = res.get("nextPageToken")
        if not page_token:
            break

    return out, errors


def fetch_search_video_ids(
    api: YouTubeAPI,
    conn: sqlite3.Connection,
    *,
    q: str,
    mode: str = "diff",
    order: str = "date",
    total_results: int = 200,
    region_code: Optional[str] = "JP",
    relevance_language: Optional[str] = "ja",
    safe_search: str = "none",
    video_duration: str = "any",
    video_definition: str = "any",
    video_type: str = "any",
    channel_id: Optional[str] = None,
    published_after: Optional[str] = None,
    published_before: Optional[str] = None,
) -> Tuple[List[str], List[str]]:
    """Return video IDs from a keyword search.

    mode:
      - 'diff': stop when we see a known video_id (only reliable with order='date')
      - 'full': traverse pages up to total_results
    """

    errors: List[str] = []
    if total_results < 1:
        return [], []

    total_results = min(int(total_results), 500)

    if mode == "diff" and order != "date":
        errors.append("diff mode is only reliable when order=date. Falling back to full.")
        mode = "full"

    known = storage.get_known_video_ids(conn)  # global
    out: List[str] = []
    seen: set = set()

    page_token: Optional[str] = None
    per_page = 50
    seen_existing = False

    while len(out) < total_results:
        batch_size = min(per_page, total_results - len(out))
        params: Dict[str, Any] = {
            "part": "snippet",
            "type": "video",
            "order": order,
            "maxResults": batch_size,
        }
        if q.strip():
            params["q"] = q
        if region_code:
            params["regionCode"] = region_code
        if relevance_language:
            params["relevanceLanguage"] = relevance_language
        if safe_search and safe_search != "none":
            params["safeSearch"] = safe_search
        if video_duration and video_duration != "any":
            params["videoDuration"] = video_duration
        if video_definition and video_definition != "any":
            params["videoDefinition"] = video_definition
        if video_type and video_type != "any":
            params["videoType"] = video_type
        if channel_id:
            params["channelId"] = channel_id
        if published_after:
            params["publishedAfter"] = published_after
        if published_before:
            params["publishedBefore"] = published_before
        if page_token:
            params["pageToken"] = page_token

        res = api._request("search", params, "search.list")
        if "error" in res:
            errors.append(str(res["error"]))
            break

        items = res.get("items", [])
        if not items:
            break

        for it in items:
            vid = (it.get("id") or {}).get("videoId")
            if not vid:
                continue
            if vid in seen:
                continue
            seen.add(vid)

            if mode == "diff" and vid in known:
                seen_existing = True
                break

            out.append(vid)
            if len(out) >= total_results:
                break

        if seen_existing or len(out) >= total_results:
            break

        page_token = res.get("nextPageToken")
        if not page_token:
            break

    return out, errors


def update_videos_details(
    api: YouTubeAPI,
    conn: sqlite3.Connection,
    video_ids: List[str],
    *,
    part: str = "snippet,contentDetails,statistics,liveStreamingDetails",
) -> Tuple[int, List[str]]:
    if not video_ids:
        return 0, []
    items, errors = fetch_videos(api, video_ids, part=part)
    n = storage.upsert_videos(conn, items)
    return n, errors


def collect_uploads_and_videos(
    api: YouTubeAPI,
    conn: sqlite3.Connection,
    channel_ids: List[str],
    *,
    mode: str = "diff",
    max_videos_per_channel: int = 20000,
    update_existing_videos: bool = False,
    include_broadcast_kinds: Optional[List[str]] = None,
) -> CollectResult:
    result = CollectResult()

    # Ensure channels table is populated with uploads playlist IDs
    n_channels, errs = update_channels(api, conn, channel_ids)
    result.channels_upserted += n_channels
    result.errors.extend(errs)

    all_new_video_ids: List[str] = []
    all_video_ids: List[str] = []

    for cid in channel_ids:
        vids, errs = fetch_channel_video_ids(
            api,
            conn,
            cid,
            mode=mode,
            max_videos=max_videos_per_channel,
        )
        result.errors.extend(errs)
        result.video_ids_found += len(vids)
        all_new_video_ids.extend(vids)

        if update_existing_videos:
            # get ALL known ids + new ids (bounded)
            known = list(storage.get_known_video_ids(conn, channel_id=cid))
            all_video_ids.extend(list(dict.fromkeys(vids + known))[:max_videos_per_channel])

    # Upsert video details
    part = "snippet,statistics,contentDetails,liveStreamingDetails"

    if update_existing_videos:
        target_ids = list(dict.fromkeys(all_video_ids))
        v_items, errs = fetch_videos(api, target_ids, part=part)
        result.errors.extend(errs)
        v_items = _filter_video_items(v_items, include_broadcast_kinds)
        result.collected_video_ids = [it.get("id") for it in v_items if it.get("id")]
        result.videos_upserted += storage.upsert_videos(conn, v_items) if v_items else 0
    else:
        target_ids = list(dict.fromkeys(all_new_video_ids))
        result.new_videos = len(target_ids)
        v_items, errs = fetch_videos(api, target_ids, part=part)
        result.errors.extend(errs)
        v_items = _filter_video_items(v_items, include_broadcast_kinds)
        result.collected_video_ids = [it.get('id') for it in v_items if it.get('id')]
        result.videos_upserted += storage.upsert_videos(conn, v_items) if v_items else 0

    return result


def collect_search_videos(
    api: YouTubeAPI,
    conn: sqlite3.Connection,
    *,
    q: str,
    mode: str = "diff",
    order: str = "date",
    total_results: int = 200,
    region_code: Optional[str] = "JP",
    relevance_language: Optional[str] = "ja",
    safe_search: str = "none",
    video_duration: str = "any",
    video_definition: str = "any",
    video_type: str = "any",
    channel_id: Optional[str] = None,
    published_after: Optional[str] = None,
    published_before: Optional[str] = None,
    update_existing_videos: bool = False,
    include_broadcast_kinds: Optional[List[str]] = None,
) -> CollectResult:
    """Collect videos by keyword search (channel-agnostic)."""

    result = CollectResult()

    known_before = storage.get_known_video_ids(conn)

    vids, errs = fetch_search_video_ids(
        api,
        conn,
        q=q,
        mode=mode,
        order=order,
        total_results=total_results,
        region_code=region_code,
        relevance_language=relevance_language,
        safe_search=safe_search,
        video_duration=video_duration,
        video_definition=video_definition,
        video_type=video_type,
        channel_id=channel_id,
        published_after=published_after,
        published_before=published_before,
    )
    result.errors.extend(errs)
    result.video_ids_found = len(vids)

    if not vids:
        return result

    # Decide fetch targets
    target_ids = list(dict.fromkeys(vids))
    if update_existing_videos:
        # refresh any videos that match search, even if already in DB
        to_fetch = target_ids
    else:
        to_fetch = [v for v in target_ids if v not in known_before]

    result.new_videos = len([v for v in target_ids if v not in known_before])

    if not to_fetch:
        return result

    v_part = "snippet,statistics,contentDetails,liveStreamingDetails"

    v_items, v_errs = fetch_videos(api, to_fetch, part=v_part)
    result.errors.extend(v_errs)

    v_items = _filter_video_items(v_items, include_broadcast_kinds)

    if v_items:
        result.collected_video_ids = [it.get("id") for it in v_items if it.get("id")]
        n_v = storage.upsert_videos(conn, v_items)
        result.videos_upserted += n_v

        c_ids = sorted({(it.get("snippet") or {}).get("channelId") for it in v_items if (it.get("snippet") or {}).get("channelId")})
        if c_ids:
            c_items, c_errs = fetch_channels(api, list(c_ids), part="snippet,statistics,contentDetails")
            result.errors.extend(c_errs)
            if c_items:
                result.channels_upserted += storage.upsert_channels(conn, c_items)

    return result


def collect_comments_for_videos(
    api: YouTubeAPI,
    conn: sqlite3.Connection,
    video_ids: List[str],
    *,
    max_pages_per_video: int = 5,
    order: str = "time",
) -> CollectResult:
    result = CollectResult()

    for vid in video_ids:
        threads: List[Dict[str, Any]] = []
        page_token: Optional[str] = None
        pages = 0

        while True:
            res = api.comment_threads_list(
                part="snippet",
                video_id=vid,
                max_results=100,
                page_token=page_token,
                order=order,
            )
            if "error" in res:
                result.errors.append(f"video {vid}: {res['error']}")
                break

            threads.extend(res.get("items", []))
            pages += 1
            if pages >= max_pages_per_video:
                break

            page_token = res.get("nextPageToken")
            if not page_token:
                break

        if threads:
            n_threads, n_comments = storage.upsert_comment_threads_and_comments(conn, vid, threads)
            result.comments_threads_upserted += n_threads
            result.comments_upserted += n_comments

    return result
