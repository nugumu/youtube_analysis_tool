from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests


BASE_URL = "https://www.googleapis.com/youtube/v3"

# Quota costs (common ones). See docs for full mapping.
# search.list is 100 by default.
QUOTA_COST = {
    "channels.list": 1,
    "videos.list": 1,
    "playlistItems.list": 1,
    "commentThreads.list": 1,
    "comments.list": 1,
    "channelSections.list": 1,
    "search.list": 100,
}


@dataclass
class ApiStats:
    calls: int = 0
    quota_units: int = 0
    last_error: Optional[str] = None


class YouTubeAPI:
    def __init__(
        self, api_key: str,
        user_agent: str = "yt-stats-tool/1.0",
        timeout: int = 30,
    ):
        self.api_key = api_key
        self.timeout = timeout
        self.sess = requests.Session()
        self.sess.headers.update({"User-Agent": user_agent})
        self.stats = ApiStats()

    def _request(
        self,
        path: str,
        params: Dict[str, Any],
        quota_key: str,
        retries: int = 5,
    ) -> Dict[str, Any]:
        url = f"{BASE_URL}/{path}"
        p = dict(params)
        p["key"] = self.api_key

        cost = QUOTA_COST.get(quota_key, 1)

        backoff = 1.0
        for attempt in range(retries + 1):
            try:
                r = self.sess.get(url, params=p, timeout=self.timeout)
                self.stats.calls += 1
                self.stats.quota_units += cost

                if r.status_code == 200:
                    self.stats.last_error = None
                    return r.json()

                # Retry on transient errors / quota-ish / rate limiting
                if r.status_code in (429, 500, 502, 503, 504):
                    msg = f"HTTP {r.status_code}: {r.text[:200]}"
                    self.stats.last_error = msg
                    if attempt < retries:
                        time.sleep(backoff)
                        backoff = min(backoff * 2, 16)
                        continue

                # Non-retryable
                self.stats.last_error = f"HTTP {r.status_code}: {r.text[:500]}"
                return {"error": {"http_status": r.status_code, "message": r.text}}

            except requests.RequestException as e:
                self.stats.calls += 1
                self.stats.quota_units += cost
                self.stats.last_error = str(e)
                if attempt < retries:
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 16)
                    continue
                return {"error": {"http_status": None, "message": str(e)}}

        return {"error": {"http_status": None, "message": "Unknown error"}}

    # ---------- high-level helpers ----------

    def channels_list(
        self,
        *,
        part: str,
        ids: Optional[List[str]] = None,
        for_username: Optional[str] = None,
        for_handle: Optional[str] = None,
        max_results: int = 50,
        page_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"part": part, "maxResults": max_results}
        if ids:
            params["id"] = ",".join(ids)
        if for_username:
            params["forUsername"] = for_username
        if for_handle:
            params["forHandle"] = for_handle
        if page_token:
            params["pageToken"] = page_token
        return self._request("channels", params, "channels.list")

    def videos_list(self, *, part: str, ids: List[str], max_results: int = 50) -> Dict[str, Any]:
        params: Dict[str, Any] = {"part": part, "id": ",".join(ids), "maxResults": max_results}
        return self._request("videos", params, "videos.list")

    def playlist_items_list(self, *, part: str, playlist_id: str, max_results: int = 50,
                            page_token: Optional[str] = None) -> Dict[str, Any]:
        params: Dict[str, Any] = {"part": part, "playlistId": playlist_id, "maxResults": max_results}
        if page_token:
            params["pageToken"] = page_token
        return self._request("playlistItems", params, "playlistItems.list")

    def comment_threads_list(
        self,
        *,
        part: str,
        video_id: str,
        max_results: int = 100,
        page_token: Optional[str] = None,
        order: str = "time",
        text_format: str = "plainText",
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "part": part,
            "videoId": video_id,
            "maxResults": max_results,
            "order": order,
            "textFormat": text_format,
        }
        if page_token:
            params["pageToken"] = page_token
        return self._request("commentThreads", params, "commentThreads.list")

    def search_list(
        self,
        *,
        part: str,
        q: str,
        type_: str = "channel",
        max_results: int = 5,
        page_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"part": part, "q": q, "type": type_, "maxResults": max_results}
        if page_token:
            params["pageToken"] = page_token
        return self._request("search", params, "search.list")

    def search_videos(
        self,
        *,
        q: str,
        total_results: int = 50,
        order: str = "date",
        region_code: Optional[str] = "JP",
        relevance_language: Optional[str] = "ja",
        safe_search: str = "none",
        video_duration: str = "any",
        video_definition: str = "any",
        video_type: str = "any",
        channel_id: Optional[str] = None,
        published_after: Optional[str] = None,
        published_before: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """search.list を使って動画を検索して返す。

        Notes:
            - search.list は 100 quota/回。
            - 50件/ページでページング。
            - 安全のため、デフォルト実装では最大500件までに制限。

        Returns:
            List[dict] with keys: video_id, title, channel_title, channel_id, published_at, description, url, embed_url
        """

        if total_results < 1:
            return []

        total_results = min(int(total_results), 500)
        per_page = 50
        items: List[Dict[str, Any]] = []
        page_token: Optional[str] = None

        while len(items) < total_results:
            batch_size = min(per_page, total_results - len(items))
            params: Dict[str, Any] = {
                "part": "snippet",
                "type": "video",
                "q": q,
                "order": order,
                "maxResults": batch_size,
            }

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

            res = self._request("search", params, "search.list")
            if "error" in res:
                raise RuntimeError(str(res["error"]))

            for it in res.get("items", []):
                vid = (it.get("id") or {}).get("videoId")
                sn = it.get("snippet") or {}
                if not vid:
                    continue
                items.append(
                    {
                        "video_id": vid,
                        "title": sn.get("title") or "",
                        "channel_title": sn.get("channelTitle") or "",
                        "channel_id": sn.get("channelId") or "",
                        "published_at": sn.get("publishedAt") or "",
                        "description": sn.get("description") or "",
                        "url": f"https://www.youtube.com/watch?v={vid}",
                        "embed_url": f"https://www.youtube.com/embed/{vid}",
                    }
                )

            page_token = res.get("nextPageToken")
            if not page_token:
                break

        return items

    def search_videos_newest(
        self,
        *,
        q: str,
        total_results: int = 50,
        region_code: Optional[str] = "JP",
        relevance_language: Optional[str] = "ja",
        safe_search: str = "none",
        video_duration: str = "any",
        video_definition: str = "any",
        video_type: str = "any",
        channel_id: Optional[str] = None,
        published_after: Optional[str] = None,
        published_before: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """search.list を使って動画を検索し、アップロード日時の新しい順で返す。

        Notes:
            - search.list は 100 quota/回。
            - 50件/ページでページング。最大500件まで。
            - ライブ配信は対象外想定のため eventType は指定しない。
        """

        return self.search_videos(
            q=q,
            total_results=total_results,
            order="date",
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


# ---------- convenience batch wrappers ----------


def batched(
    seq: List[str],
    n: int,
) -> List[List[str]]:
    return [seq[i:i + n] for i in range(0, len(seq), n)]


def fetch_channels(
    api: YouTubeAPI,
    channel_ids: List[str],
    part: str,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    items: List[Dict[str, Any]] = []
    errors: List[str] = []
    for batch in batched(channel_ids, 50):
        res = api.channels_list(part=part, ids=batch)
        if "error" in res:
            errors.append(str(res["error"]))
            continue
        items.extend(res.get("items", []))
    return items, errors


def fetch_videos(
    api: YouTubeAPI,
    video_ids: List[str],
    part: str,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    items: List[Dict[str, Any]] = []
    errors: List[str] = []
    for batch in batched(video_ids, 50):
        res = api.videos_list(part=part, ids=batch)
        if "error" in res:
            errors.append(str(res["error"]))
            continue
        items.extend(res.get("items", []))
    return items, errors
