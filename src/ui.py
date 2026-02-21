from __future__ import annotations

import re
import pandas as pd
from typing import Dict, List, Optional, Tuple
import streamlit as st

from src import analysis


# RFC3339 like: 2024-01-01T00:00:00Z or with fractional seconds
_RFC3339_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$")


def top_search_bar(default_api_key: str = "") -> Tuple[str, str, bool]:
    """A Tableau-like top bar: API key + query + submit.

    Returns:
        (api_key, query, submitted)
    """
    col1, col2, col3 = st.columns([2, 4, 1], vertical_alignment="bottom")
    with col1:
        api_key = st.text_input(
            "APIキー",
            type="password",
            value=default_api_key or "",
            placeholder="AIza...（この入力値は保存しません）",
            help="このアプリはAPIキーをローカルに保存しません（セッション内のみ）。",
        )
    with col2:
        query = st.text_input("検索ワード", placeholder="例: VTuber 統計 分析")
    with col3:
        submitted = st.button("検索", type="primary", use_container_width=True)

    return api_key, query, submitted


def advanced_search_filters_expander() -> Dict[str, object]:
    """Advanced filters for search.list.

    Notes:
        - Keep it aligned with YouTubeAPI.search_videos_newest arguments.
        - We intentionally do not expose eventType to keep it 'non-live' oriented.
    """
    with st.expander("検索条件（詳細）", expanded=False):
        total_results = st.slider("取得件数（最大500）", min_value=10, max_value=500, value=50, step=10)

        col1, col2, col3 = st.columns(3)
        with col1:
            region_code = st.text_input("regionCode（任意）", value="JP", help="例: JP, US（空欄可）").strip() or None
            channel_id = st.text_input("channelId（任意）", value="", help="特定チャンネル内検索（空欄可）").strip() or None
        with col2:
            relevance_language = st.text_input(
                "relevanceLanguage（任意）",
                value="ja",
                help="例: ja, en（空欄可）",
            ).strip() or None
            safe_search = st.selectbox("safeSearch", ["none", "moderate", "strict"], index=0)
        with col3:
            video_duration = st.selectbox("videoDuration", ["any", "short", "medium", "long"], index=0)
            video_definition = st.selectbox("videoDefinition", ["any", "high", "standard"], index=0)

        col4, col5, col6 = st.columns(3)
        with col4:
            video_type = st.selectbox("videoType", ["any", "episode", "movie"], index=0)
        with col5:
            st.write("")
        with col6:
            st.write("")

        st.markdown("**期間指定** 例: `2024-01-01T00:00:00Z`")
        col7, col8 = st.columns(2)
        with col7:
            published_after = st.text_input("publishedAfter（任意）", value="", help="指定した日時以降（空欄可）").strip() or None
            if published_after and not _looks_like_rfc3339(published_after):
                st.warning("publishedAfter の形式が正しくありません（例: 2024-01-01T00:00:00Z）")
        with col8:
            published_before = st.text_input("publishedBefore（任意）", value="", help="指定した日時以前（空欄可）").strip() or None
            if published_before and not _looks_like_rfc3339(published_before):
                st.warning("publishedBefore の形式が正しくありません（例: 2024-01-01T00:00:00Z）")

    return {
        "total_results": int(total_results),
        "region_code": region_code,
        "relevance_language": relevance_language,
        "safe_search": safe_search,
        "video_duration": video_duration,
        "video_definition": video_definition,
        "video_type": video_type,
        "channel_id": channel_id,
        "published_after": published_after,
        "published_before": published_before,
    }


def advanced_search_collect_filters_expander() -> Dict[str, object]:
    """Advanced filters for *collection* based on search.list.

    Differences from advanced_search_filters_expander:
        - exposes `order`
        - adds `exclude_live` (applied after videos.list via snippet.liveBroadcastContent)
    """

    with st.expander("収集条件（検索）", expanded=True):
        total_results = st.slider("最大取得件数（最大500）", min_value=10, max_value=500, value=200, step=10)

        col1, col2, col3 = st.columns(3)
        with col1:
            order_opts = ["date", "relevance", "viewCount", "rating", "title"]
            order_label = {
                "date": "公開日（新しい順）",
                "relevance": "関連度",
                "viewCount": "再生数",
                "rating": "評価",
                "title": "タイトル",
            }
            order = st.selectbox(
                "並び順（order）",
                order_opts,
                index=0,
                format_func=lambda x: order_label.get(x, x),
                help="diff収集は『公開日（新しい順）』のときのみ有効です。",
            )
            video_type = st.selectbox("種別（videoType）", ["any", "episode", "movie"], index=0)
        with col2:
            region_code = st.text_input("国コード（regionCode, 任意）", value="JP").strip() or None
            relevance_language = st.text_input("言語（relevanceLanguage, 任意）", value="ja").strip() or None
        with col3:
            safe_search = st.selectbox("セーフサーチ（safeSearch）", ["none", "moderate", "strict"], index=0)
            channel_id = st.text_input("チャンネルIDで絞る（channelId, 任意）", value="").strip() or None

        col4, col5, col6 = st.columns(3)
        with col4:
            video_duration = st.selectbox("動画長（videoDuration）", ["any", "short", "medium", "long"], index=0)
        with col5:
            video_definition = st.selectbox("画質（videoDefinition）", ["any", "high", "standard"], index=0)

        st.markdown("**期間指定**（RFC3339）例: `2024-01-01T00:00:00Z`")
        col7, col8 = st.columns(2)
        with col7:
            published_after = st.text_input("開始（publishedAfter, 任意）", value="").strip() or None
            if published_after and not _looks_like_rfc3339(published_after):
                st.warning("publishedAfter の形式が正しくありません（例: 2024-01-01T00:00:00Z）")
        with col8:
            published_before = st.text_input("終了（publishedBefore, 任意）", value="").strip() or None
            if published_before and not _looks_like_rfc3339(published_before):
                st.warning("publishedBefore の形式が正しくありません（例: 2024-01-01T00:00:00Z）")

    return {
        "total_results": int(total_results),
        "order": str(order),
        "region_code": region_code,
        "relevance_language": relevance_language,
        "safe_search": safe_search,
        "video_duration": video_duration,
        "video_definition": video_definition,
        "video_type": video_type,
        "channel_id": channel_id,
        "published_after": published_after,
        "published_before": published_before,
    }


def render_video_cards(results: List[dict]) -> None:
    """Render search results as cards, similar to the reference sorter UI."""
    st.divider()
    st.subheader(f"検索結果（新しい順）: {len(results)}件")

    for r in results:
        title = (r.get("title") or "").strip()
        url = r.get("url") or ""
        embed_url = r.get("embed_url") or ""
        channel_title = (r.get("channel_title") or "").strip()
        published_at = (r.get("published_at") or "").strip()
        description = (r.get("description") or "").strip().replace("\n", " ")

        with st.container(border=True):
            left, right = st.columns([2, 3], vertical_alignment="top")

            with left:
                if embed_url:
                    st.components.v1.iframe(embed_url, height=220, scrolling=False)
                else:
                    st.write("")

            with right:
                if title and url:
                    st.markdown(f"### [{_escape_md(title)}]({url})")
                elif title:
                    st.markdown(f"### {_escape_md(title)}")

                meta = " / ".join([x for x in [channel_title, published_at] if x])
                if meta:
                    st.caption(meta)

                if description:
                    st.write(_truncate(description, 180))

def video_filter_panel(
    ch_df: pd.DataFrame,
    labeler,
    *,
    key_prefix: str,
    show_top_n: bool = False,
    show_view_range: bool = True,
    show_duration_max: bool = True,
    show_duration_min: bool = True,
) -> Tuple[analysis.VideoFilters, Optional[int]]:
    all_ids = ch_df["channel_id"].astype(str).tolist()

    # Row 1
    r1 = st.columns([1, 1])
    with r1[0]:
        try:
            channel_ids = st.multiselect(
                "チャンネル（複数可）",
                options=all_ids,
                format_func=labeler,
                default=[],
                placeholder="チャンネル名で検索",
                key=f"{key_prefix}_channels",
            )
        except TypeError:
            channel_ids = st.multiselect(
                "チャンネル（複数可）",
                options=all_ids,
                format_func=labeler,
                default=[],
                key=f"{key_prefix}_channels",
            )
    with r1[1]:
        title_contains = st.text_input("タイトル（部分一致）", value="", key=f"{key_prefix}_title")

    # Row 2
    r2 = st.columns([1, 1, 1, 1,])
    with r2[0]:
        date_from = st.text_input("公開日（開始 YYYY-MM-DD）", value="", key=f"{key_prefix}_from")
    with r2[1]:
        date_to = st.text_input("公開日（終了 YYYY-MM-DD）", value="", key=f"{key_prefix}_to")
    with r2[2]:
        bc = st.selectbox(
            "配信状態",
            ["すべて", "通常動画", "ライブアーカイブ", "ライブ配信中", "予約/配信予定", "判定不可"],
            index=0,
            key=f"{key_prefix}_bc",
        )
    with r2[3]:
        shorts = st.selectbox("ショート動画", ["すべて", "Shortsのみ", "Shorts除外"], index=0, key=f"{key_prefix}_shorts")

    min_views = max_views = dur_max = None

    # Row 3（閲覧用オプション）
    if show_view_range or show_duration_max or show_duration_min:
        r3 = st.columns([1, 1, 1, 1])
        with r3[0]:
            if show_view_range:
                min_views = st.number_input("最小再生数", min_value=0, value=0, step=100, key=f"{key_prefix}_minv")
        with r3[1]:
            if show_view_range:
                max_views = st.number_input("最大再生数（0=無制限）", min_value=0, value=0, step=100, key=f"{key_prefix}_maxv")
        with r3[2]:
            if show_duration_min:
                dur_min = st.number_input("最小長（秒, 0=無制限）", min_value=0, value=0, step=10, key=f"{key_prefix}_min_dur")
        with r3[3]:
            if show_duration_max:
                dur_max = st.number_input("最大長（秒, 0=無制限）", min_value=0, value=0, step=10, key=f"{key_prefix}_max_dur")
    # Row 4（閲覧用オプション）
    top_n = None
    if show_top_n:
        r4 = st.columns([1, 1, 1, 1])
        with r4[0]:
            top_n = st.number_input("上位N", min_value=5, max_value=100, value=20, step=5, key=f"{key_prefix}_topn")

    f = analysis.VideoFilters(
        channel_ids=(channel_ids or None),
        date_from=(date_from.strip() or None),
        date_to=(date_to.strip() or None),
        title_contains=(title_contains.strip() or None),
        min_views=(int(min_views) if min_views else None),
        max_views=(int(max_views) if max_views else None),
        max_duration_sec=(int(dur_max) if dur_max else None),
        min_duration_sec=(int(dur_min) if dur_max else None),
        include_shorts=(True if shorts == "Shortsのみ" else False if shorts == "Shorts除外" else None),
        broadcast_kinds=(None if bc == "すべて" else [bc]),
    )
    return f, (int(top_n) if top_n is not None else None)


def _truncate(s: str, n: int) -> str:
    return s if len(s) <= n else s[: n - 1] + "…"


def _escape_md(s: str) -> str:
    # Escape square brackets in markdown link text
    return s.replace("[", "［").replace("]", "］")


def _looks_like_rfc3339(s: str) -> bool:
    return bool(_RFC3339_RE.match(s))
