from __future__ import annotations

import re
from typing import Dict, List, Optional, Tuple

import streamlit as st


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
        with col6:
            video_type = st.selectbox("種別（videoType）", ["any", "episode", "movie"], index=0)

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


def _truncate(s: str, n: int) -> str:
    return s if len(s) <= n else s[: n - 1] + "…"


def _escape_md(s: str) -> str:
    # Escape square brackets in markdown link text
    return s.replace("[", "［").replace("]", "］")


def _looks_like_rfc3339(s: str) -> bool:
    return bool(_RFC3339_RE.match(s))
