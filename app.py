from __future__ import annotations

import json
from pathlib import Path
from typing import List, Optional

import pandas as pd
import streamlit as st

from src.config import CFG
from src.utils import parse_inputs, slugify, ensure_dir, safe_filename
from src.youtube_api import YouTubeAPI
from src import storage
from src import collector
from src import analysis
from src import charts
from src import artifacts
from src import ui


st.set_page_config(page_title=CFG.app_name, layout="wide")


def _make_channel_labeler(ch_df: pd.DataFrame):
    """Return a formatter that shows channel title (and a short suffix if duplicated)."""
    if ch_df is None or ch_df.empty:
        return lambda x: x
    m = dict(zip(ch_df["channel_id"].astype(str), ch_df["title"].fillna("").astype(str)))
    titles = pd.Series(list(m.values())).fillna("")
    counts = titles.value_counts().to_dict()

    def f(cid: str) -> str:
        cid = str(cid)
        t = (m.get(cid) or "").strip() or cid
        if counts.get(t, 0) > 1 and cid.startswith("UC"):
            return f"{t}（…{cid[-6:]}）"
        return t

    return f


def _list_projects(data_root: Path) -> List[str]:
    if not data_root.exists():
        return []
    return sorted([p.stem for p in data_root.glob("*.db")])


def _ctx_ready() -> bool:
    return bool(st.session_state.get("api_key")) and bool(st.session_state.get("project_slug"))


def _get_db_path() -> Path:
    return Path(st.session_state["db_path"])


def _get_outputs_root() -> Path:
    return Path(st.session_state.get("outputs_root", str(CFG.outputs_root)))


def _get_api() -> YouTubeAPI:
    api = st.session_state.get("api_client")
    if api is None or api.api_key != st.session_state.get("api_key"):
        api = YouTubeAPI(st.session_state.get("api_key"))
        st.session_state["api_client"] = api
    return api


def _open_conn():
    db_path = _get_db_path()
    storage.init_db(db_path)
    return storage.connect(db_path)


def page_setup():
    st.title(CFG.app_name)

    with st.sidebar:
        st.header("セットアップ")
        api_key = st.text_input("YouTube Data API v3 APIキー", type="password", value=st.session_state.get("api_key", ""))
        st.caption("APIキーはセッション内でのみ保持します（ローカル保存しません）。")

        outputs_root = st.text_input("出力フォルダ", value=st.session_state.get("outputs_root", str(CFG.outputs_root)))

        st.divider()
        st.subheader("プロジェクト")
        existing = _list_projects(CFG.data_root)
        selected = st.selectbox("既存プロジェクト", options=[""] + existing, index=0)
        new_name = st.text_input("新規作成（任意）", value="")

        if st.button("適用", use_container_width=True):
            if api_key:
                st.session_state["api_key"] = api_key.strip()
            st.session_state["outputs_root"] = outputs_root.strip() or str(CFG.outputs_root)

            if new_name.strip():
                slug = slugify(new_name.strip())
            elif selected:
                slug = selected
            else:
                slug = ""

            if slug:
                ensure_dir(CFG.data_root)
                db_path = CFG.data_root / f"{slug}.db"
                storage.init_db(db_path)
                st.session_state["project_slug"] = slug
                st.session_state["db_path"] = str(db_path)
                st.success(f"アクティブ: {slug}")
            else:
                st.warning("既存プロジェクトを選ぶか、新規作成してください。")

    st.markdown(
        """
このアプリは **YouTube Data API v3** を利用したデータ収集 & 分析ツールです。

- チャンネル/動画/コメント（トップコメント）を **SQLite（ローカルDB）** に保存
- フィルタ付きプレビュー → **CSV出力**
- 基本統計とグラフ → **HTML保存**

注意:
- `/@ハンドル` 形式も扱えますが、チャンネルID（`UC...`）または `/channel/UC...` を含むURLで検索することを推奨します。
- APIが返す統計値は **取得時点のスナップショット** です。時系列が欲しい場合は **スナップショット** を定期実行してください。
- コメントは件数が膨大になり得るため、ページ上限/サンプリング前提です。
"""
    )

    if _ctx_ready():
        st.info(f"準備OK: DB=`{st.session_state['db_path']}`")


def page_collect_by_search():
    st.header("データ収集")
    st.info("日時は、YouTube Data APIの値に従い基本的にUTC（末尾Z）です。日本時間（JST）にするには +9時間してください。")

    if not _ctx_ready():
        st.warning("先に『セットアップ』でAPIキーとプロジェクトを設定してください。")
        return

    api = _get_api()

    st.subheader("1) 収集対象")
    target_mode = st.radio(
        "収集の入口",
        options=["検索条件", "チャンネル指定"],
        horizontal=True,
        index=0,
    )

    use_search = target_mode == "検索条件"
    use_channels = target_mode == "チャンネル指定"

    # ---- search conditions ----
    q = ""
    filters = {}
    allow_blank_q = False
    search_mode = "diff"
    if use_search:
        st.subheader("2) 検索条件")
        q = st.text_input("検索ワード（任意）", value=st.session_state.get("last_collect_query", ""))
        filters = ui.advanced_search_collect_filters_expander()
        with st.expander("検索条件の収集設定", expanded=True):
            search_mode_label = st.selectbox(
                "収集モード（検索）",
                options=["差分（diff）", "全件（full）"],
                index=0,
                help="差分は order=date のとき、既知video_idが出た時点で止める近似です。全件は取得件数まで走査します。",
            )
            search_mode = "diff" if "diff" in search_mode_label else "full"
        if not q.strip():
            allow_blank_q = st.checkbox(
                "検索ワードが空でも実行する（結果が広すぎる可能性あり）",
                value=False,
            )

    # ---- channel inputs ----
    parsed_channels = None
    channel_ids: List[str] = []
    max_videos = 5000
    channel_mode = "diff"
    if use_channels:
        st.subheader("2) チャンネル指定")
        raw = st.text_area(
            "チャンネルID/URLを貼り付け（1行1つ）",
            height=160,
            placeholder="UCxxxxxxxx...\nhttps://www.youtube.com/channel/UCxxxxxxxx...\nhttps://www.youtube.com/@handle",
        )
        parsed_channels = parse_inputs(raw.splitlines())
        channel_ids = parsed_channels.channel_ids
        st.write("検出結果（チャンネルID）:")
        st.code("\n".join(channel_ids) if channel_ids else "（検出なし）", language="text")

        if parsed_channels.handles:
            if st.button("ハンドル（/@...）をチャンネルIDに変換"):
                resolved: List[str] = []
                errs: List[str] = []
                for h in parsed_channels.handles:
                    res = api.channels_list(part="snippet", for_handle=f"@{h}")
                    if "error" in res:
                        errs.append(f"@{h}: {res['error']}")
                        continue
                    items = res.get("items", [])
                    if items and items[0].get("id"):
                        resolved.append(items[0]["id"])
                resolved = list(dict.fromkeys(resolved))
                if resolved:
                    st.success(f"解決できたチャンネルID: {len(resolved)}件")
                    st.code("\n".join(resolved), language="text")
                    channel_ids = list(dict.fromkeys(channel_ids + resolved))
                if errs:
                    st.error("\n".join(errs[:10]))

        if parsed_channels.custom_usernames:
            st.warning("/c/ や /user/ は Data API v3 だけで確実にチャンネルIDへ変換できません（必要ならチャンネルIDを貼ってください）。")

        with st.expander("チャンネル指定の収集設定", expanded=True):
            channel_mode_label = st.selectbox(
                "収集モード（チャンネル）",
                options=["差分（diff）", "全件（full）"],
                index=0,
                help="差分は『既知video_idが出たら停止』の近似です（高速）。全件はuploadsプレイリストを最後まで辿ります。",
            )
            channel_mode = "diff" if "diff" in channel_mode_label else "full"
            max_videos = st.number_input(
                "チャンネルあたり最大動画数",
                min_value=50,
                max_value=100000,
                value=5000,
                step=50,
            )

    st.subheader("3) 共通設定")
    update_existing = st.checkbox("既存動画も更新（タイトル/概要欄/統計）", value=False)

    kind_opts = ["通常動画", "ライブアーカイブ", "ライブ配信中", "予約/配信予定"]
    include_kinds = st.multiselect(
        "動画の配信状態（フィルタ）",
        options=kind_opts,
        default=["通常動画"],
        help="『ライブアーカイブ』判定は liveStreamingDetails の actualStart/End を使います。旧データは判定不可のことがあるので、必要なら『既存動画も更新』で補完してください。",
    )
    include_kinds = include_kinds or kind_opts

    with st.expander("4) コメント収集（任意）", expanded=False):
        fetch_comments = st.checkbox("コメントも取得（トップレベルのみ）", value=False)

        comment_order_label = st.selectbox(
            "コメントの並び順",
            options=["新しい順", "人気順"],
            index=0,
            help="YouTube Data APIの order=time / relevance に対応します。",
        )
        comment_order = "time" if comment_order_label == "新しい順" else "relevance"

        comment_pages = st.number_input(
            "取得ページ上限/動画（1ページ=最大100件）",
            min_value=1,
            max_value=20,
            value=5,
            step=1,
            help="例：5ページなら最大500件/動画（トップレベルのみ）。",
        )

        comment_max_videos = st.number_input(
            "コメント取得の対象動画数上限（0=制限なし）",
            min_value=0,
            max_value=50000,
            value=500,
            step=50,
            help="動画数×ページ上限だけAPI呼び出しが増えるため、暴走防止に推奨。",
        )

    st.caption("クォータ目安（概算）")
    est_rows = []
    if use_search:
        pages = (int(filters.get("total_results", 0)) + 49) // 50
        est_rows.append({"入口": "検索", "API": "search.list", "推定units": int(pages * 100)})
        est_rows.append({"入口": "検索", "API": "videos.list", "推定units": int((int(filters.get("total_results", 0)) + 49) // 50)})
    if use_channels:
        n_ch = len(channel_ids)
        pages = (int(max_videos) + 49) // 50
        # playlistItems.list + videos.list を「最悪ケース」で概算（diffは実際もっと少ないことが多い）
        est_rows.append({"入口": "チャンネル", "API": "playlistItems.list", "推定units": int(n_ch * pages)})
        est_rows.append({"入口": "チャンネル", "API": "videos.list", "推定units": int(n_ch * pages)})
    if est_rows:
        st.table(pd.DataFrame(est_rows))

    disabled = False
    if use_search and (not q.strip()) and (not allow_blank_q):
        disabled = True
    if use_channels and (not channel_ids):
        disabled = True
    if not (use_search or use_channels):
        disabled = True

    if st.button("収集を実行", type="primary", use_container_width=True, disabled=disabled):
        conn = _open_conn()
        try:
            st.session_state["last_collect_query"] = q.strip()

            job_id = storage.create_job(conn, "collect_conditions", {
                "target_mode": target_mode,
                "q": q.strip(),
                "filters": filters,
                "channel_ids": channel_ids,
                "search_mode": search_mode if use_search else None,
                "channel_mode": channel_mode if use_channels else None,
                "max_videos_per_channel": int(max_videos) if use_channels else None,
                "update_existing": bool(update_existing),
                "include_broadcast_kinds": list(include_kinds) if include_kinds else None,
            })
            conn.commit()

            res_search = None
            res_channel = None

            with st.spinner("収集中..."):
                if use_search:
                    res_search = collector.collect_search_videos(
                        api,
                        conn,
                        q=q.strip(),
                        mode=search_mode,
                        order=str(filters["order"]),
                        total_results=int(filters["total_results"]),
                        region_code=filters["region_code"],
                        relevance_language=filters["relevance_language"],
                        safe_search=str(filters["safe_search"]),
                        video_duration=str(filters["video_duration"]),
                        video_definition=str(filters["video_definition"]),
                        video_type=str(filters["video_type"]),
                        channel_id=filters["channel_id"],
                        published_after=filters["published_after"],
                        published_before=filters["published_before"],
                        update_existing_videos=bool(update_existing),
                        include_broadcast_kinds=list(include_kinds) if include_kinds else None,
                    )

                if use_channels:
                    res_channel = collector.collect_uploads_and_videos(
                        api,
                        conn,
                        channel_ids,
                        mode=channel_mode,
                        max_videos_per_channel=int(max_videos),
                        update_existing_videos=bool(update_existing),
                        include_broadcast_kinds=list(include_kinds) if include_kinds else None,
                    )

                conn.commit()

            res_comments = None
            if fetch_comments:
                vids_for_comments = []

                if res_search is not None:
                    vids_for_comments += (res_search.collected_video_ids or [])
                if res_channel is not None:
                    vids_for_comments += (res_channel.collected_video_ids or [])

                # 重複除去・空除去
                vids_for_comments = list(dict.fromkeys([v for v in vids_for_comments if v]))

                # 上限適用
                if comment_max_videos and int(comment_max_videos) > 0:
                    vids_for_comments = vids_for_comments[: int(comment_max_videos)]

                if vids_for_comments:
                    job2 = storage.create_job(conn, "collect_comments", {
                        "video_count": len(vids_for_comments),
                        "max_pages_per_video": int(comment_pages),
                        "order": comment_order,
                    })
                    conn.commit()

                    with st.spinner("コメント収集中..."):
                        res_comments = collector.collect_comments_for_videos(
                            api,
                            conn,
                            vids_for_comments,
                            max_pages_per_video=int(comment_pages),
                            order=comment_order,
                        )
                        conn.commit()

                    storage.finish_job(conn, job2, "ok", {
                        "threads_upserted": res_comments.comments_threads_upserted,
                        "comments_upserted": res_comments.comments_upserted,
                        "errors": res_comments.errors[:50],
                        "api_calls": api.stats.calls,
                        "quota_units": api.stats.quota_units,
                    })
                    conn.commit()
                else:
                    st.warning("今回の収集でコメント対象の動画が無かったため、コメント収集はスキップしました。")

            # Track the search run -> videos mapping (optional)
            if use_search:
                try:
                    run_id = storage.create_search_run(
                        conn,
                        q=q.strip(),
                        order_by=str(filters["order"]),
                        mode=search_mode,
                        filters=filters,
                    )
                    if res_search is not None:
                        storage.insert_search_run_videos(conn, run_id, res_search.collected_video_ids)
                except Exception:
                    pass
                conn.commit()

            storage.finish_job(conn, job_id, "ok", {
                "target_mode": target_mode,
                "search": None if res_search is None else {
                    "video_ids_found": res_search.video_ids_found,
                    "new_videos": res_search.new_videos,
                    "videos_upserted": res_search.videos_upserted,
                    "channels_upserted": res_search.channels_upserted,
                    "errors": res_search.errors[:50],
                },
                "channels": None if res_channel is None else {
                    "video_ids_found": res_channel.video_ids_found,
                    "new_videos": res_channel.new_videos,
                    "videos_upserted": res_channel.videos_upserted,
                    "channels_upserted": res_channel.channels_upserted,
                    "errors": res_channel.errors[:50],
                },
                "api_calls": api.stats.calls,
                "quota_units": api.stats.quota_units,
            })
            conn.commit()

            msgs = []
            if res_search is not None:
                msgs.append(f"検索: 発見={res_search.video_ids_found}, 新規={res_search.new_videos}, 動画保存={res_search.videos_upserted}")
            if res_channel is not None:
                msgs.append(f"チャンネル: 発見={res_channel.video_ids_found}, 新規={res_channel.new_videos}, 動画保存={res_channel.videos_upserted}")
            if res_comments is not None:
                msgs.append(f"コメント: スレッド保存={res_comments.comments_threads_upserted}, コメント保存={res_comments.comments_upserted}")
            st.success(" / ".join(msgs) if msgs else "完了")
            st.info(f"API呼び出し回数={api.stats.calls}, 推定クォータ={api.stats.quota_units}")

            errs = []
            if res_search and res_search.errors:
                errs.extend(res_search.errors)
            if res_channel and res_channel.errors:
                errs.extend(res_channel.errors)
            if errs:
                st.warning("エラー/警告がありました（最大20件表示）:")
                st.code("\n".join(errs[:20]), language="text")

        finally:
            conn.close()

    st.divider()
    st.subheader("直近の収集ログ")
    conn = _open_conn()
    try:
        jobs = storage.list_jobs(conn, limit=50)
        df = pd.DataFrame([
            {
                "ID": j.id,
                "種別": j.job_type,
                "開始": j.started_at,
                "終了": j.ended_at,
                "状態": j.status,
            }
            for j in jobs if j.job_type in ("collect_search_videos", "collect_conditions")
        ])
        st.dataframe(df, use_container_width=True, hide_index=True)
    finally:
        conn.close()


def page_explore_export():
    st.header("データ閲覧 & CSV出力")
    if not _ctx_ready():
        st.warning("先に『セットアップ』でAPIキーとプロジェクトを設定してください。")
        return

    st.info("日時は、YouTube Data APIの値に従い基本的にUTC（末尾Z）です。日本時間（JST）にするには +9時間してください。")

    conn = _open_conn()
    try:
        ch_df = analysis.load_channels_df(conn)
        labeler = _make_channel_labeler(ch_df)

        dataset_labels = {
            "channels": "チャンネル",
            "videos": "動画",
            "comments": "コメント",
            "channel_snapshots": "チャンネルスナップショット（チャンネル名付き）",
            "video_snapshots": "動画スナップショット（タイトル/チャンネル名付き）",
            "search_runs": "検索収集ログ",
            "search_run_videos_join": "検索収集ログ×動画（結合）",
        }

        dataset = st.selectbox(
            "データセット",
            options=list(dataset_labels.keys()),
            index=1,
            format_func=lambda x: dataset_labels.get(x, x),
        )

        df = pd.DataFrame()
        if dataset == "channels":
            df = ch_df
        elif dataset == "videos":
            st.subheader("フィルタ（動画）")
            f, _ = ui.video_filter_panel(
                ch_df,
                labeler,
                key_prefix="ex_v",
                show_top_n=False,
                show_view_range=True,
                show_duration_max=True,
            )
            df = analysis.load_videos_df(conn, f)
        elif dataset == "comments":
            st.subheader("フィルタ（コメント）")
            f, _ = ui.video_filter_panel(
                ch_df,
                labeler,
                key_prefix="ex_v",
                show_top_n=False,
                show_view_range=True,
                show_duration_max=True,
            )
            df = analysis.load_videos_df(conn, f)

        elif dataset == "channel_snapshots":
            df = analysis.load_channel_snapshots_df(conn, limit=20000)
        elif dataset == "video_snapshots":
            df = analysis.load_video_snapshots_df(conn, limit=20000)

        elif dataset == "search_runs":
            df = pd.read_sql_query("SELECT * FROM search_runs ORDER BY collected_at DESC", conn)

        elif dataset == "search_run_videos_join":
            df = pd.read_sql_query(
                """
                SELECT r.id AS run_id, r.q, r.order_by, r.mode, r.collected_at,
                       v.video_id, v.channel_id, v.title, v.published_at, v.view_count, v.like_count, v.comment_count,
                       c.title AS channel_title
                FROM search_runs r
                JOIN search_run_videos rv ON rv.run_id = r.id
                JOIN videos v ON v.video_id = rv.video_id
                LEFT JOIN channels c ON c.channel_id = v.channel_id
                ORDER BY r.collected_at DESC
                """,
                conn,
            )

        st.subheader("プレビュー")
        view_df = df
        # 収集パート以外では、できるだけチャンネル名/動画タイトルを優先して表示
        if "channel_title" in view_df.columns:
            cols = [c for c in view_df.columns if c not in ("channel_title", "channel_id")]
            cols = ["channel_title"] + cols
            view_df = view_df[cols]
        if "video_title" in view_df.columns:
            cols = [c for c in view_df.columns if c != "video_title"]
            # channel_title の次に video_title を置く
            if "channel_title" in cols:
                idx = cols.index("channel_title") + 1
                cols.insert(idx, "video_title")
            else:
                cols.insert(0, "video_title")
            view_df = view_df[cols]
        # 画面表示ではID列は極力隠す（CSVには残す）
        for id_col in ("channel_id",):
            if id_col in view_df.columns:
                view_df = view_df.drop(columns=[id_col])
        st.dataframe(view_df.head(200), use_container_width=True, hide_index=True)
        st.caption(f"行数: {len(df)}（先頭200行を表示）")

        if df.empty:
            return

        # export
        project_slug = st.session_state["project_slug"]
        out_dir = artifacts.ensure_output_dir(_get_outputs_root(), project_slug)

        fname = st.text_input("出力ファイル名（CSV）", value=f"{dataset}.csv")
        csv_bytes = df.to_csv(index=False).encode("utf-8")

        if st.button("outputsフォルダに保存", use_container_width=True):
            p = artifacts.save_bytes(out_dir, safe_filename(fname), csv_bytes)
            st.success(f"保存しました: {p}")

    finally:
        conn.close()


def page_stats_charts():
    st.header("統計 & グラフ")
    if not _ctx_ready():
        st.warning("先に『セットアップ』でAPIキーとプロジェクトを設定してください。")
        return

    st.info("日時は、YouTube Data APIの値に従い基本的にUTC（末尾Z）です。日本時間（JST）にするには +9時間してください。")

    conn = _open_conn()
    try:
        ch_df = analysis.load_channels_df(conn)

        st.subheader("動画フィルタ")
        labeler = _make_channel_labeler(ch_df)

        f, top_n = ui.video_filter_panel(
            ch_df,
            labeler,
            key_prefix="st_v",
            show_top_n=True,
            show_view_range=True,
            show_duration_max=True,
        )
        df = analysis.load_videos_df(conn, f)

        if df.empty:
            st.info("条件に一致する動画がありません。")
            return

        chart_kind = st.selectbox(
            "グラフ",
            options=[
                "日別アップロード数",
                "再生数分布",
                "再生数×動画長",
                "再生数×高評価数",
                "再生数×コメント数",
                "高評価数分布",
                "コメント数分布",
                "高評価率（LIKE/再生）分布",
                "コメント率（コメント/再生）分布",
                "エンゲージメント率（(LIKE+コメント)/再生）分布",
                "再生数 上位動画",
                "高評価数 上位動画",
                "コメント数 上位動画",
                "エンゲージメント率 上位動画",
                "チャンネル別: 総再生数 上位",
                "チャンネル別: 平均再生数 上位",
                "チャンネル別: 平均エンゲージメント率 上位",
            ],
        )

        if chart_kind == "日別アップロード数":
            fig = charts.uploads_timeseries(df)
            title = "uploads_per_day"
        elif chart_kind == "再生数分布":
            fig = charts.views_hist(df)
            title = "views_distribution"
        elif chart_kind == "再生数×動画長":
            fig = charts.views_vs_duration(df)
            title = "views_vs_duration"
        elif chart_kind == "再生数×高評価数":
            fig = charts.views_vs_likes(df)
            title = "views_vs_likes"
        elif chart_kind == "再生数×コメント数":
            fig = charts.views_vs_comments(df)
            title = "views_vs_comments"
        elif chart_kind == "高評価数分布":
            fig = charts.likes_hist(df)
            title = "likes_distribution"
        elif chart_kind == "コメント数分布":
            fig = charts.comments_hist(df)
            title = "comments_distribution"
        elif chart_kind == "高評価率（LIKE/再生）分布":
            fig = charts.like_rate_hist(df)
            title = "like_rate_distribution"
        elif chart_kind == "コメント率（コメント/再生）分布":
            fig = charts.comment_rate_hist(df)
            title = "comment_rate_distribution"
        elif chart_kind == "エンゲージメント率（(LIKE+コメント)/再生）分布":
            fig = charts.engagement_rate_hist(df)
            title = "engagement_rate_distribution"
        elif chart_kind == "再生数 上位動画":
            fig = charts.top_videos(df, n=int(top_n), metric="view_count")
            title = f"top_{int(top_n)}_videos_by_views"
        elif chart_kind == "高評価数 上位動画":
            fig = charts.top_videos(df, n=int(top_n), metric="like_count")
            title = f"top_{int(top_n)}_videos_by_likes"
        elif chart_kind == "コメント数 上位動画":
            fig = charts.top_videos(df, n=int(top_n), metric="comment_count")
            title = f"top_{int(top_n)}_videos_by_comments"
        elif chart_kind == "エンゲージメント率 上位動画":
            fig = charts.top_videos(df, n=int(top_n), metric="engagement_rate")
            title = f"top_{int(top_n)}_videos_by_engagement"
        elif chart_kind == "チャンネル別: 総再生数 上位":
            fig = charts.top_channels(df, n=int(top_n), metric="views_sum")
            title = f"top_{int(top_n)}_channels_by_views_sum"
        elif chart_kind == "チャンネル別: 平均再生数 上位":
            fig = charts.top_channels(df, n=int(top_n), metric="views_mean")
            title = f"top_{int(top_n)}_channels_by_views_mean"
        else:
            fig = charts.top_channels(df, n=int(top_n), metric="engagement_mean")
            title = f"top_{int(top_n)}_channels_by_engagement_mean"

        # Streamlitのテーマ差分で見た目が変わらないように theme=None を試す
        try:
            st.plotly_chart(fig, use_container_width=True, theme=None)
        except TypeError:
            st.plotly_chart(fig, use_container_width=True)

        html = charts.plotly_to_html_bytes(fig)

        project_slug = st.session_state["project_slug"]
        out_dir = artifacts.ensure_output_dir(_get_outputs_root(), project_slug)

        save_name = st.text_input("保存ファイル名（HTML）", value=f"{title}.html")
        if st.button("outputsフォルダに保存", use_container_width=True):
            p = artifacts.save_bytes(out_dir, safe_filename(save_name), html)
            st.success(f"保存しました: {p}")

        st.divider()
        st.subheader("サマリ")
        m1, m2, m3, m4, m5, m6, m7 = st.columns(7)
        with m1:
            st.metric("動画数", int(len(df)))
        with m2:
            st.metric("総再生数", int(df["view_count"].fillna(0).sum()))
        with m3:
            st.metric("再生数中央値", int(df["view_count"].fillna(0).median()))
        with m4:
            st.metric("長さ中央値（分）", round(float((df["duration_seconds"].fillna(0) / 60.0).median()), 2))
        with m5:
            shorts_ratio = float(df["is_short"].mean()) if "is_short" in df.columns else 0.0
            st.metric("Shorts比率", f"{shorts_ratio:.1%}")
        with m6:
            st.metric("総高評価数", int(df["like_count"].fillna(0).sum()))
        with m7:
            st.metric("総コメント数", int(df["comment_count"].fillna(0).sum()))

    finally:
        conn.close()


def page_snapshots():
    st.header("スナップショット")
    if not _ctx_ready():
        st.warning("先に『セットアップ』でAPIキーとプロジェクトを設定してください。")
        return

    api = _get_api()

    conn = _open_conn()
    try:
        st.info("日時は、YouTube Data APIの値に従い基本的にUTC（末尾Z）です。日本時間（JST）にするには +9時間してください。")
        ch_df = analysis.load_channels_df(conn)
        labeler = _make_channel_labeler(ch_df)
        ch_df2 = ch_df.copy()
        ch_df2["title"] = ch_df2["title"].fillna("")
        ch_df2 = ch_df2.sort_values("title")
        vid_df = analysis.load_videos_df(conn, analysis.VideoFilters())

        if ch_df.empty:
            st.info("先にチャンネルを収集してください。")
            return

        st.subheader("チャンネルスナップショットの記録")
        channel_pick = st.multiselect(
            "対象チャンネル",
            options=ch_df2["channel_id"].tolist(),
            default=ch_df2["channel_id"].tolist()[:3],
            format_func=labeler,
        )
        refresh = st.checkbox("記録前にチャンネル統計を更新", value=True)

        if st.button("チャンネルスナップショットを記録", use_container_width=True):
            if refresh:
                n, errs = collector.update_channels(api, conn, channel_pick)
                conn.commit()
                if errs:
                    st.warning("\n".join(errs[:10]))

            n = storage.insert_channel_snapshots(conn, channel_pick)
            conn.commit()
            st.success(f"記録しました: {n}行")

        st.subheader("動画スナップショットの記録")
        latest_n = st.number_input("各チャンネルの最新N本", min_value=1, max_value=500, value=50, step=1)
        refresh_videos = st.checkbox(
            "記録前に動画統計を更新",
            value=False,
            help="大量更新でもクォータは比較的軽め（videos.list: 1unit/50本）ですが時間はかかります。",
        )

        if st.button("動画スナップショットを記録", use_container_width=True, disabled=vid_df.empty):
            targets: List[str] = []
            for cid in channel_pick:
                subset = vid_df[vid_df["channel_id"] == cid].sort_values("published_at", ascending=False).head(int(latest_n))
                targets.extend(subset["video_id"].tolist())
            targets = list(dict.fromkeys(targets))

            if refresh_videos and targets:
                n, errs = collector.update_videos_details(api, conn, targets)
                conn.commit()
                if errs:
                    st.warning("\n".join(errs[:10]))

            n = storage.insert_video_snapshots(conn, targets)
            conn.commit()
            st.success(f"記録しました: {n}行")

        st.divider()
        st.subheader("直近のスナップショット")
        c1, c2 = st.columns(2)
        with c1:
            df_cs = analysis.load_channel_snapshots_df(conn, limit=5000)
            if channel_pick:
                df_cs = df_cs[df_cs["channel_id"].isin(channel_pick)]
            st.dataframe(df_cs.head(200), use_container_width=True, hide_index=True)

        with c2:
            df_vs = analysis.load_video_snapshots_df(conn, limit=5000)
            if channel_pick and "channel_id" in df_vs.columns:
                df_vs = df_vs[df_vs["channel_id"].isin(channel_pick)]
            st.dataframe(df_vs.head(200), use_container_width=True, hide_index=True)

    finally:
        conn.close()


def page_logs():
    st.header("ログ")
    if not _ctx_ready():
        st.warning("先に『セットアップ』でAPIキーとプロジェクトを設定してください。")
        return

    api = st.session_state.get("api_client")
    if api is not None:
        st.subheader("API使用状況（現在のセッション）")
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("呼び出し回数", int(api.stats.calls))
        with c2:
            st.metric("推定クォータ（units）", int(api.stats.quota_units))
        with c3:
            st.metric("直近エラー", str(api.stats.last_error or "（なし）"))

    conn = _open_conn()
    try:
        st.subheader("最近のジョブ")
        jobs = storage.list_jobs(conn, limit=100)
        rows = []
        for j in jobs:
            type_labels = {
                "create_project": "プロジェクト作成",
                "collect_channels_videos": "収集（チャンネル→動画）",
                "collect_search_videos": "収集（検索条件→動画）",
                "collect_conditions": "収集（条件→動画）",
                "collect_comments": "コメント収集",
            }
            type_disp = type_labels.get(j.job_type, j.job_type)
            d = {}
            if j.details_json:
                try:
                    d = json.loads(j.details_json)
                except Exception:
                    d = {"details": j.details_json}
            # 表示を整える（detailsは必要最低限のみ）
            summary_key_labels = {
                "channels_upserted": "チャンネル保存",
                "videos_upserted": "動画保存",
                "new_videos": "新規動画",
                "video_ids_found": "発見動画ID",
                "comments_upserted": "コメント保存",
                "threads_upserted": "スレッド保存",
                "api_calls": "API呼び出し",
                "quota_units": "推定クォータ",
            }
            summary = []
            for k, label in summary_key_labels.items():
                if k in d and d[k] not in (None, ""):
                    summary.append(f"{label}={d[k]}")

            rows.append({
                "ID": j.id,
                "種別": f"{type_disp}（{j.job_type}）" if type_disp != j.job_type else j.job_type,
                "開始": j.started_at,
                "終了": j.ended_at,
                "状態": j.status,
                "要約": ", ".join(summary),
                "エラー": "\n".join(d.get("errors", [])) if isinstance(d.get("errors"), list) else (d.get("errors") or ""),
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    finally:
        conn.close()


def page_data_dictionary():
    st.header("データガイド")
    st.write(
        "このツールは、YouTube Data API v3 のレスポンスをSQLite（data/<project>.db）に保存しています。\n"
        "画面で扱うデータセット（channels / videos / comments / snapshots など）の英語カラム名の意味をここにまとめます。"
    )
    st.info("日時は、YouTube Data APIの値に従い基本的にUTC（末尾Z）です。日本時間（JST）にするには +9時間してください。")

    def _df(rows):
        return pd.DataFrame(rows)[["カラム", "意味（日本語）", "説明", "注意"]]

    tabs = st.tabs([
        "channels",
        "videos",
        "comments",
        "channel_snapshots",
        "video_snapshots",
        "search_runs",
        "search_run_videos_join",
        "画面上の派生列",
    ])

    with tabs[0]:
        st.markdown("### channels（チャンネル）")
        st.dataframe(
            _df([
                {"カラム": "channel_id", "意味（日本語）": "チャンネルID", "説明": "UCから始まるID。動画やスナップショットのキー。", "注意": "表示上は title（チャンネル名）を優先。"},
                {"カラム": "title", "意味（日本語）": "チャンネル名", "説明": "YouTube上の表示名。", "注意": "同名チャンネルが存在し得ます。"},
                {"カラム": "description", "意味（日本語）": "説明文", "説明": "チャンネルの概要欄。", "注意": "長文です。"},
                {"カラム": "published_at", "意味（日本語）": "チャンネル作成日時", "説明": "snippet.publishedAt", "注意": "UTCの可能性が高いです。"},
                {"カラム": "country", "意味（日本語）": "国", "説明": "snippet.country。", "注意": "未設定の場合はNULL。"},
                {"カラム": "custom_url", "意味（日本語）": "カスタムURL", "説明": "snippet.customUrl", "注意": "未設定の場合はNULL。"},
                {"カラム": "uploads_playlist_id", "意味（日本語）": "uploadsプレイリストID", "説明": "contentDetails.relatedPlaylists.uploads", "注意": "初回取得時に保存。"},
                {"カラム": "view_count", "意味（日本語）": "総再生数", "説明": "statistics.viewCount", "注意": "取得時点の値。"},
                {"カラム": "subscriber_count", "意味（日本語）": "登録者数", "説明": "statistics.subscriberCount", "注意": "非公開の場合NULL。"},
                {"カラム": "video_count", "意味（日本語）": "公開動画数", "説明": "statistics.videoCount", "注意": "取得時点の値。"},
                {"カラム": "fetched_at", "意味（日本語）": "取得日時", "説明": "この行をAPIから保存した時刻。", "注意": "UTCで保存。"},
            ]),
            use_container_width=True,
            hide_index=True,
        )

    with tabs[1]:
        st.markdown("### videos（動画）")
        st.dataframe(
            _df([
                {"カラム": "video_id", "意味（日本語）": "動画ID", "説明": "YouTubeの動画ID。", "注意": "動画URLの `v=` 部分。"},
                {"カラム": "channel_id", "意味（日本語）": "チャンネルID", "説明": "動画を投稿したチャンネル。", "注意": "画面表示では channel_title を優先。"},
                {"カラム": "title", "意味（日本語）": "動画タイトル", "説明": "snippet.title", "注意": ""},
                {"カラム": "description", "意味（日本語）": "概要欄", "説明": "snippet.description", "注意": "長文です。"},
                {"カラム": "published_at", "意味（日本語）": "公開日時", "説明": "snippet.publishedAt", "注意": "UTCの可能性が高いです。"},
                {"カラム": "duration_seconds", "意味（日本語）": "動画長（秒）", "説明": "contentDetails.duration（ISO8601）を秒に変換。", "注意": "Shorts判定などに使用。"},
                {"カラム": "category_id", "意味（日本語）": "カテゴリID", "説明": "snippet.categoryId", "注意": "カテゴリ名への変換は未実装。"},
                {"カラム": "tags_json", "意味（日本語）": "タグ（JSON）", "説明": "snippet.tags の配列をJSON文字列で保存。", "注意": "未設定の場合NULL。"},
                {"カラム": "default_language", "意味（日本語）": "既定言語", "説明": "snippet.defaultLanguage", "注意": ""},
                {"カラム": "live_broadcast_content", "意味（日本語）": "配信状態（API）", "説明": "snippet.liveBroadcastContent（none/live/upcoming）", "注意": "ライブアーカイブ判定にはこれだけでは不十分です。"},
                {"カラム": "scheduled_start_time", "意味（日本語）": "配信予定開始（UTC）", "説明": "liveStreamingDetails.scheduledStartTime", "注意": "予約動画/配信予定のときのみ。"},
                {"カラム": "actual_start_time", "意味（日本語）": "実開始（UTC）", "説明": "liveStreamingDetails.actualStartTime", "注意": "ライブ配信（配信中/アーカイブ）由来のときのみ。"},
                {"カラム": "actual_end_time", "意味（日本語）": "実終了（UTC）", "説明": "liveStreamingDetails.actualEndTime", "注意": "ライブアーカイブで入ることがあります。"},
                {"カラム": "view_count", "意味（日本語）": "再生数", "説明": "statistics.viewCount", "注意": "取得時点の値。"},
                {"カラム": "like_count", "意味（日本語）": "高評価数", "説明": "statistics.likeCount", "注意": "動画によりNULLのことがあります。"},
                {"カラム": "comment_count", "意味（日本語）": "コメント数", "説明": "statistics.commentCount", "注意": "コメント無効化等でNULL/0のことがあります。"},
                {"カラム": "fetched_at", "意味（日本語）": "取得日時", "説明": "この行をAPIから保存した時刻。", "注意": "UTCで保存。"},
            ]),
            use_container_width=True,
            hide_index=True,
        )

    with tabs[2]:
        st.markdown("### comments（コメント：トップレベルのみ）")
        st.dataframe(
            _df([
                {"カラム": "comment_id", "意味（日本語）": "コメントID", "説明": "top-level comment のID。", "注意": "返信は保存していません。"},
                {"カラム": "video_id", "意味（日本語）": "動画ID", "説明": "どの動画へのコメントか。", "注意": ""},
                {"カラム": "author_channel_id", "意味（日本語）": "投稿者チャンネルID", "説明": "コメント投稿者（存在する場合）。", "注意": "非公開等でNULLのことがあります。"},
                {"カラム": "author_display_name", "意味（日本語）": "投稿者表示名", "説明": "コメント投稿者名。", "注意": ""},
                {"カラム": "text", "意味（日本語）": "本文", "説明": "textDisplay or textOriginal", "注意": "絵文字やHTMLが含まれることがあります。"},
                {"カラム": "published_at", "意味（日本語）": "投稿日時", "説明": "コメントの投稿日時。", "注意": "UTCの可能性が高いです。"},
                {"カラム": "updated_at", "意味（日本語）": "更新日時", "説明": "編集された場合の更新日時。", "注意": ""},
                {"カラム": "like_count", "意味（日本語）": "コメント高評価数", "説明": "コメントのlikeCount。", "注意": ""},
                {"カラム": "fetched_at", "意味（日本語）": "取得日時", "説明": "この行をAPIから保存した時刻。", "注意": "UTCで保存。"},
            ]),
            use_container_width=True,
            hide_index=True,
        )

    with tabs[3]:
        st.markdown("### channel_snapshots（チャンネルスナップショット）")
        st.dataframe(
            _df([
                {"カラム": "id", "意味（日本語）": "連番", "説明": "スナップショット行のID。", "注意": ""},
                {"カラム": "captured_at", "意味（日本語）": "記録時刻", "説明": "スナップショットを記録した時刻。", "注意": "UTCで保存。"},
                {"カラム": "channel_id", "意味（日本語）": "チャンネルID", "説明": "対象チャンネル。", "注意": "画面/CSVでは channel_title を併記。"},
                {"カラム": "channel_title", "意味（日本語）": "チャンネル名（結合列）", "説明": "channels.title をJOINした表示用列。", "注意": "DB本体のchannel_snapshotsには存在しません（表示/出力時の結合）。"},
                {"カラム": "view_count", "意味（日本語）": "総再生数", "説明": "記録時点の総再生数。", "注意": ""},
                {"カラム": "subscriber_count", "意味（日本語）": "登録者数", "説明": "記録時点の登録者数。", "注意": ""},
                {"カラム": "video_count", "意味（日本語）": "動画数", "説明": "記録時点の動画数。", "注意": ""},
            ]),
            use_container_width=True,
            hide_index=True,
        )

    with tabs[4]:
        st.markdown("### video_snapshots（動画スナップショット）")
        st.dataframe(
            _df([
                {"カラム": "id", "意味（日本語）": "連番", "説明": "スナップショット行のID。", "注意": ""},
                {"カラム": "captured_at", "意味（日本語）": "記録時刻", "説明": "スナップショットを記録した時刻。", "注意": "UTCで保存。"},
                {"カラム": "video_id", "意味（日本語）": "動画ID", "説明": "対象動画。", "注意": "画面/CSVでは video_title を併記。"},
                {"カラム": "video_title", "意味（日本語）": "動画タイトル（結合列）", "説明": "videos.title をJOINした表示用列。", "注意": "DB本体のvideo_snapshotsには存在しません（表示/出力時の結合）。"},
                {"カラム": "channel_id", "意味（日本語）": "チャンネルID", "説明": "対象動画の投稿元。", "注意": ""},
                {"カラム": "channel_title", "意味（日本語）": "チャンネル名（結合列）", "説明": "channels.title をJOINした表示用列。", "注意": ""},
                {"カラム": "view_count", "意味（日本語）": "再生数", "説明": "記録時点の再生数。", "注意": ""},
                {"カラム": "like_count", "意味（日本語）": "高評価数", "説明": "記録時点の高評価数。", "注意": ""},
                {"カラム": "comment_count", "意味（日本語）": "コメント数", "説明": "記録時点のコメント数。", "注意": ""},
            ]),
            use_container_width=True,
            hide_index=True,
        )

    with tabs[5]:
        st.markdown("### search_runs（検索収集ログ）")
        st.dataframe(
            _df([
                {"カラム": "id", "意味（日本語）": "連番", "説明": "検索実行（run）のID。", "注意": ""},
                {"カラム": "q", "意味（日本語）": "検索語", "説明": "search.list に渡したクエリ。", "注意": "空文字のrunもあり得ます（チャンネル指定のみ収集など）。"},
                {"カラム": "order_by", "意味（日本語）": "並び順", "説明": "date / relevance など。", "注意": ""},
                {"カラム": "mode", "意味（日本語）": "収集モード", "説明": "diff / full など。", "注意": ""},
                {"カラム": "filters_json", "意味（日本語）": "条件（JSON）", "説明": "期間・言語などの詳細条件。", "注意": "JSON文字列です。"},
                {"カラム": "collected_at", "意味（日本語）": "実行時刻", "説明": "この検索runを保存した時刻。", "注意": "UTCで保存。"},
            ]),
            use_container_width=True,
            hide_index=True,
        )

    with tabs[6]:
        st.markdown("### search_run_videos_join（検索run×動画の結合ビュー）")
        st.write("Explore画面で選べる結合済みデータセットです（実体テーブルではなくJOIN結果）。")
        st.dataframe(
            _df([
                {"カラム": "run_id", "意味（日本語）": "検索run ID", "説明": "search_runs.id", "注意": ""},
                {"カラム": "q", "意味（日本語）": "検索語", "説明": "search_runs.q", "注意": ""},
                {"カラム": "order_by", "意味（日本語）": "並び順", "説明": "search_runs.order_by", "注意": ""},
                {"カラム": "mode", "意味（日本語）": "収集モード", "説明": "search_runs.mode", "注意": ""},
                {"カラム": "collected_at", "意味（日本語）": "収集日時", "説明": "search_runs.collected_at", "注意": "UTCで保存。"},
                {"カラム": "video_id", "意味（日本語）": "動画ID", "説明": "videos.video_id", "注意": ""},
                {"カラム": "title", "意味（日本語）": "動画タイトル", "説明": "videos.title", "注意": ""},
                {"カラム": "published_at", "意味（日本語）": "公開日時", "説明": "videos.published_at", "注意": "UTCの可能性が高いです。"},
                {"カラム": "view_count", "意味（日本語）": "再生数", "説明": "videos.view_count", "注意": "取得時点の値。"},
                {"カラム": "like_count", "意味（日本語）": "高評価数", "説明": "videos.like_count", "注意": ""},
                {"カラム": "comment_count", "意味（日本語）": "コメント数", "説明": "videos.comment_count", "注意": ""},
                {"カラム": "channel_title", "意味（日本語）": "チャンネル名", "説明": "channels.title（JOIN）", "注意": ""},
            ]),
            use_container_width=True,
            hide_index=True,
        )

    with tabs[7]:
        st.markdown("### 画面上の派生列（DBには保存しない列）")
        st.dataframe(
            _df([
                {"カラム": "channel_title", "意味（日本語）": "チャンネル名（結合）", "説明": "videos/comments/snapshots表示時にJOINで付与。", "注意": "DBテーブルの列ではない場合があります。"},
                {"カラム": "video_title", "意味（日本語）": "動画タイトル（結合）", "説明": "comments/snapshots表示時にJOINで付与。", "注意": ""},
                {"カラム": "published_date", "意味（日本語）": "公開日（date型）", "説明": "published_atから日付だけを抽出。", "注意": "UTC基準で日付になることがあります。"},
                {"カラム": "duration_min", "意味（日本語）": "動画長（分）", "説明": "duration_seconds/60", "注意": ""},
                {"カラム": "is_short", "意味（日本語）": "Shorts判定", "説明": "duration_seconds <= 60 をShortsとして扱う簡易判定。", "注意": "厳密なShorts判定ではありません。"},
                {"カラム": "broadcast_kind", "意味（日本語）": "配信状態（推定）", "説明": "live_broadcast_content と actual_start/end から推定（通常/ライブ配信中/予約/ライブアーカイブ）。", "注意": "旧データは判定不可のことがあります。"},
                {"カラム": "engagement_rate", "意味（日本語）": "エンゲージメント率", "説明": "(like_count + comment_count) / view_count", "注意": "view_countが0/NULLの場合は0扱いにしています。"},
            ]),
            use_container_width=True,
            hide_index=True,
        )


PAGES = {
    "セットアップ": page_setup,
    "データ収集": page_collect_by_search,
    "データ閲覧 & CSV出力": page_explore_export,
    "統計 & グラフ": page_stats_charts,
    "スナップショット": page_snapshots,
    "データガイド": page_data_dictionary,
    "ログ": page_logs,
}


def main():
    with st.sidebar:
        st.title("メニュー")
        choice = st.radio("ページ", list(PAGES.keys()), index=0)
        if st.session_state.get("project_slug"):
            st.caption(f"プロジェクト: {st.session_state['project_slug']}")
        if st.session_state.get("db_path"):
            st.caption(f"DB: {st.session_state['db_path']}")

    PAGES[choice]()


if __name__ == "__main__":
    main()
