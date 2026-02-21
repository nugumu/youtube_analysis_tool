from __future__ import annotations

from typing import Dict, Optional
import numpy as np

import pandas as pd
import plotly.express as px
import plotly.io as pio


# Streamlit 側のテーマに引っ張られると、保存したHTMLと色・フォントがズレやすいので
# ここでテンプレートを固定し、st.plotly_chart(..., theme=None) とセットで使う。
DEFAULT_TEMPLATE = "plotly_white"


def apply_plotly_defaults(fig, *, kind: str = "generic"):
    """Set layout defaults so that standalone HTML matches the in-app look."""
    fig.update_layout(
        template=DEFAULT_TEMPLATE,
        height=540,
        margin=dict(l=80, r=30, t=70, b=70),
        # Standalone HTML での見え方を安定させるため、背景と文字色を明示する。
        # （Streamlitのテーマやブラウザの自動ダークモードの影響を受けにくくする）
        paper_bgcolor="white",
        plot_bgcolor="white",
        font=dict(size=14, color="#111"),
    )
    fig.update_xaxes(automargin=True)
    fig.update_yaxes(automargin=True)

    if kind in ("barh_long_labels",):
        fig.update_layout(margin=dict(l=220, r=30, t=70, b=70))

    return fig


def plotly_to_html_bytes(fig) -> bytes:
    apply_plotly_defaults(fig)
    # `include_plotlyjs="cdn"` にすると、ネットワーク制限/広告ブロッカー等で
    # JSが読み込めず「真っ黒/空白」に見えるケースがある。
    # 研究用途の再現性も考え、HTMLへ plotly.js を埋め込む。
    div = pio.to_html(
        fig,
        full_html=False,
        include_plotlyjs=True,
        config={"responsive": True, "displaylogo": False},
    )

    html = f"""<!doctype html>
<html>
  <head>
    <meta charset=\"utf-8\" />
    <meta name=\"darkreader-lock\" />
    <meta name=\"color-scheme\" content=\"light only\" />
    <meta name=\"theme-color\" content=\"#ffffff\" />
    <style>
      html, body {{ height: 100%; margin: 0; background: #ffffff; }}
    </style>
  </head>
  <body>
    {div}
  </body>
</html>"""
    return html.encode("utf-8")


# ----------------------------
# Chart builders (Plotly only)
# ----------------------------


def uploads_timeseries(df: pd.DataFrame):
    if df.empty:
        fig = px.line(title="日別アップロード数")
        return apply_plotly_defaults(fig)

    tmp = df.copy()
    tmp["published_date"] = pd.to_datetime(tmp["published_at"], errors="coerce").dt.date
    g = tmp.groupby(["published_date"]).agg(uploads=("video_id", "count")).reset_index()

    fig = px.line(
        g,
        x="published_date",
        y="uploads",
        title="日別アップロード数",
        labels={"published_date": "日付", "uploads": "本数"},
    )
    return apply_plotly_defaults(fig)


def views_hist(df: pd.DataFrame):
    if df.empty:
        fig = px.histogram(title="再生数分布")
        return apply_plotly_defaults(fig)
    fig = px.histogram(df, x="view_count", nbins=50, title="再生数分布", labels={"view_count": "再生数"})
    return apply_plotly_defaults(fig)


def likes_hist(df: pd.DataFrame):
    if df.empty:
        fig = px.histogram(title="高評価数（LIKE）分布")
        return apply_plotly_defaults(fig)
    fig = px.histogram(df, x="like_count", nbins=50, title="高評価数（LIKE）分布", labels={"like_count": "高評価数"})
    return apply_plotly_defaults(fig)


def comments_hist(df: pd.DataFrame):
    if df.empty:
        fig = px.histogram(title="コメント数分布")
        return apply_plotly_defaults(fig)
    fig = px.histogram(df, x="comment_count", nbins=50, title="コメント数分布", labels={"comment_count": "コメント数"})
    return apply_plotly_defaults(fig)


def views_vs_duration(df: pd.DataFrame):
    if df.empty:
        fig = px.scatter(title="再生数×動画長（分）")
        return apply_plotly_defaults(fig)
    tmp = df.copy()
    tmp["duration_min"] = (tmp["duration_seconds"].fillna(0) / 60.0)
    fig = px.scatter(
        tmp,
        x="duration_min",
        y="view_count",
        hover_data=["title", "video_id", "channel_title"],
        title="再生数×動画長（分）",
        labels={"duration_min": "長さ（分）", "view_count": "再生数"},
    )
    return apply_plotly_defaults(fig)


def views_vs_likes(df: pd.DataFrame):
    if df.empty:
        fig = px.scatter(title="再生数×高評価数")
        return apply_plotly_defaults(fig)
    fig = px.scatter(
        df,
        x="view_count",
        y="like_count",
        hover_data=["title", "video_id", "channel_title"],
        title="再生数×高評価数",
        labels={"view_count": "再生数", "like_count": "高評価数"},
    )
    return apply_plotly_defaults(fig)


def views_vs_comments(df: pd.DataFrame):
    if df.empty:
        fig = px.scatter(title="再生数×コメント数")
        return apply_plotly_defaults(fig)
    fig = px.scatter(
        df,
        x="view_count",
        y="comment_count",
        hover_data=["title", "video_id", "channel_title"],
        title="再生数×コメント数",
        labels={"view_count": "再生数", "comment_count": "コメント数"},
    )
    return apply_plotly_defaults(fig)


def _safe_rate(numer, denom):
    n = pd.to_numeric(numer, errors="coerce")
    d = pd.to_numeric(denom, errors="coerce")

    # 0除算回避
    try:
        d = d.replace(0, np.nan)
    except AttributeError:
        d = np.nan if d == 0 else d

    rate = n / d
    if hasattr(rate, "replace"):
        rate = rate.replace([np.inf, -np.inf], np.nan)
        return rate.astype("float64")
    return np.nan if rate in (np.inf, -np.inf) else float(rate)


def like_rate_hist(df: pd.DataFrame):
    if df.empty:
        fig = px.histogram(title="高評価率（LIKE/再生）分布")
        return apply_plotly_defaults(fig)
    tmp = df.copy()
    tmp["like_rate"] = _safe_rate(tmp["like_count"].fillna(0), tmp["view_count"].fillna(0))
    fig = px.histogram(tmp, x="like_rate", nbins=60, title="高評価率（LIKE/再生）分布", labels={"like_rate": "高評価率"})
    return apply_plotly_defaults(fig)


def comment_rate_hist(df: pd.DataFrame):
    if df.empty:
        fig = px.histogram(title="コメント率（コメント/再生）分布")
        return apply_plotly_defaults(fig)
    tmp = df.copy()
    tmp["comment_rate"] = _safe_rate(tmp["comment_count"].fillna(0), tmp["view_count"].fillna(0))
    fig = px.histogram(tmp, x="comment_rate", nbins=60, title="コメント率（コメント/再生）分布", labels={"comment_rate": "コメント率"})
    return apply_plotly_defaults(fig)


def engagement_rate_hist(df: pd.DataFrame):
    if df.empty:
        fig = px.histogram(title="エンゲージメント率（(LIKE+コメント)/再生）分布")
        return apply_plotly_defaults(fig)
    tmp = df.copy()
    tmp["engagement_rate"] = _safe_rate(
        tmp["like_count"].fillna(0) + tmp["comment_count"].fillna(0),
        tmp["view_count"].fillna(0),
    )
    fig = px.histogram(
        tmp,
        x="engagement_rate",
        nbins=60,
        title="エンゲージメント率（(LIKE+コメント)/再生）分布",
        labels={"engagement_rate": "エンゲージメント率"},
    )
    return apply_plotly_defaults(fig)


def top_videos(df: pd.DataFrame, n: int = 20, *, metric: str = "view_count", title: Optional[str] = None):
    if df.empty:
        fig = px.bar(title="上位動画")
        return apply_plotly_defaults(fig)

    metric_label = {
        "view_count": "再生数",
        "like_count": "高評価数",
        "comment_count": "コメント数",
        "engagement_rate": "エンゲージメント率",
    }.get(metric, metric)

    tmp = df.copy()
    if metric == "engagement_rate":
        tmp["engagement_rate"] = _safe_rate(
            tmp["like_count"].fillna(0) + tmp["comment_count"].fillna(0),
            tmp["view_count"].fillna(0),
        )

    tmp = tmp.sort_values(metric, ascending=False).head(int(n))
    fig = px.bar(
        tmp,
        x=metric,
        y="title",
        orientation="h",
        title=title or f"{metric_label} 上位{int(n)}本",
        labels={metric: metric_label, "title": "タイトル"},
        hover_data={"channel_title": True, "video_id": True, "published_at": True},
    )
    return apply_plotly_defaults(fig, kind="barh_long_labels")


def top_channels(df: pd.DataFrame, n: int = 20, *, metric: str = "view_count"):
    if df.empty:
        fig = px.bar(title="チャンネル別サマリ")
        return apply_plotly_defaults(fig)

    metric_label = {
        "views_sum": "総再生数",
        "likes_sum": "総高評価数",
        "comments_sum": "総コメント数",
        "views_mean": "平均再生数",
        "engagement_mean": "平均エンゲージメント率",
    }.get(metric, metric)

    tmp = df.copy()
    if "channel_title" in tmp.columns:
        tmp["channel_title"] = tmp["channel_title"].fillna(tmp.get("channel_id"))
    tmp["engagement_rate"] = _safe_rate(
        tmp["like_count"].fillna(0) + tmp["comment_count"].fillna(0),
        tmp["view_count"].fillna(0),
    )

    g = (
        tmp.groupby("channel_title")
        .agg(
            videos=("video_id", "count"),
            views_sum=("view_count", "sum"),
            likes_sum=("like_count", "sum"),
            comments_sum=("comment_count", "sum"),
            views_mean=("view_count", "mean"),
            engagement_mean=("engagement_rate", "mean"),
        )
        .reset_index()
    )

    g = g.sort_values(metric, ascending=False).head(int(n))
    fig = px.bar(
        g,
        x=metric,
        y="channel_title",
        orientation="h",
        title=f"チャンネル別 {metric_label} 上位{int(n)}",
        labels={"channel_title": "チャンネル", metric: metric_label},
        hover_data={"videos": True},
    )
    return apply_plotly_defaults(fig, kind="barh_long_labels")
