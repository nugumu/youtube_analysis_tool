# YouTube Data API v3 – 収集 & 統計分析ツール（Streamlit）

**YouTube Data API v3（APIキーのみ）**で、VTuber関連チャンネル/動画/コメントをローカルに収集し、基本統計・可視化・CSV出力を行うツールです。

- ローカルSQLiteに保存（差分更新・全件取得）
- フィルタしながらプレビュー → CSV出力
- グラフはHTML/PNGで保存

## このツールが意図的に使わないもの
- OAuth / YouTube Analytics API（チャンネルオーナー権限が必要なため）
- Twitter/X API
- ライブ配信のチャットリプレイ等（v3だけでは後追い取得が難しいため）

## セットアップ
1. Google Cloudでプロジェクトを作成し、**YouTube Data API v3** を有効化
2. APIキーを作成
3. 依存パッケージをインストール

```bash
pip install -r requirements.txt
```

## Run
```bash
streamlit run app.py
```

## 制限・注意点
- コメントは膨大になり得るため、動画ごとにページ上限を設ける前提です。
- YouTube Data APIが返す統計値（viewCount等）は **取得時点のスナップショット** です。時系列が欲しい場合は **スナップショット** を定期的に記録してください。

## Typical workflow
1. **セットアップ**: APIキーを入力し、プロジェクト（SQLite DB）を作成/選択
2. **収集（検索条件→動画）**: チャンネルを特定しない条件（検索ワード等）で動画をDBへ収集（diff/full）
3. **収集（チャンネル→動画）**: チャンネルIDからアップロード動画を収集（差分更新/全件）
4. **閲覧 & CSV出力**: フィルタしてCSVをダウンロード/保存
5. **統計 & グラフ**: 代表的なグラフを作ってHTML/PNG保存

## 注意（search.list のコスト）
検索条件ベースの収集は `search.list` を使うため、**100 quota/回** です。件数を増やすとすぐ上限に達します。
大量探索より、チャンネルIDベースの収集（playlistItems.list + videos.list）を推奨します。
