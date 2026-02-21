# YouTube  データ収集 & 統計ツール

YouTube Data API v3 を使って、YouTubeチャンネル/動画/コメントのデータをローカル環境に収集し、基本統計・可視化・CSV出力を行うツールです。

- ローカルSQLiteに保存（差分更新・全件取得）
- フィルタしながらプレビュー → CSV出力
- グラフはHTMLで保存

## 事前準備

### (1) API キーの取得
Google Cloud で YouTube Data API v3 を有効化し、APIキーを取得してください。

### (2) Python 環境構築
このツールのコードは Python で書かれており、動かすには Python が必要です。


## 動かし方（Mac / Windows 共通）

### (1) ZIP を展開
このリポジトリ（ZIP）を任意の場所に展開して、コンソールから当該フォルダに移動します。

```bash
cd youtube_analysis_tool
```

### (2) 仮想環境を作る（必須ではないが推奨）
#### Mac / Linux
```bash
python3 -m venv .venv
source .venv/bin/activate
```

#### Windows（PowerShell）
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### (3) 必要なツールをインストール
```bash
pip install -r requirements.txt
```

### (4) 起動
```bash
streamlit run app.py
```

## 注意点
- コメントは膨大になり得るため、動画ごとにページ上限を設ける前提です。
- APIが返す統計値（viewCount等）は **取得時点のスナップショット** です。時系列が欲しい場合は **スナップショット** を定期的に記録してください。

## タブ一覧
1. **セットアップ**: APIキーを入力し、プロジェクト（SQLite DB）を作成/選択
2. **データ収集**: 条件を指定してデータをDBへ収集
3. **データ閲覧 & CSV出力**: フィルタしてCSVをダウンロード/保存
4. **統計 & グラフ**: 集計値からグラフを作ってHTML保存
5. **スナップショット**: チャンネルまたは動画のスナップショットを取得
6. **データガイド**: データの列名ガイド
7. **ログ**: ツールの実行ログを確認

## 注意（search.list のコスト）
検索条件ベースの収集は `search.list` を使うため、**100 quota/回** です。件数を増やすとすぐ上限に達します。
大量探索より、チャンネルIDベースの収集（playlistItems.list + videos.list）を推奨します。
