# SokuDL API（Render 用）

## Render の設定

- Runtime: Python
- Build Command: `pip install -r requirements.txt`
- Start Command: `python start.py`
- Health Check Path: `/healthz`

Render は PORT を自動で渡します。start.py はその PORT で待ち受けます。

## 環境変数（Render の Environment に入れる）

| 変数 | 値 |
|---|---|
| ENVIRONMENT | production |
| INTERNAL_TOKEN | INTERNAL_TOKEN.txt の値（Worker と同じ） |
| IP_HASH_SALT | 16 文字以上のランダム値（INTERNAL_TOKEN とは別） |
| WEB_ORIGINS | https://klyn.site |
| TRUST_PROXY | true |
| STORAGE_DIR | ./data/files |
| REDIS_URL | 空のままなら内蔵 Redis を使う。Render の Key Value を使う場合はその URL |

## 注意

- 内蔵 Redis はメモリ上です。再起動するとキューは消えます（完成ファイルは残ります）。
- Render の無料枠はアイドル時にスリープします。ワーカーが常時つながるには有料枠を推奨します。
- ファイルの保存先（STORAGE_DIR）は、Render の再デプロイで消える場合があります。永続が必要なら Render の Disk を付けてください。