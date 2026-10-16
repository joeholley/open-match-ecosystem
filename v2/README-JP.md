This doc in English: [Open Match v2.x Ecosystem](README.md)

# Open Match v2.x Ecosystem

このサブディレクトリには、Open Match v2.xをベースにしたマッチメーカーを構築するためのサンプルと開発フレームワークが含まれています。以下の内容が含まれます：

* `examples/` ディレクトリには、Open Matchと連携するマッチメーカーアプリケーションの実装例が含まれています。詳細については、同ディレクトリ内のREADME（[英語版](examples/README.md) / [日本語版](examples/README-JP.md)）を参照してください。
* `internal/` ディレクトリには、`examples/` ディレクトリ内のアプリケーションで使用される共有モジュールが含まれています。
* [`cloudbuild.yaml`](cloudbuild.yaml) ファイルは、`examples/` ディレクトリ内のアプリケーションのコンテナイメージをビルドするために使用できます。使用方法の手順はファイル先頭のコメントに記載されています。アプリケーションのコンテナイメージをデプロイする手順については、各アプリケーションのディレクトリ内を参照してください。

# 開発 (Development)

`examples/` ディレクトリにあるローカル開発用サンプルマッチメーカー、スタンドアロンのゲームサーバーDirector、スタンドアロンのマッチメイキングキュー、およびマッチメイキング機能（MMF）は、[CNCF Buildpacks](https://www.cncf.io/projects/buildpacks/) を使用してビルドされるように設計されたGoアプリケーションであり、主要なビルドプロセスでは特に [Google Cloud の Buildpacks](https://cloud.google.com/docs/buildpacks/overview) を使用しています。実行にはOpen Match 2が必要であり、OM2インスタンスに接続できない場合は起動しません。

### メンテナー向け (For Maintainers)

以下は新しいリリースを行う際のベストプラクティスです（必須ではありません）：

* [cloud.google.com](https://cloud.google.com) にアクセスし、クリーンなCloud Shellターミナルを開きます。`go version` を実行して、Cloud Shellでデフォルトで提供されているバージョンを確認します。次に、（後述のコマンドのように）packを使用するCloud Buildを実行し、そこでデフォルトとして使用されているGoのバージョンを示す行をビルドログから確認します。ビルドログには以下のような行が表示されるはずです：
```
Step #1: ===> BUILDING
Step #1: [builder] target distro name/version labels not found, reading /etc/os-release file
Step #1: [builder] === Go - Runtime (google.go.runtime@0.9.1) ===
Step #1: [builder] Go version not specified, using latest available Go runtime for the stack "ubuntu2204"
Step #1: [builder] 2025/08/27 06:00:39 [DEBUG] GET https://go.dev/dl/?mode=json
Step #1: [builder] ***** CACHE MISS: "go"
Step #1: [builder] Installing Go v1.24.6.
```
これら2つのバージョンのうち低い方（通常はpackビルダー側が遅れています）を、`go mod edit -go=<VERSION>` を使用してOpen Match 2関連プロジェクトの `go.mod` ファイルに設定してください（現在の [`go.mod`](go.mod) では Go `1.26+`）。これは、手順に従って自身のプロジェクトでビルドを行うユーザーが、デフォルトのCloud Shell環境と最新のGoogle Cloud Buildpacksを使って問題なくビルドできるようにするためです。
* 最新の依存関係でビルドが動作することを確認した後、`go.mod` への変更をリポジトリにコミットしてください。

## ビルド (Build)

このリポジトリには、[Google Cloud 上の Cloud Build を使用してリモートでビルドする](https://cloud.google.com/docs/buildpacks/build-application#remote_builds)ための [`cloudbuild.yaml`](cloudbuild.yaml) が含まれています。リポジトリをクローンし、`gcloud` を初期化し、`myregistry` という名前の既存の [Docker Artifact Registry](https://cloud.google.com/artifact-registry/docs/docker/store-docker-container-images) がある状態での典型的なビルド手順は以下のようになります：
```
# 依存関係を最新バージョンに更新し、テストを実行して、Cloud Buildを開始する
go get $(go list -m -f '{{if not (or .Main .Indirect)}}{{.Path}}@latest{{end}}' all) && go mod tidy && go vet ./... && go test -race ./... && gcloud builds submit --async --substitutions=COMMIT_SHA=abc123,_ARTIFACT_REGISTRY_URI=myregistry,_GOOGLE_BUILDABLE=./examples/mmf,_JOB_NAME=soloduel
```
`_GOOGLE_BUILDABLE` には `examples/` ディレクトリ内のアプリケーションの1つ（`main.go` ファイルを含むディレクトリ）を指定します。`_JOB_NAME` は生成されるコンテナイメージの名前になります（デフォルトでは `COMMIT_SHA` 置換変数でタグ付けされます）。

ルートの [`cloudbuild.yaml`](cloudbuild.yaml) が [Google Cloud の Buildpacks](https://cloud.google.com/docs/buildpacks/overview)（`gcr.io/k8s-skaffold/pack`）を呼び出すため、`Dockerfile` を用意する必要はありません。

## デプロイと実行 (Deploy & Run)

アーキテクチャの詳細、デプロイマニフェスト、および実行時の設定については、各サブディレクトリのドキュメントを参照してください：

* **アーキテクチャと設計の概要**: [`examples/README-JP.md`](examples/README-JP.md)（[英語版](examples/README.md)）では、コアコンポーネント、リコンシリエーションループ、バックフィルワークフロー、チケット衝突の処理（`assignments` と `acceptedInCycle`）、および `om-core` とのOpenTelemetryメトリクスのペアリングについて解説しています。
* **オールインワン開発用マッチメーカー**: [`examples/matchmaker/README.md`](examples/matchmaker/README.md) および [`examples/matchmaker/deploy/cloudbuild.yaml`](examples/matchmaker/deploy/cloudbuild.yaml) では、サービスアカウントの作成、Cloud Runへのデプロイ、HTTPエンドポイント、およびキュー・Director・ローカルMMFを単一プロセスで実行するための環境変数について説明しています。
* **マッチメイキング機能 (MMF)**: [`examples/mmf/README.md`](examples/mmf/README.md) および [`examples/mmf/deploy/cloudbuild.yaml`](examples/mmf/deploy/cloudbuild.yaml) では、スタンドアロンのgRPC MMFサービス（`fifo` および `debug`）のビルド、デプロイ、および実装方法について説明しています。
* **スタンドアロンの本番構成コンポーネント**: [`examples/standalone/gsdirector/deploy/cloudbuild.yaml`](examples/standalone/gsdirector/deploy/cloudbuild.yaml) および [`examples/standalone/mmqueue/deploy/cloudbuild.yaml`](examples/standalone/mmqueue/deploy/cloudbuild.yaml) では、ゲームサーバーDirectorとマッチメイキングキューをGoogle Cloud Pub/Subで接続された独立したCloud RunサービスとしてデプロイするためのCloud Buildパイプラインを提供しています。
