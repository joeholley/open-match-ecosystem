This doc in English: [Open Match v2.x Ecosystem](README.md)

# Open Match v2.x Ecosystem

このサブディレクトリには、Open Match v2.xをベースにしたマッチメーカーを構築するためのサンプルと開発フレームワークが含まれています。以下の内容が含まれます：

* `examples/` ディレクトリには、Open Matchと連携するマッチメーカーアプリケーションの実装例が含まれています。詳細については、同ディレクトリ内のREADME（[英語版](examples/README.md) / [日本語版](examples/README-JP.md)）を参照してください。
* `internal/` ディレクトリには、`examples/` ディレクトリ内のアプリケーションで使用される共有モジュールが含まれています。
* `cloudbuild.yaml` ファイルは、`examples/` ディレクトリ内のアプリケーションのコンテナイメージをビルドするために使用できます。使用方法の手順はファイル先頭のコメントに記載されています。アプリケーションのコンテナイメージをデプロイする手順については、各アプリケーションのディレクトリ内を参照してください。

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
これら2つのバージョンのうち低い方（通常はpackビルダー側が遅れています）を、`go mod edit -go=<VERSION>` を使用してOpen Match 2関連プロジェクトの `go.mod` ファイルに設定してください。これは、手順に従って自身のプロジェクトでビルドを行うユーザーが、デフォルトのCloud Shell環境と最新のGoogle Cloud Buildpacksを使って問題なくビルドできるようにするためです。
* 最新の依存関係でビルドが動作することを確認した後、`go.mod` への変更をリポジトリにコミットしてください。

## ビルド (Build)

このリポジトリには、[Google Cloud 上の Cloud Build を使用してリモートでビルドする](https://cloud.google.com/docs/buildpacks/build-application#remote_builds)ために必要なファイルが含まれています。リポジトリをクローンし、`gcloud` を初期化し、`myregistry` という名前の既存の [Docker Artifact Registry](https://cloud.google.com/artifact-registry/docs/docker/store-docker-container-images) がある状態での典型的なビルド手順は以下のようになります：
```
# 依存関係を最新バージョンに更新し、テストを実行して、Cloud Buildを開始する
go get -u -t examples/... && go mod tidy  && go vet examples/... && go test -race examples/... && gcloud builds submit --async --substitutions=COMMIT_SHA=abc123,_ARTIFACT_REGISTRY_URI=myregistry,_GOOGLE_BUILDABLE=./examples/mmf,_JOB_NAME=soloduel
```
`_GOOGLE_BUILDABLE` には `examples/` ディレクトリ内のアプリケーションの1つ（`main.go` ファイルを含むディレクトリ）を指定します。`_JOB_NAME` は生成されるコンテナイメージの名前になります（デフォルトでは現在のgitコミットSHAでタグ付けされます）。

このために個別の `cloudbuild.yaml` や `Dockerfile` を用意する必要はありません。[Google Cloud の Buildpacks](https://cloud.google.com/docs/buildpacks/overview) がすべて自動的に処理します。

## デプロイ (Deploy)
`deploy/cloudbuild.yaml` ファイルには、Google CloudのCloud Runに `matchmaker-example` サービスをデプロイするためのステップが含まれています。
このファイルには、以下の置換変数（substitutions）を設定する必要があります：
* `_ARTIFACT_REGISTRY_URI` および `_OTEL_SIDECAR_IMAGE`: Artifact RegistryリポジトリのURIと、`otel-collector-sidecar` イメージのURI（`OTEL_SIDECAR=true` の場合に `gsdirector.*` および `mmqueue.*` のOpenTelemetryメトリクスをGoogle Cloud Monitoringへエクスポートするために必要です）。
* `_SERVICE_ACCOUNT`: `matchmaker-example` 用に作成したサービスアカウント。適切なIAMロールを持つサービスアカウントの作成手順については、次のセクションを参照してください。
* `_OM_CORE_ADDR`: Cloud Runから到達可能な `om-core` のCloud Run HTTPS URL。
* `_SOLODUEL_ADDR` / `_SOLODUEL_PORT` および `_DEBUGMMF_ADDR` / `_DEBUGMMF_PORT`: Cloud Run上のMMFサービスのURL（および `--use-http2` でCloud Runにデプロイした場合はポート `443`）、またはインプロセスのローカルテストMMFサーバーを使用する場合は `http://localhost`。

`examples/matchmaker` は受信HTTPリクエストとは独立して継続的なバックグラウンドループ（`mmqueue.Run` および `gsdirector.Run`）を実行し、インメモリでチケットとアサインメントの状態を保持するため、`deploy/cloudbuild.yaml` はデフォルトで常時稼働・CPUスロットリングなしのシングルトン（`--no-cpu-throttling --min-instances=1 --max-instances=1`）として `opentelemetry-collector` サイドカーコンテナと共にデプロイします。

あるいは、設定を記述した `service.yaml` ファイルを使用して、次のようなコマンドでサービスをデプロイすることもできます：
```
gcloud run services replace service.yaml
```
使用するマッチメーカーに応じて、スケーリング設定や `om-core` に許可するメモリ量を調整する必要がある場合があります。

`core` はコンテナイメージ内の単なるgRPC/HTTP Goアプリケーションであるため、少しの手間でほとんどの環境（ローカルのDocker / Minikube / Kubernetes / Knative）にデプロイできます。

## サービスアカウント (Service Account)
ベストプラクティスに従い、Cloud Runへのアプリケーションのデプロイは最小限のIAM権限で行います。以下のコマンドは、Cloud Run起動元（Cloud Run Invoker）権限のみを持つ `matchmaker-example-identity` という名前のサービスアカウントを作成します（これによりCloud Runで実行中の `om-core` と通信できます）。このサービスアカウントは、あなた自身とデフォルトのCloud Buildサービスアカウントのみが使用できます。

```
export PROJECT_ID=$(gcloud config get project)
export PROJECT_NUMBER=$(gcloud projects list --filter=$(gcloud config get project) --format="value(PROJECT_NUMBER)")
export GCLOUD_ACCOUNT=$(gcloud config get account)

gcloud iam service-accounts create matchmaker-example-identity \
  --display-name="Example Matchmaker Service Account"

gcloud iam service-accounts add-iam-policy-binding \
  matchmaker-example-identity@${PROJECT_ID}.iam.gserviceaccount.com \
  --role=roles/iam.serviceAccountUser \
  --member=user:${GCLOUD_ACCOUNT}

gcloud iam service-accounts add-iam-policy-binding \
  matchmaker-example-identity@${PROJECT_ID}.iam.gserviceaccount.com \
  --role=roles/iam.serviceAccountUser \
  --member=serviceAccount:${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
--member=serviceAccount:matchmaker-example-identity@${PROJECT_ID}.iam.gserviceaccount.com \
--role=roles/run.invoker
```

さらに、`cloudbuild.yaml` 内のステップを使用してデプロイする場合は、Cloud Buildサービスアカウント（`<PROJECT_NUMBER>-compute@developer.gserviceaccount.com`）にCloud Runへのデプロイ権限が必要です。ビルドされたコンテナイメージをCloud Console UIや `gcloud` CLIを使用して手動でCloud Runにデプロイする場合は、この権限は不要です。

## サンプルマッチメーカーの使用方法 (Using the example matchmaker)

サンプルマッチメーカーの動作は環境変数で制御できます。ローカルで実行する際に設定すると便利な環境変数は以下の通りです：
* `LOGGING_FORMAT=text` - コンソールでログを読みやすくします。
* `LOGGING_LEVEL=debug` - トラブルシューティングを容易にするため、より詳細なログを表示します。
* `OTEL_SIDECAR=false` - OpenTelemetryインスタンスへのメトリクスのエクスポートを試行せず、代わりに `localhost:${OTEL_PROM_PORT}/metrics`（デフォルトは `:2227/metrics`）でPrometheusメトリクスをローカルに提供します。
* `SOLODUEL_ADDR` / `SOLODUEL_PORT`（デフォルトは `http://localhost` / `50080`）および `DEBUGMMF_ADDR` / `DEBUGMMF_PORT`（デフォルトは `http://localhost` / `50081`） - `fifo` および `debug` MMFのターゲットホストとポート。`http://localhost` に設定すると、マッチメーカーは設定されたポートでインプロセスのgRPCサーバーを起動します。リモートURL（ポート `443` のCloud Run `https://...` URLなど）に設定すると、`om-core` はgRPC/TLS経由でリモートのMMFサービスを呼び出します。
* `REJECT_MATCHES_ON_DEACTIVATION_TIMEOUT=false`（デフォルトは `false`） - `extensions["deactivation_timeout"] = true`（`OM_MATCH_TICKET_DEACTIVATION_WAIT=true` の際に、チケット非アクティブ化のローカルキャッシュへのレプリケーションが `OM_MATCH_TICKET_DEACTIVATION_TIMEOUT_MS` 以内に確認できなかった場合に `om-core` によって設定される）が付与されて返されたマッチを `gsdirector` がどう処理するかを制御します。`false` の場合、Directorはそのマッチを承認し（サイクル間の衝突検出は `assignments` マップに委ねます）、警告ログを出力し、`gsdirector.matchmaking_cycle.match.deactivation_timeouts` / `gsdirector.matchmaking_cycle.ticket.deactivation_timeouts` を記録して、次サイクルのスリープバックオフを増加させます。`true` の場合、Directorは該当マッチを拒否し、`ActivateTickets` を介してそれらのチケットを再有効化します。
* `WAIT_FOR_BACKFILL_MMFS=false`（デフォルトは `false`） - 同一サイクル内でフリートの `MmfRequest` 呼び出しを開始する前に、バックフィルの `MmfRequest` 呼び出しが完了するのを `gsdirector` が待機するかどうかを制御します。`false`（デフォルト）の場合、バックフィルの `MmfRequest` が最初に開始され、直後にフリートの `MmfRequest` が開始されるため、両方が同じプレイヤープールを参照します。`true` の場合、`gsdirector` は各バックフィル `MmfRequest` ストリームが完了し（かつ `om-core` がマッチしたチケットを非アクティブ化し終える）まで待機してから、後続の `MmfRequest` を開始します。
* `MM_CYCLE_MIN_DURATION_MS=5000`（デフォルトは `5000`） - 各マッチメイキングサイクルの最小所要時間（ミリ秒）。次のサイクルが開始される前にチケットの非アクティブ化や再有効化が `om-core` インスタンス間でレプリケートされる時間を確保するため、観測された `om_core.cache.replication.lag` のp99以上に設定する必要があります。サイクル間のチケット衝突や `deactivation_timeout` アノテーションが発生した場合、`gsdirector` はこのスリープ時間を適応的に乗算（`assignedBackoffFactor`）して、`om-core` のレプリケーションが追いつく時間を与えます。
* `OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL=500`（デフォルトは `500`） - 1回の `ActivateTickets` 呼び出しで送信されるチケットIDの最大数。`om-core` の `OM_MAX_STATE_UPDATES_PER_CALL`（デフォルト `500`）以下である必要があります（この値は `om-core` で `OM_CACHE_PACK_TICKET_STATE_UPDATES=true` が有効な場合に単一のレプリケーションストリームエントリにパックされるチケットIDの上限でもあります）。
* `NUM_CONSECUTIVE_EMPTY_MM_CYCLES_BEFORE_QUIT`（デフォルトは `math.MaxInt32`） - マッチ提案がゼロのマッチメイキングサイクルがN回連続した後に、`gsdirector` が `Run()` を終了するようにします。ローカルテストや自動負荷テストで決定論的に終了させるのに便利です。

なお、このサンプルマッチメーカーは、Open Match 2を使用してマッチメーカーを構築するために**あなた自身が作成・保守する必要があるすべてのコード要素**の例であることに注意してください。これは、本番環境向けにそのコードをどのようにパッケージ化すべきかを示す例では**ありません**。特に本番環境では、マッチを要求するすべてのゲームクライアントからの負荷を処理するのに十分なインスタンス数を持つ、水平スケール可能なサービスとしてマッチメイキングキューとマッチメイキング機能（MMF）を構成する必要がありますが、ゲームサーバーマネージャーやDirectorの同一コピーを何十個も実行することは通常**望ましくありません**。`v2/examples/standalone` ディレクトリには、本番環境でマッチメーカーをパッケージ化してデプロイする方法のモデルとして、各コンポーネントの個別のコピーが含まれています。しかし、開発中にマッチングロジックやチケットパラメータを素早く反復改善しようとしているときに、ローカルマシン上で5つ以上のマイクロサービスを立ち上げて相互に正しく通信させるのは大きな手間がかかります。この「オールインワン」のmatchmakerアプリケーションは、本番環境でサービスとして実行する実際のコードをすべて1つのプロセス内で起動するため、本番稼働前にコードを新しいコードベースへ「移植」することなく、マッチメイキングのアプローチを素早く固めることができます。（初代Open Matchのユーザー向け補足：このアプリケーションは `minimatch` に相当します。）

### 将来のリファクタリング提案（Directorのテスト容易性向上）

`internal/gsdirector/gsdirector.go` 内の `MockDirector.Run()` は、サンプルが上から下へ順序立てて読めるチュートリアルとなるよう、意図的に `ProposalsLoop` をインラインで保持しています。包括的なユニットテストスイートを備えた本番コードベース向けに `gsdirector` を適合させる際は、以下のリファクタリングを推奨します：
1. **`ProposalsLoop` を独立した純粋関数として抽出する**: 例えば `evaluateProposal(match *pb.Match, assignments map[string]bool, pendingMmfRequestsByHash map[string]*pb.MmfRequest, cfg *viper.Viper) (decision, reason, stats)` のように抽出し、純粋なマッチ提案の評価（`deactivation_timeout` の確認、`mmfName` / `profileName` / `score` の検証、チケット衝突に関する `assignments` の確認、バックフィル用 `MmfRequest` 拡張の抽出）を、副作用（チケット再有効化のキューイング、ゲームサーバーの割り当て、メトリクスの記録）から分離します。
2. **`OmClient` をインターフェースの背後に抽出する**: `InvokeMatchmakingFunctions` と `ActivateTicketsWithResults` のための最小限のインターフェースを定義し、HTTPテストサーバーを立ち上げることなく、すべての提案拒否・承認ブランチおよびサイクルループの挙動を直接テーブルテストできるようにします。
