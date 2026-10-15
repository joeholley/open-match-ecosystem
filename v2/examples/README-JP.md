This doc in English: [Open Match 2 Examples](README.md)  
GitHubレポ [https://github.com/googleforgames/open-match-ecosystem/tree/main/v2](https://github.com/googleforgames/open-match-ecosystem/tree/main/v2)

# Open Match 2 サンプル (Examples)

v2/ディレクトリにあるサンプルマッチメーカーは、Open Match 2を使用して完全なマッチメイキングシステムを構築する方法を示します。ローカル開発用のモノリシックなデプロイメントと、本番環境で推奨される分離されたマイクロサービスベースのアーキテクチャの両方をサポートするように、柔軟に設計されています。

#### **コアコンポーネントとワークフロー**

このシステムは主にexamples/ディレクトリにある3つのアプリケーションで構成されており、これらはinternal/ディレクトリの共有ロジックを活用しています。

**1\. アプリケーション (examples/ディレクトリ):**

* **matchmaker**: すべてのコンポーネントを単一のプロセスで実行する開発およびテスト用のハーネスです。メモリ内のmmqueue、gsdirector、およびローカルのマッチメイキング機能（MMF）サーバーを初期化するため、複数のサービスをデプロイすることなくローカル開発を行うのに理想的です。  
* **standalone/mmqueue (マッチメイキングキュー):** ゲームクライアント向けのフロントエンドサービスです。  
  * **機能:** クライアントからのマッチメイキングリクエストを受け取り、Open MatchのTicket protobufを作成し、omclientを介してom-coreでの作成と有効化のためにキューに入れます。また、Directorから最終的なゲームサーバーのアサインメントを受け取り、クライアントに返します。  
* **standalone/gsdirector (ゲームサーバーDirector):** ゲームサーバーマネージャーとのマッチメイキングプロセスを調整するバックエンドサービスです。  
  * **機能:** ゲームサーバーマネージャー（この例ではモックのAgonesインテグレーション）にクエリを送信し、どのような種類のマッチが必要かを判断します。次にProfileオブジェクトを構築し、om-coreのInvokeMatchmakingFunctionsエンドポイントを呼び出します。Matchの提案を受け取った後、それらを評価し、承認されたマッチにゲームサーバーを割り当て、接続詳細をmmqueueに送信します。  
* **mmf (マッチメイキング機能):** カスタムのゲーム固有のマッチングロジックを含む、スタンドアロンのgRPCサーバーです。  
  * **機能:** om-coreはこのサービスのRunメソッドを呼び出し、フィルタリングされたチケットのプールを含むProfileをストリーミングします。MMFは自身のロジック（例：提供されているfifo関数は単純な先入れ先出しのマッチを作成）を実行し、Matchの提案をストリームで返します。

**2\. 共有ロジック (internal/ディレクトリ):**

* **omclient**: om-coreサービスのgRPC-Gatewayエンドポイントと通信するためのRESTful HTTPクライアント。チケットの作成、有効化、マッチメイキング機能の呼び出しを処理します。  
* **mmqueue**: マッチメイキングキューのコアロジックを含むモジュール。クライアントリクエストの管理やGoチャネルを介したアサインメントの処理が含まれます。モノリシックなmatchmakerの一部として、またはスタンドアロンのmmqueueサービスとして実行できます。  
* **gsdirector**: ゲームサーバーDirectorのロジックを含みます。このモジュールにはMockAgonesIntegrationが含まれており、これは特定のリージョンで利用可能なゲームモードなどのマッチメイキング要件を提供し、モックのサーバー割り当てを処理することで、ゲームサーバーマネージャーをシミュレートします。  
* **assignmentdistributor**: サンプルがゲームサーバーDirectorからマッチメイキングキューにアサインメントを返すために使用するインターフェース、および2つの実装（1つはGoogle Cloud Pub/Subを使用、もう1つはローカル開発用にgolangチャネルを使用）。  
* **metrics & logging**: すべてのアプリケーションで使用される、OpenTelemetryメトリクスと構造化されたLogrusロギングを初期化するための中央集権的なモジュールです。  
* **extensions**: Open Matchのprotobufメッセージで広く使用されるgoogle.protobuf.Any型を扱うためのユーティリティ関数を提供します。  
* **mocks/gameclient**: テスト目的でゲームクライアントからのリクエストをシミュレートする、モックのTicketオブジェクトを生成するユーティリティです。

### **マッチメーカーの設計解説**

Open Match v2サンプルマッチメーカーの設計は、3つの主要な情報を調整（リコンサイル）することを中心に展開されます。それは、特定のゲームモードに対して**プレイヤーをどのようにマッチングさせるか**のルール、**利用可能なゲームサーバー**のインベントリとそれらがホストできるマッチの種類、そしてマッチを待っている**利用可能なプレイヤー**のプールです。

#### **単一の信頼できる情報源（Single Source of Truth）としてのゲームサーバー（サーバーの可用性とマッチロジック）**

本番環境では、サーバーの可用性に関する情報とマッチ作成のルールは連携しており、ゲームサーバー自体が単一の信頼できる情報源として機能します。

* **推奨される本番設計**: GameMode（マッチメイキングルール、プレイヤーのプール、MMFパラメータを含む）の定義は、マッチメーカーにハードコードするのではなく、TerraformやHelmのようなInfrastructure-as-Code（IaC）ツールを通じて管理されます。デプロイ時に、これらのGameMode仕様はAgones Fleetなどのゲームサーバーテンプレートにメタデータ（例：Kubernetesアノテーション）として適用されます。その結果、そのFleetから作成されたすべてのGameServerインスタンスは、ホストするように設計されたマッチの正確な定義を自動的に継承します。  
* **サンプルでのシミュレーション方法**: サンプルマッチメーカーは、v2/internal/gsdirector/gsdirector.go内の**MockAgonesIntegration**を使用してこの本番設計をシミュレートします。  
  * 静的なGameMode、FleetConfig、zonePools変数は、IaCツールに存在するであろう設定の代役です。  
  * MockAgonesIntegration.Init()関数はデプロイプロセスをシミュレートし、これらの静的設定を消費してメモリ内にgameServersのマップを生成します。このマップ内の各モックサーバーは、それが必要とする特定のMmfRequestに関連付けられます。  
  * 次に、Directorは**GetMMFParams()**を呼び出します。実際のシステムでは、この関数はKubernetes APIにクエリを送信して準備完了状態のGameServerを検出し、そのマッチメイキングメタデータを直接読み取ります。このサンプルでは、初期化時に作成されたメモリ内のマップから単純に読み取るだけです。  
* このアプローチにより、Directorは常にゲームサーバーインフラの最新の状態に基づいて動作し、別途設定を必要とせずに、どのような種類のマッチが必要かを自動的に検出できます。

#### **利用可能なプレイヤー情報の保存**

この責務は、マッチメイキングキューとOpen Match Coreの連携によって担われます。

* **マッチメイキングキュー (mmqueue)**: このサービス (v2/internal/mmqueue/mmqueue.go) はプレイヤーにとっての入り口です。プレイヤーからのマッチ検索リクエストを受け付け、Open MatchのTicketオブジェクトを作成します。  
* **Open Match Core (om-core)**: om-coreは権威あるステートストアです。mmqueueはom-core内でチケットを作成・有効化し、om-coreは現在マッチを待っているすべてのプレイヤーのクエリ可能な確定的なプールを維持します。

### **リコンシリエーション（調整）プロセス**

システムの主要な機能は、まずサーバーの可用性とそれがホストできるゲームモードのルールを調整し、どのようなプレイヤーが必要かを理解することです。次に、そのニーズを利用可能なプレイヤーのプールと照合して最適なマッチを作成し、適切なサーバーに割り当てます。

#### **サーバーニーズの調整**

* **担当コンポーネント**: ゲームサーバーDirector **(gsdirector)**。  
* **プロセス**: gsdirectorのメインループは、ゲームサーバーマネージャー（MockAgonesIntegration.GetMMFParams()によってシミュレート）にクエリを送信することから始まります。これにより、利用可能なゲームサーバーのメタデータから直接MmfRequestオブジェクトが検出されます。この最初のステップで、サーバーの可用性とマッチ要件が調整され、実行すべき具体的なマッチメイキングタスクのリストが生成されます。

#### **プレイヤーとサーバーニーズの調整**

* **担当コンポーネント**: **gsdirector**、**om-core**、および**mmf**の連携。  
* **プロセス**:  
  * gsdirectorは検出したMmfRequestをom-coreに送信します。  
  * om-coreは、各リクエストのPools内の基準に基づいて、利用可能なプレイヤーのデータベースをフィルタリングします。  
  * 次に、om-coreは適切な**マッチメイキング機能 (mmf)**を呼び出し、フィルタリングされたプレイヤーのプールをストリーミングします。  
  * mmfはゲーム固有のロジックを実行し、Matchの提案をストリームで返します。  
  * 最後に、gsdirectorはこれらの提案を受け取り、最終検証を行い、承認された各マッチに特定の準備完了状態のゲームサーバーを割り当て、プレイヤーからサーバーへのアサインメントを完了させます。\[v2/internal/gsdirector/gsdirector.go\]

### **高度なマッチメーカー操作：バックフィルとチケット衝突の処理**

主要なマッチメイキングフローに加えて、堅牢なシステムは、部分的に空のゲームサーバーを埋める（バックフィル、途中参加［join-in-progress］、高密度ゲームサーバーなど）や、同じプレイヤーが同時に複数のマッチに提案されるケースを解決する（チケット衝突）といった一般的なシナリオを処理する必要があります。

#### **バックフィルの処理**

サンプルマッチメーカーは、バックフィルを、MMFが満員でないマッチを作成した際に生成される、特定の高優先度のマッチメイキングリクエストとして扱います。

* **バックフィルリクエストの生成 (MMF内)**:  
  * プロセスはv2/examples/mmf/functions/fifo/fifo.goなどのMMFで始まります。MMFがマッチを形成するものの、利用可能なチケットプールから全てのプレイヤースロットを埋めることができない場合、そのマッチを不完全と判断します。  
  * om-coreにマッチ提案を返す前に、MMFは*新しい*MmfRequestを構築します。この新しいリクエストには、空のスロットを埋めるために必要な特定のタイプと数のプレイヤーを見つけるために調整された新しいProfileが含まれています。  
  * この新しいバックフィルMmfRequestは、元のMatch提案のextensionsフィールドに添付されます。\[v2/examples/mmf/functions/fifo/fifo.go\]  
* **バックフィルリクエストの処理 (Director内)**:  
  * gsdirectorはMMFからマッチ提案を受け取り、そのextensionsを検査します。添付されたバックフィルリクエストを見つけた場合、このマッチをホストするゲームサーバーが後でさらにプレイヤーを必要とすることを知ります。  
  * 次に、Directorはゲームサーバーマネージャー（MockAgonesIntegration）のUpdateMMFRequestを呼び出します。この関数は、割り当てられたゲームサーバーのメタデータを更新し、新しいバックフィル要件を反映させます。これにより、次のマッチメイキングサイクルで、この特定のサーバーが新しいユニークなマッチメイキングリクエストの源となります。\[v2/internal/gsdirector/gsdirector.go\]  
  * **優先順位付け**: Directorはこれらの特定のバックフィルリクエストを本質的に優先します。GetMMFParams関数がゲームサーバーからすべてのマッチメイキングリクエストを収集する際、同じリクエストを共有するサーバーの数に基づいてソートします。バックフィルリクエストは単一のサーバーに固有であるため、参照カウントが最も低くなり、リストの先頭に配置されます。これにより、バックフィルリクエストが最初にom-coreに送信され、迅速に処理される可能性が高まります。\[v2/internal/gsdirector/gsdirector.go\]

#### **チケット衝突の処理**

Open Match 2は複数のMMFを並行して実行できるため、同じTicketが同一サイクル内で複数のMatch提案に含まれる可能性があります（また、状態レプリケーションが進行中の場合はサイクル間でも発生し得ます）。これらの「チケット衝突」を検出し解決する責任は、完全にDirectorにあります。

* **衝突の検出**:  
  * `gsdirector`は獲得済みチケットIDのセットを2つ維持しています。**過去のサイクル**でゲームサーバーに正常に割り当てられたすべてのチケットIDを記録する`assignments`マップと、**同一サイクル内**でより優先度の高い提案によって既に獲得されたすべてのチケットIDを記録するサイクル単位の`acceptedInCycle`マップです。\[v2/internal/gsdirector/gsdirector.go\]  
  * 1サイクル分のすべてのマッチ提案を受け取り、サーバーを割り当てる前に、Directorは候補となる提案をソートし、`backfill.`提案が標準のフリート提案よりも先に評価されるようにします。そのうえで各提案マッチを反復処理し、すべてのチケットIDを`assignments`（サイクル間の衝突）と`acceptedInCycle`（同一サイクル内の重複）の両方と照合します。  
* **衝突の解決**:  
  * Directorが提案されたマッチ内に、`assignments`または`acceptedInCycle`に既に存在するチケットを見つけた場合、これを衝突とみなします。  
  * 衝突するチケットを含むマッチ提案全体は、直ちに**拒否**されます。  
  * 拒否されたマッチのうち未獲得のチケット（`assignments`または`acceptedInCycle`に既に含まれているチケットIDは明示的にスキップされます）は、`ActivateTickets` APIを介して再有効化のために`om-core`に送り返されます。これにより、既にマッチが成立したプレイヤーが誤って再有効化されることを防ぎつつ、残りのプレイヤーを次のマッチメイキングサイクルで再び利用可能にします。\[v2/internal/gsdirector/gsdirector.go\]

##### **`om-core`によるサイクル間衝突の軽減と、レプリケーション遅延・MMF重複の見分け方**

チケット衝突が発生した原因を正確に把握するには、**サイクル間のレプリケーション遅延**と**同一サイクル内のMMF重複**を区別する必要があります：

1. **`om-core`がサイクル間の衝突を軽減する仕組み**: MMFが`Match`を返すと、`om-core`はそのマッチのroster内のすべてのチケットIDを検証し、状態ストレージのレプリケーションストリームに非アクティブ化（deactivation）更新を書き込みます。そしてデフォルト（`OM_MATCH_TICKET_DEACTIVATION_WAIT=true`）では、マッチをDirectorへストリームで返す前に、その非アクティブ化がローカルのチケットキャッシュへレプリケートされるまで最大`OM_MATCH_TICKET_DEACTIVATION_TIMEOUT_MS`（デフォルト`60000`ミリ秒）待機します。これにより、通常の状態では、返されたマッチ内のチケットは次のマッチメイキングサイクルが開始される前にローカルキャッシュ上で既に非アクティブとしてマークされています。
2. **`deactivation_timeout`マッチ拡張フィールド**: ローカルキャッシュへのレプリケーション待機がタイムアウトした場合（またはマッチのすべてのチケット非アクティブ化書き込みがレプリケーションIDの生成に失敗した場合）、`om-core`は`om_core.mmf.deactivation.timeouts`をインクリメントし、返される`Match`に`extensions["deactivation_timeout"] = true`（`google.protobuf.BoolValue`、定数`extensions.MatchDeactivationTimeoutKey`）を設定します。
   * デフォルト（`REJECT_MATCHES_ON_DEACTIVATION_TIMEOUT=false`）では、`gsdirector`はそのマッチを**承認**します（多くの場合、Redisへの非アクティブ化書き込み自体は成功しておりローカルキャッシュへの適用だけが遅れているためであり、サイクル間および同一サイクル内の重複は引き続き`assignments`マップと`acceptedInCycle`マップで防止されるためです）。同時に警告ログを出力し、`gsdirector.matchmaking_cycle.match.deactivation_timeouts`（および`gsdirector.matchmaking_cycle.ticket.deactivation_timeouts`）を記録したうえで、次のサイクルのスリープバックオフ（`assignedBackoffFactor`）を増加させて`om-core`のレプリケーションが追いつく時間を与えます。
   * `REJECT_MATCHES_ON_DEACTIVATION_TIMEOUT=true`を設定すると、`gsdirector`は`deactivation_timeout`が付与されたマッチを即座に拒否し、`ActivateTickets`を介してそれらのチケットを再有効化します。
3. **レプリケーション遅延と同一サイクル内のMMF重複の区別**: `om-core`は`InvokeMatchmakingFunctions`呼び出しごとにアクティブなチケットのポイントインタイム・スナップショットを取得するため、その呼び出し内で起動されたすべてのMMFは全く同じアクティブプールを参照します。したがって、重複するプールを共有する2つの並行MMF（または同一のDirectorサイクル内で並列に発行された複数の`InvokeMatchmakingFunctions`呼び出し）は、単一サイクル内で同じチケットを含むマッチを正当に提案する可能性があります。
   * `gsdirector`は（`backfill.`提案を標準のフリート提案より先にソートしたうえで）`ProposalsLoop`内でこれら2つのケースを直接区別して処理します：
     * **サイクル間の衝突 (`assignments`)**: **過去のサイクル**で既に割り当てられたチケット（`numAssigned > 0`）は、`gsdirector.matchmaking_cycle.match.rejections.assigned`（および`gsdirector.ticket.rejections.assigned`）をインクリメントし、`om-core`のレプリケーションが追いつく時間を与えるために次サイクルのスリープバックオフ（`assignedBackoffFactor`）を増加させます。
     * **同一サイクル内の重複 (`acceptedInCycle`)**: **同一サイクル内**の先行する提案で獲得されたチケット（`numSameCycleOverlap > 0` かつ `numAssigned == 0`）は、重複した提案を拒否して残りの未獲得チケットを再有効化します（`gsdirector.matchmaking_cycle.match.rejections`をインクリメント）が、`rejections.assigned`のインクリメントやサイクル間のレプリケーション遅延バックオフは発生させません。
4. **メトリクスのペアリングと対処方法**: マッチメーカー側（`gsdirector.*` / `mmqueue.*`）のメトリクスと`om-core`側（`om_core.*`）のメトリクスを組み合わせて監視することで、チケット衝突やサイクル時間の増加がレプリケーション遅延によるものかMMFの重複によるものかを診断できます：

| v2メトリクスの兆候 | ペアで確認すべき`om-core`メトリクス | 解釈 |
|---|---|---|
| `gsdirector.matchmaking_cycle.match.deactivation_timeouts` ↑ | `om_core.mmf.deactivation.timeouts`, `om_core.mmf.deactivation.wait` p99, `om_core.cache.replication.lag` | 非アクティブ化が時間内にローカルキャッシュへ適用されていません。Redis/ストリームのバックログが増加しているか、`OM_MATCH_TICKET_DEACTIVATION_TIMEOUT_MS`が小さすぎます。 |
| `gsdirector.matchmaking_cycle.match.rejections.assigned` ↑（timeouts = 0） | `om_core.mmf.prep.duration`, `om_core.cache.replication.lag` | ローカル非アクティブ化タイムアウトを伴わないサイクル間衝突です。`MM_CYCLE_MIN_DURATION_MS`がインスタンス間の非アクティブ化または*再有効化*のレプリケーション遅延より短いことが原因です（同一サイクル内のMMFプール重複は`acceptedInCycle`で捕捉され、`rejections.assigned`をインクリメントせずに`gsdirector.matchmaking_cycle.match.rejections`のみをインクリメントします）。 |
| `gsdirector.matchmaking_cycle.duration` ↑ | `om_core.mmf.deactivation.wait`, `om_core.cache.incoming.backlog`, `om_core.cache.incoming.apply.duration` | 受信レプリケーションが追いつくまでの`om-core`の非アクティブ化待機時間がマッチメイキングサイクル時間に含まれています。 |
| `mmqueue.ticket.activations.failures` / `gsdirector.ticket.reactivations.failures` > 0 | `om_core.cache.outgoing.backlog`, `om_core.cache.outgoing.send.duration` | 送信レプリケーション経路が飽和しているか、無効なチケットIDが（再）有効化されています。 |
| TPCに対するスループットの頭打ち | `om_core.cache.outgoing.updates`, `om_core.cache.incoming.polls.full` | `OM_CACHE_PACK_TICKET_STATE_UPDATES=true`の有効化を検討すべきケースです（すべての`om-core`インスタンスのアップグレード後のみ）。 |

   * **対処方法**: `om_core.cache.replication.lag`や`om_core.mmf.deactivation.timeouts`が増加している場合は、`MM_CYCLE_MIN_DURATION_MS`を`om_core.cache.replication.lag`のp99以上に調整する、1サイクルあたりのMMFファンアウトを減らす、`om-core`やRedisをスケールさせる、または`OM_CACHE_PACK_TICKET_STATE_UPDATES=true`を有効にして複数チケットの状態更新を単一のRedis Streamエントリにまとめることを検討してください（**重要**: `OM_CACHE_PACK_TICKET_STATE_UPDATES=true`は、同じRedisインスタンスから読み取るすべての`om-core`インスタンスがパックされたストリームエントリ形式をサポートするバージョンにアップグレードされた後にのみ有効にしてください）。
