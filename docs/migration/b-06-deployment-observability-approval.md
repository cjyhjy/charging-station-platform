# 模块审批单：B-06 部署、联调和可观测性

## 基本信息

- 模块编号：B-06
- 模块名称：部署、联调和可观测性
- 开发线：**B 线**（基础设施与集成线）
- 开发分支：`codex/backend/b-06-deployment-observability`
- 基线提交：`5e1bd26`（PR #39 合入 develop 后的最新 develop 顶端；`origin/develop` 已包含
  `backend/migrations/0007_charger_command_outcomes.sql` 与两份新路线文档）
- 目标集成分支：`develop`
- 工作区：`/home/penty/projects/charging-station-platform-backend-deploy`

## 变更范围

新增：

- `nginx/ncs-api.conf.template`：静态资源、`/api/v1/` 反代、`/api/v1/internal/charger-events` 来源限制、
  `/metrics`、`/readyz` 来源限制、TLS、trace ID 规则、HTTP→HTTPS 301；
- `nginx/README.md`：渲染变量、**envsubst 变量白名单**、端口与域名口径、密钥与证书注入、trace ID 说明；
- `backend/deploy/systemd/ncs-migrate.service`、`ncs-api.service`、`ncs-worker.service`、
  `ncs-outbox-publisher.service`、`ncs-backend.env.example`；
- `backend/deploy/Dockerfile`、`backend/deploy/docker-compose.yml`（可选路径，见"已知风险"第 5 条）；
- `backend/deploy/README.md`：启动顺序、systemd/容器安装、密钥注入、端口、指标与告警、备份恢复、故障演练、回滚；
- `backend/scripts/local-stack.sh`：本地联调（一条命令起全栈，可选带 Nginx，`--seed` 载入开发种子）；
- `backend/scripts/nginx-render.sh`：渲染 + `nginx -t`，`--drill` 在本机跑通静态/反代/限制/跳转；
- `backend/scripts/backup-postgres.sh`、`restore-postgres.sh`、`drill-backup-restore.sh`；
- `backend/scripts/verify-backup.sh`（每日完整性校验）、`wal-archive.sh`（WAL 归档，RPO 来源）、
  `push-backup-remote.sh`（先加密再异地推送）；
- `backend/deploy/systemd/ncs-backup{,-weekly,-verify}.{service,timer}`、`ncs-restore-drill.{service,timer}`、
  `ncs-backup-wal.service`（备份编排，systemd timer）；
- `backend/internal/observability/{probe.go,httpserver.go,process_metrics.go}` 及测试、`backend/.gitignore`。
- `backend/scripts/fault-drill.sh`、`backend/scripts/loadtest-orders.sh`；
- `backend/internal/observability/{prometheus.go,histogram.go}` 及测试；
- `backend/internal/repository/postgres/{health.go,health_integration_test.go}`；
- `backend/cmd/api/{observability.go,observability_test.go}`。

修改：

- `backend/cmd/api/main.go`：`/metrics` 路由与路由级埋点、就绪探针接线、`-migrate-only` 迁移门禁；
- `backend/cmd/worker/main.go`、`backend/cmd/outbox-publisher/main.go`：启动时核对 schema 版本；
- `backend/internal/observability/registry.go`：API/依赖指标名、直方图存储、`IncGauge`。

**明确未修改**：`backend/internal/order|auth|station|admin`、`api/openapi.yaml`、`docs/migration/approval-log.md`、
既有已批准迁移、仓库根 `docs/*`（不在 B 线范围，运维内容写在 `backend/deploy/README.md`）。
业务路由、状态码与响应体未变。

## 契约和数据

- 新增或修改 API：**无业务 API 变更**。新增 `GET/HEAD /metrics`（内部运维端点，**不登记 OpenAPI**，
  按裁决由 Nginx 限制来源）；`GET /readyz` 保留 OpenAPI 登记不变，语义改为由真实依赖探针驱动
  （PostgreSQL 与 Redis 任一不可达即 503），H5 与普通 Agent 不得依赖它。
- 请求/响应示例：

```text
GET /metrics  → 200, Content-Type: text/plain; version=0.0.4; charset=utf-8
  # TYPE ncs_dependency_up gauge
  ncs_dependency_up{dependency="postgres"} 1
  ncs_dependency_up{dependency="redis"} 1
  ncs_pg_migrations_version 7
  ncs_pg_outbox_unpublished 0
  # TYPE ncs_api_request_duration_seconds histogram
  ncs_api_request_duration_seconds_bucket{le="0.005",method="GET",route="/api/v1/stations",status="200"} 24
  ...

GET /readyz   → 200 {"success":true,"code":0,"message":"ok","data":{"ready":true}}
依赖不可达时  → 503（既有统一错误包络）
POST /metrics → 405（统一错误包络）
```

- PostgreSQL 迁移：**无新增迁移**。新增的是"启动前必须已应用迁移"的门禁与版本断言。
- Redis Key/Stream：**不新增、不改名**。
- 幂等和并发策略：**不变**；压测与故障演练用于验证既有语义（幂等键、设备命令永久去重、Publisher
  advisory lock 单活）。

## 验证记录

```text
环境：PostgreSQL 16.15（127.0.0.1:55439）、Redis 7（DB 14）、nginx 1.24.0、docker 29.1.3、
      systemd running、Go 1.22 工具链；验证机无 root、无 docker compose 插件、无法访问镜像仓库。

命令：gofmt -l ./cmd ./internal
结果：无输出

命令：go build ./... && go vet ./...
结果：通过

命令：NCS_TEST_PG_DSN=... NCS_REDIS_TEST_DB=14 go test -count=1 -race ./...
结果：全部 ok —— cmd/api 1.991s、cmd/mock-gateway 1.953s、cmd/outbox-publisher 2.103s、
      cmd/worker 2.406s、admin 2.032s、auth 5.218s、config 1.035s、event 1.043s、httpapi 1.040s、
      observability 1.184s、order 2.044s、repository/postgres 6.909s、repository/redis 3.491s、
      station 1.803s、worker 1.146s

命令：ncs-api -migrate-only（对已有库）
结果：level=INFO msg="migration gate complete" schema_version=7 expected=7 applied_this_run=0，exit 0

命令：ncs-api -migrate-only（对空库，drill 中）
结果：schema_version=7 expected=7 applied_this_run=7 —— 空库可一次迁移到 7

命令：curl /healthz、/readyz、/metrics（真实 API + 真实 PG/Redis）
结果：healthz=200；readyz={"ready":true}；metrics 含
      ncs_dependency_up{dependency="postgres"} 1、{redis} 1、ncs_pg_migrations_version 7、
      ncs_pg_outbox_unpublished 0；Content-Type 为 Prometheus 文本格式；POST /metrics → 405

命令：一次真实请求后的 metrics（证明按路由模板计数）
结果：ncs_api_requests_total{method="GET",route="/api/v1/stations",status="401"} 1
      ncs_api_requests_in_flight 0
      ncs_api_request_duration_seconds_{sum,count}{...} 各 1

命令：backend/scripts/nginx-render.sh --drill
结果：PASS —— nginx -t 通过；静态 200；http:8088 → https 301；
      /metrics、/readyz、/api/v1/internal/charger-events 在允许网段之外均为 403；
      /healthz 无 allow 列表、经反代返回（无后端时为 502）；访问日志带 request_id

命令：systemd-analyze verify backend/deploy/systemd/*.service
结果：通过，仅提示二进制尚未安装（部署步骤安装）

命令：NCS_TEST_PG_DSN=... backend/scripts/fault-drill.sh（真实 API，TCP 切断注入故障）
结果：PASS —— baseline healthz=200 readyz=200；切断 Redis → readyz=503、redis gauge=0、
      healthz 仍 200；恢复 Redis → readyz=200、gauge=1；切断 PostgreSQL → readyz=503、
      postgres gauge=0；恢复 → readyz=200；全过程 API 同一进程存活，故障均有日志

命令：NCS_TEST_PG_DSN=... backend/scripts/drill-backup-restore.sh（RPO/RTO 实测）
结果：PASS —— 备份 109 对象 / 42664 字节；备份后写入的行在恢复副本中确认缺失（RPO 边界）；
      恢复耗时 0 秒（RTO 0.00 分钟 ≤ 60）；恢复后 schema version=7、备份前的订单与两个钱包都在；
      保留策略 30 天。脚本同时打印诚实边界：同实例恢复、未演练 PITR

命令：NCS_API_URL=... NCS_POSTGRES_DSN=... backend/scripts/loadtest-orders.sh（并发 8 × 3 轮 + 重复提交）
结果：PASS —— 24 次并发读 p50=4ms p95=6ms max=6ms；
      同一 Idempotency-Key 的 8 个并发下单全部返回**同一个订单号**，库中恰好 1 条订单与
      1 条幂等记录；同一幂等键的 2 个并发 start 均 202/STARTING，库中恰好 1 条
      CHARGE_START_REQUESTED 与 1 条 CHARGER_COMMAND_REQUESTED；API 自身指标与流量一致

命令：NCS_POSTGRES_DSN=... backend/scripts/local-stack.sh --seed --with-nginx
结果：迁移门禁 → 四进程启动 → 冒烟（登录成功、20 个站点可见、metrics 可达）→
      Nginx 渲染并启动：static=200、/healthz 经反代=200

命令：docker-compose.yml 结构校验（本机无 compose 插件）
结果：YAML 可解析，8 个服务（含 migrate 一次性门禁、mock-gateway profile）、
      依赖关系为 api←migrate+redis、worker←migrate+api+mock-gateway、publisher←migrate+redis
```

### 反向验证（每条回归断言先破坏其保护的行为）

```text
1) 直方图累计性：把 observe 改成只累加首个覆盖桶 → TestHistogramBucketsAreCumulativeAndBounded
   与 TestWritePrometheusRendersHistogramBuckets 失败（counts = [1 1 3], want [1 2 3]）
2) 就绪跟随依赖：把 setReady(postgresUp && redisUp) 改成 setReady(true) →
   TestDependencyProbeDrivesReadiness 的 3 个子用例失败（ready = true, want false）
3) 迁移门禁：移除 worker 启动时的 AssertSchemaVersion 并重建 → 对**未迁移库**启动，
   进程不再退出（exit=124 持续运行到超时），日志中无 "schema is out of date"；
   恢复门禁后同一库 exit=1 且打印 "schema version 0 is behind the expected 7"
4) metrics 标签基数：单测逐条断言快照中不含请求路径（只有路由模板），
   3 个不同订单号不产生 3 条序列
5) 指标端点方法限制：POST /metrics → 405（单元 + 真实运行各一次）
```

## 验收口径对照（裁决第 11.2 条）

```text
1) 可用的 Nginx 站点与 API 代理模板：nginx/ncs-api.conf.template + --drill 实测通过 —— 完成
2) TLS 证书、密钥注入与生产配置说明：自签证书演练 + /etc/ncs/backend.env 0600 + 变量清单 —— 完成
3) 使用现有静态目录验证 H5 静态通路：--with-nginx 用 apps/dashboard 作为 NCS_STATIC_ROOT，
   静态 200；drill 用占位静态目录同样 200 —— 完成
4) Go API、Worker、Publisher 启动配置：三个 systemd 单元 + 迁移门禁单元，verify 通过，
   本机四进程实跑 —— 完成
5) healthz/readyz/metrics/日志通路：见验证记录（含故障时翻转与恢复）—— 完成
6) 设备回执路径网络限制：drill 中 /api/v1/internal/charger-events 在允许网段外 403 —— 完成
7) 本地联调、备份恢复、故障演练脚本：local-stack.sh、backup/restore/drill-backup-restore.sh、
   fault-drill.sh、loadtest-orders.sh 全部实跑通过 —— 完成
```

## 风险和回滚

- 已知风险：
  1. **`cmd/api` 启动路径被改动**：`/metrics` 与探针均为增量接线，且探针在 goroutine 中运行，
     不阻塞监听；迁移失败仍按原行为拒绝启动；
  2. **就绪语义变严**：依赖不可用时 API 会报 503（原实现启动后恒为 ready）。这是冻结的 fail-closed
     策略的可执行化，但部署侧必须确认负载均衡会摘除不健康实例，否则只会把 500 换成 503；
  3. **Nginx 模板替换现有站点**：模板以独立文件名与可配置端口交付，本机 80 端口已有默认站点，
     生产切换须"先加站点、验证后切换"；设备回执与运维网段必须按真实网络填写，不能留 `0.0.0.0/0`；
  4. **备份恢复风险最高**：脚本对非一次性库名直接拒绝，恢复会 DROP 目标库；演练脚本自带一次性库
     命名与清理，仍建议只在测试实例运行；
  5. **compose 路径未在本机验证**（无 compose 插件、镜像仓库不可达）：已在校验与文档中标注，
     验收以 systemd + 脚本路径为准；
  6. **RPO 依赖定时任务**：dump 间隔是数据丢失上界，需要 systemd timer 或 cron 真正按 15 分钟触发；
     更小 RPO 需 WAL 归档（未启用）。
- 回滚方式：删除新增文件即可；`cmd/api` 回退单个提交恢复原就绪行为；Nginx 切回上一份站点并 reload；
  无数据库迁移需要回滚。
- 是否影响旧 C++ 系统：**否**。未修改旧站点与旧代码，Nginx 切换由集成人员按发布计划执行。

## 第二轮：四项裁决的落地

### ① `/metrics` / `/readyz` 口径

```text
- /metrics 不登记 OpenAPI（内部运维端点）；/readyz 保留登记但语义为系统运维接口；/healthz 保持公开；
- 两者都只允许本机/内网/监控网段访问：API 业务监听 127.0.0.1:8080，指标在独立地址（默认 127.0.0.1:9090），
  Nginx 对 /metrics、/readyz 保留 allow/deny，且不向公网暴露 /metrics；
- H5 与普通 Agent 不使用这两个接口（模板未开放 CORS，文档明确禁用）；
- envsubst 变量白名单沿用（nginx/README.md），脚本固定使用白名单，禁止无白名单调用。

实测：三进程指标端点均绑定 127.0.0.1（9090/9091/9092）；从本机非环回地址访问 9091 返回连接被拒绝；
Nginx drill 中 /metrics、/readyz、设备回执在允许网段外均 403。
```

### ② 生产域名、证书与密钥注入

```text
- NCS_PUBLIC_HOST 由部署环境注入，模板与代码均无默认域名（渲染脚本用 :? 强制要求）；
- 公网 443 HTTPS；Go API 127.0.0.1:8080；H5 静态目录；metrics 仅内网；
- 证书不入 Git、不通过普通环境变量传递：只传路径，固定挂载
  /etc/ncs/tls/fullchain.pem 与 /etc/ncs/tls/privkey.pem，0600 root:ncs；
- /etc/ncs/backend.env 由 systemd EnvironmentFile 加载，0600 root:ncs；
- 数据库口令、Redis 口令、网关令牌只写入该文件或外部密钥系统；H5、日志、Nginx 配置均不输出，
  访问日志只记录 trace ID 与状态码。
```

### ③ 备份编排与异地保留

```text
编排（systemd timer，不用 cron；新增 9 个单元，均通过 systemd-analyze verify）：
  每日 03:20   ncs-backup.service          全量逻辑备份 → 本机保留 7 天，并触发异地推送
  每周日 04:10 ncs-backup-weekly.service   周快照 → 保留 12 周
  每日 05:30   ncs-backup-verify.service   完整性校验（checksum + pg_restore --list，可选真恢复）
  每周日 06:00 ncs-restore-drill.service   恢复演练（实测 RPO/RTO）
  常驻         ncs-backup-wal.service      pg_receivewal 持续归档 WAL —— RPO 的真正来源
异地：加密传输、加密存储（openssl AES-256-CBC/PBKDF2）、独立凭据（/etc/ncs/backup-remote.env 0600）、
  目的地版本保留、30 天保留、备份角色非超级用户（ncs_backup + pg_read_all_data + REPLICATION，附 SQL）、
  凭据不进仓库。

实测：
  backup-postgres.sh           → 93 KB / 108 对象 / 1 s，manifest 带 sha256，打印本机 7 天保留
  backup-postgres.sh --weekly  → 写入 weekly/ 并打印 12 周保留
  verify-backup.sh             → sha256 与 108 对象可读："PASS: verified 1 backup(s)"
  wal-archive.sh --once        → 以 ncs_test 角色连接成功、创建复制槽 ncs_wal_archive（wal_level=replica）
  drill-backup-restore.sh      → RPO 15 分钟（备份后写入的行确认缺失）、RTO 0.00 分钟、schema 7
未实测（不隐藏）：PITR 重放本身未演练（本机无第二个 PostgreSQL 实例，恢复演练走 dump 路径）；
  异地对象存储推送未实测（本机无对象存储与 rclone），只交付脚本与配置约束。
修正记录：wal-archive.sh 首版用未导出的 shell 变量传连接参数，pg_receivewal 退回默认 socket 与别的角色，
  而 --once 又用 || true 吞掉退出码，给出"检查通过"的假成功。两处都已修正：变量一律 export，
  smoke check 改为断言复制槽存在。
```

### ④ Worker / Publisher 指标

```text
三进程各自暴露 Prometheus 文本指标，默认 127.0.0.1:9090（API）/9091（Worker）/9092（Publisher），
可用 NCS_METRICS_ADDR 覆盖；非环回、非私网地址默认拒绝启动
（实测 NCS_METRICS_ADDR=0.0.0.0:9099 → 退出并打印 "metrics address must be a loopback or private address"）。

裁决要求 → 实际系列（一次真实运行后的抓取结果，POST /orders 201 产生流量）：
  消费成功数             ncs_worker_events_total{event_type="ORDER_CREATED",outcome="succeeded",stream="ncs:stream:order-event"} 1
  消费失败数             ncs_worker_events_total{outcome="retried"|"dead_lettered"|"lease_held"}（首次出现后）
  重试数                 ncs_worker_retries_total{attempt}（首次重试后）
  死信数                 ncs_worker_dead_lettered_total{reason}（首次死信后）/ ncs_stream_dead_letter_length 0
  Pending 数             ncs_stream_pending{stream=...} 0（三个 stream 齐全）
  Stream 延迟            ncs_stream_lag{stream=...} 0
  Outbox 未发布数量      ncs_pg_outbox_unpublished 0（API 与 Publisher 都暴露）
  Publisher 失锁次数     ncs_publisher_lock_losses_total 0
  Worker 最近成功时间    ncs_worker_last_success_timestamp_seconds 1.789463635e+09（真实消费后更新）
  Publisher 最近成功时间 ncs_publisher_last_publish_timestamp_seconds 1.789463635e+09（真实发布后更新）
  PG/Redis 连接状态      ncs_dependency_up{dependency="postgres"} 1、{dependency="redis"} 1

固定系列（依赖状态、各 stream 的 Pending/Lag/Length、死信长度、未发布、迁移版本、Publisher 计数与
时间戳）在启动时以初值创建，抓取始终能找到；**标签来自流量的系列不预造**——那会造出永远不动的幽灵 0，
看起来像发生过却从未发生的流量。该规则由 TestRegisterProcessMetricsPreCreatesTheRequiredSeries 固定。
```

## 第三轮：四项裁定的落地

### ① 异地对象存储（配置已批准、尚未实测）

```text
rclone 远端 ncs-backup-prod、桶 ncs-prod-backup 已写死为脚本默认值；daily/ 30 天、weekly/ 12 周
两个前缀分开（生命周期规则只能按前缀表达）；上传前 openssl AES-256-CBC/PBKDF2 加密，上传时再请求
服务端加密；凭据只在 /etc/ncs/backup-remote.env 与 /etc/ncs/rclone/rclone.conf（或外部密钥系统），
不进仓库；桶需开启对象版本保护与生命周期规则（脚本内的 rclone delete 只是兜底）。
未实测：本机没有对象存储与 rclone。已能离线校验的只有参数拼装与 --dry-run 路径。
```

### ② 恢复演练策略（每周 dump、每月 PITR）——PITR 已在本机实测通过

```text
新增 ncs-pitr-drill.{service,timer}（每月第一个周日 07:00）与 scripts/pitr-drill.sh；每周 dump 演练
保持 ncs-restore-drill.{service,timer}。PITR 在**隔离临时实例**上执行（pg_basebackup 建目录 →
pg_ctl 独立端口/socket 启动 → 重放 WAL → 断言 → 停止并删除），恢复目标 5～15 分钟（参数校验），
演练报告含恢复目标、耗时、归档覆盖与失败原因；结束清理包含**删除复制槽**。

实测（PASS）：target 2026-09-15 09:30:27.431219+00；before_target=1、after_target=0（恢复恰好停在
两个已知提交之间）；耗时 9 秒；归档 3 个已封段；演练后无残留进程、无残留槽、无残留目录。

跑通这一遍发现并修复的四个真实缺陷：
  1. `pg_receivewal --create-slot` 建槽后**立即退出** —— 原先 wal-archive.sh 服务模式带这个参数，
     实际行为是"建槽—退出—重启"循环，**一字节 WAL 都不会归档**，而 RPO 正是依赖它；已拆成
     "单独建槽 + 纯流式运行"，并用 pg_stat_replication 确认 streaming（这是本轮最重要的发现）；
  2. `recovery_target_time` 不接受 ISO 的 T/Z 写法（配置解析器报 invalid value）→ 改为 `... +00`；
  3. pg_basebackup 留下的数据目录权限非 0700 → 启动报 has invalid permissions → chmod 700；
  4. 断言连错库（标记表在源库）且用 `|| echo missing` 把查询失败伪装成"标记不存在" → 改为连源库、
     查询失败原样报告；
  另：`wal-archive.sh --once` 原先会留下无人服务的复制槽（一直占住 WAL）→ 检查完即删除。
```

### ③ 监控与告警阈值

```text
批准表已落为可加载规则文件 backend/deploy/monitoring/ncs-alerts.yml（3 组 12 条），阈值、持续时长、
级别与批准表一致；"最近成功时间"类规则一律与"确实有活干"取交集
（ncs_pg_outbox_unpublished > 0 or sum(ncs_stream_pending) > 0），否则空闲时必然误报。
为支持"Outbox 最老记录超过 N 分钟"，新增指标 ncs_pg_outbox_oldest_unpublished_seconds
（最老未发布记录等待秒数；空 Outbox 为 0，COALESCE 保证序列不消失），API/Worker/Publisher 三处探针
都已暴露并保留 HELP 说明。
未实测：本机没有 Prometheus/Alertmanager，规则**尚未在真实触发条件下验证**；上线前需人为触发一次
（例如停掉 Publisher，确认两条 Outbox 规则按 Warning→Critical 升级）。
```

### ④ 演练实例（NCS_TEST_PG_DSN）

```text
drill-backup-restore.sh 现在：未配置即拒绝执行（不回退默认库）；库名白名单（ncs_drill_*/*test*/*scratch*
等）且含 prod/production 一律拒绝；开跑前真的建一个 ncs_drill_capability_check 库并删除以验证
CREATEDB 权限；结束断言不留 ncs_drill_* 残余库；失败也打印"演练失败"报告并说明原因。
本机 ncs_a03 的账号实测具备 CREATEDB 与 REPLICATION（两者都被演练用到），可继续作为候选实例；
生产演练必须使用专门的、可删除的实例。
```

## 待审批事项

1. 异地对象存储的账号、桶策略与生命周期规则落地后，需要一次真实上传核验（内容、版本、30 天/12 周规则）；
2. 告警规则需要在接入监控系统后做一次真实触发验证，并按现场噪声调 `for` 时长；
3. 每月 PITR 演练的实例由谁提供（独立容器或临时 WSL 实例），以及演练报告留存位置；
4. 演练用 `NCS_TEST_PG_DSN` 的正式实例地址（不得使用开发共用库）。

## 审批结论

```text
状态：PENDING（第三轮裁定：通过条件审批，可合入并进入部署准备；不得宣称"生产灾备已验证"）
审批人：Codex
审批时间：
修改要求：上线前仍须补齐并实测：真实 rclone 异地上传、第二个 PostgreSQL 实例上的 PITR 重放（本机已用隔离临时实例跑通一次，生产实例上仍需一次）、告警规则实际触发、临时数据库创建/恢复/删除全流程。
```
