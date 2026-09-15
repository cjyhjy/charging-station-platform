# 迁移模块审批单：BE-B-05 故障恢复与可观测性

第 6 版：已 rebase 到 **六次修复后的 BE-B-04**（`b728d8c`），并按 B-04 第 6 轮的新语义扩展；本版同时包含第 1 轮审查意见的 6 项修复与第 4、5 版内容。

审查意见指出"B05 会把 B04 的错误死信计入指标"：B-04 第 5 轮修掉了错误死信本身（`superseded` 静默），第 6 轮修掉了让该保护失效的两条真实链路；本版据此复验，指标语义未变。

审查意见"B05 自身改动通过，但基于 B04，会把错误死信计入指标"已按此处理：B-04 第 5 轮修掉了错误死信本身，本版新增 `superseded` 结果让"旧代次静默"这件事可观测，并且**不再把被取代的投递计入死信**。

见"第 5 版：第四次 rebase"、"第 4 版：第三次 rebase"与"第 3 版：第 1 轮审查意见的修复"三节。

本文件按 `docs/migration/module-approval-template.md` 填写。审批人确认后，请将结论补登到
`docs/migration/approval-log.md`（该文件由集成人员维护，本模块未修改）。

## 基本信息

- 模块编号：BE-B-05
- 模块名称：故障恢复与可观测性
- 开发线：B
- 开发分支：`codex/migration/backend-events/b-05-recovery-observability`
- 分支基点：`b728d8c`（经**六轮**修复的 `codex/migration/backend-events/b-04-charge-command-workers` 顶端；
  第 1 版基点 `7a1e33e`，第 2 版 `d6b257a`，第 4 版 `22fd8f6`，第 5 版 `ad4c524`，本版为第 5 次 rebase）
- 目标集成分支：`codex/migration-integration`
- 提交哈希：第 6 版（当前）代码 `3e4721b`，材料见文末"提交记录"。历史哈希见文末

### 启动前置说明：已第二次 rebase，并已修正一处错误结论

**1. 已两次 rebase 到修复后的 B-04。** 本模块最初创建于 B-04 的**修复前**顶端（`7a1e33e`），因此当时
它携带了 B-04 复审认定的 5 个缺陷；第 1 次 rebase 到 `a6f4b0a` 后又出现第 3 轮复审的 3 项交叉故障
路径修复，故第 2 次 rebase 到 `d6b257a`。两次冲突都集中在 `worker.go`（以及 `runner.go`、
`cmd/worker/main.go`），均按"两侧都保留"解决：

- 第 2 轮修复的 `ErrLeaseHeld` 分支上报**独立结果** `OutcomeLeaseHeld` 而不是并入 `retried`；
- 第 3 轮把重试预算改为**消费记录自己的尝试次数**后，本模块的日志与死信标签同步改用该次数（不再用
  Redis 投递计数），使"attempt"在所有观测面上含义一致；
- 第 3 轮新增的 `DeadLetterGuard.ReleaseDeadLetter` 与本模块的"死信写入被抑制"计数共存于同一分支。
  （第 4 轮把声明改成三态后，"被抑制"专指**已完成写入存在**这一种情况；另有 `dead_letter_held` 表示另一方
  正在写、结果未知。见"第 4 版"一节。）

**2. 更正上一版的一处错误结论。** 上一版把"消费组以 `$` 创建从而跳过既有积压"写成"刻意的、无需处理"。
**这是错的**，且正是 B-04 复审列为 P0 的缺陷。已更正，详见"真实 Redis 语义核实"一节的说明。

**遗留的 rebase 义务**：B-01、B-04 合入集成后，本分支仍需 rebase 到最新集成。

## 契约第 5 节要求的逐条交付

| 契约要求 | 交付方式 | 证据 |
|---|---|---|
| Stream lag | `redis.StreamInfo`/`GroupInfo`（`XINFO STREAM`/`XINFO GROUPS`）+ `observability.Collector` 写入 `ncs_stream_lag` 指标。**消费组不存在时不发布该序列**（lag 在该状态下没有意义），改以 `ncs_stream_group_missing=1` 表示，见第 3 版修复 3 | 真实 Redis 集成测试 + 端到端实测 + `TestCollectorReportsAMissingGroupInsteadOfZeroLag` |
| Pending 数量 | 同上，写入 `ncs_stream_pending` 指标 | 端到端实测中启动快照显示 `pending=2`，退出快照 `pending=0` |
| 重试次数 | `ncs_worker_retries_total`（按 attempt 标签，**固定上界 20**，见第 3 版修复 5）+ 每条重试日志 | `TestWorkerReportsRetryAndPermanentOutcomes` 等 |
| 死信数量 | 两个维度：`ncs_worker_dead_lettered_total`（按 reason，计数器）与 `ncs_stream_dead_letter_length`（`XLEN`，仪表） | `TestCollectorSamplesLagPendingLengthAndDeadLetters` |
| Worker 重启恢复 | `Worker.Prepare`（`Run` 的第一步）在首次读取**之前**先执行 Pending 恢复；`PendingRecovered` 上报回收条数并记录日志 | 端到端实测：前置进程遗留 2 条 pending → 启动即 `recovered: 2`；`Runner.SetOnReady` 让启动快照等在这次恢复之后 |
| 关键业务 trace ID | `observability.WithTraceID` 把事件 trace_id 注入处理器上下文；`observability.ContextHandler` 让**每条**日志自动带上 `trace_id` | `TestWorkerLogsCarryTheTraceID`、`TestWorkerPropagatesTheTraceIDToTheHandlerContext` |

## 第 2 版新增（按 B-04 修复后的语义对齐）

rebase 之后，B-04 引入了几项新的可靠性事实。如果 B-05 不覆盖它们，这些事实就是"实现了但不可观测"，
与 B-05 的目标直接冲突：

| 新增 | 内容 | 为什么单独成项 |
|---|---|---|
| `OutcomeLeaseHeld` | 事件被他人活跃租约持有时上报独立结果 | 持有时**没有失败**，并入 `retried` 会让人以为处理器在报错，而实际是另一消费者持有工作、或持有者已死但租约未到期；且它不消耗重试预算 |
| `ncs_worker_dead_letter_suppressed_total` | 死信守卫因事件已停放而**跳过写入**时计数 | 与"已写入死信"是相反的事实：这里什么都没写。合并会让运维面对的停放积压看起来虚高 |
| `DegradationSampler` | 把 B-01 的 Redis 能力失败计数桥接到指标注册表，按 `fail_open`/`fail_closed` 分别发布**增量** | **没有它，降级策略完全不可观测**：一个缓存悄悄停止缓存、或幂等守卫悄悄停止守卫的部署，在其他所有指标上都显示健康，因为策略本来就把失败吸收了 |
| 启动日志含 `start_id` | `consumer group ready` 行记录 `start_id` 与 `max_attempts` | 起点决定"本进程启动前已发布的积压"会被消费还是被跳过，运维排查"昨晚的事件为何没处理"时正需要它 |

`DegradationSampler` 的两处设计要点：

- 源是**累计值**而计数器需要**增量**，因此每次采样只发布差值。直接发布累计值会让速率计算失真，改成
  仪表又会丢掉计数器应有的单调性。
- 两种模式**分别跟踪**而不是从总数推导。推导会在能力切换模式时重复计数；而这两个标签对运维含义相反
  （fail-open 是隐藏了失败，fail-closed 是拒绝了流量）。

## 验证记录（第 2 版）

## 变更范围

### 新增文件

| 文件 | 行数 | 内容 |
|---|---:|---|
| `internal/repository/redis/stream_inspector.go` | 约 250 | `XINFO STREAM`/`XINFO GROUPS`/`XLEN` 解析；`StreamInfo`/`GroupInfo`；`EffectiveLag` |
| `internal/observability/registry.go` | 约 190 | 线程安全指标注册表（计数器/仪表、标签、有序快照） |
| `internal/observability/observer.go` | 约 120 | `Observer` 契约、`Outcome` 闭集、Registry 实现、Noop |
| `internal/observability/trace.go` | 约 80 | trace_id 上下文传递 + `slog` 上下文处理器 |
| `internal/observability/collector.go` | 约 190 | 周期采样 lag/pending/length/死信长度并上报 |
| 测试 4 个文件 | — | inspector（单元 + 真实 Redis）、registry/trace、collector、worker 插桩 |

### 修改文件（均为 B 线自有）

| 文件 | 改动 |
|---|---|
| `internal/repository/redis/settings.go` | `Capabilities.Inspector`（复用同一个连接池的 `*StreamsClient`） |
| `internal/worker/worker.go` | `SetObserver`/`SetLogger`；启动恢复上报；带 trace_id 的结构化日志；死信原因标签 |
| `internal/worker/runner.go` | 把 observer 与 logger 转发到每个流的 Worker |
| `cmd/worker/main.go` | trace 感知日志、采样间隔配置、启动/周期/退出三处指标快照 |

### 明确未修改

未触碰 `cmd/api/`、`internal/config/`、`internal/httpapi/`、`internal/auth|order|admin|station|charger|wallet/`、
`internal/repository/postgres/`、`backend/migrations/`、`go.mod`、`go.sum`、`api/`、`.github/`。

## 契约和数据

### 新增或修改 API

**无 HTTP 接口新增。** 这一点需要说明：契约第 4.1 节规定"B 线对外提供的健康检查、事件管理和运维接口
必须先登记到 OpenAPI，再实现"，而 `api/openapi.yaml` 属集成人员范围。因此本模块**不实现**
`/metrics` 或运维端点，改为把快照写入日志（周期可配置），并在下方给出建议的 OpenAPI 片段供您决定。
`internal/httpapi/` 亦属 A 线。

### PostgreSQL 迁移

无。

### Redis Key/Stream

未新增或改名任何键。只**读取**既有流的状态，并新增以下指标序列（非 Redis 键）。

### 幂等和并发策略

- 注册表由多个流的 Worker 并发写入（`-race` 下 16 协程 × 100 次写入的测试通过）。
- 标签基数按构造有界：stream 取自冻结的流清单，event_type 经 `boundedEventTypeLabel` 收敛到冻结
  事件清单（其余为 `unknown`），outcome 与 reason 为闭集类型，attempt 上界为**常量 20**（`20+`）。
  **不使用** event_id、charger_id 等无界值作标签，以免序列集随流量增长。
  > 第 2 版的这一条写的是"attempt 上界为 `MaxAttempts`"，**这是错的**：`MaxAttempts` 是配置项而不是
  > 常量，把它当上界等于让基数由部署配置决定。第 3 版修复 5 已改为常量并补测试。
- `Collector.Collect` 单个流的失败不阻断其他流，并计入 `ncs_observability_collector_errors_total`。

## 真实 Redis 语义核实（实现前先验证，避免凭假设写解析器）

用 `redis-cli` 直接确认了 Redis 7.0.15 的实际回复，其中两点会影响设计：

1. `XINFO STREAM` 是**扁平字段数组**，且内嵌 `first-entry`/`last-entry` **嵌套数组**。
   解析器按索引成对遍历并校验字段数为偶数——若静默丢弃尾字段，会把积压报得更健康。
2. `XINFO GROUPS` 的 `entries-read` 与 `lag` 在无法计算时返回 **nil**（实测：组以 `$` 创建时
   `entries-read` 为空、`lag` 为 0）。因此二者以 `-1` 表示未知，并提供 `EffectiveLag`：
   服务端 lag 可用则直接用；否则用 `entries-added - entries-read` 推导，并夹紧为非负。
   **不报告 0**——那会让运维看到"健康"，这是最不该给出的答案。

顺带验证并固化了一个容易误解的语义（`TestIntegrationGroupCreatedAtHeadReportsNoLag`）：组以 `$`
创建时被定位在流末尾，**已存在的历史条目不会被投递、也不计入 lag**。

**此处更正我上一版的一处错误结论。** 我上一版写的是"Worker 用 `$` 创建消费组，这是刻意的（首次部署
不应回放全量历史）"，并据此认为无需处理。**那是错的**：对 P0 闭环而言，跳过"先由 Outbox 发布、Worker
后才首次启动"的积压不是设计取舍而是数据丢失。BE-B-04 复审已把该缺陷列为 P0，其默认起点已改为
`0-0`（`worker.Config.StartID`）。

因此本模块的这一测试现在只声明**Redis 语义**：只有运维显式把起点配成 `$` 时才会发生，而不是 Worker
的默认行为。测试注释已同步更正。

## 验证记录（第 1 版，rebase 前）

在 `charging-station-platform-backend-events/backend` 下执行，Redis 版本 `7.0.15`：

```text
命令：gofmt -l .
结果：无输出

命令：go vet ./...
结果：通过，无告警

命令：NCS_REDIS_TEST_ADDR=127.0.0.1:6379 go test -race ./...
结果：全部 ok（含 observability 1.096s、repository/redis、worker）

命令：NCS_REDIS_TEST_ADDR=127.0.0.1:6379 go test -count=20 -race ./internal/repository/redis/ ./internal/worker/ ./internal/observability/
结果：repository/redis ok 101.324s；worker ok 2.847s；observability ok 2.452s；
      执行后 db15 dbsize = 0（无残留键）

命令：NCS_REDIS_TEST_ADDR=127.0.0.1:6379 go test -cover ./internal/repository/redis/ ./internal/worker/ ./internal/observability/ ./internal/event/
结果：repository/redis 86.5%；worker 86.5%；observability 98.8%；event 75.4%

命令：git status --porcelain | grep -E "go\.mod|go\.sum|api/|internal/(httpapi|repository/postgres)/"
结果：无匹配
```

- 测试函数：redis 287 个、worker 59 个、observability 30 个、event 13 个
- `observability` 覆盖率 98.8%

### 端到端实测：重启恢复 + trace ID + 全部指标

用真实 Redis 构造了"上一个进程已投递但未确认"的状态，然后启动 Worker：

```text
$ redis-cli XREADGROUP GROUP g dead-consumer COUNT 2 STREAMS probe:recover >
$ redis-cli XPENDING probe:recover g     → 2 条 pending，归属 dead-consumer

$ NCS_REDIS_DB=15 NCS_WORKER_SAMPLE_INTERVAL=1s ./ncs-worker -streams 'probe:recover=g'

{"msg":"stream state at startup","metrics":"ncs_stream_dead_letter_length=0
   ncs_stream_lag{stream=\"probe:recover\"}=0 ncs_stream_length{stream=\"probe:recover\"}=2
   ncs_stream_pending{stream=\"probe:recover\"}=2"}
{"msg":"recovered pending entries","stream":"probe:recover","group":"g",
   "consumer":"worker-69535-probe-recover","recovered":2}
{"msg":"event consumed","event_id":"evt_r1","event_type":"CHARGE_STARTED","aggregate_id":"order_r1",
   "stream_id":"1789400755318-0","attempt":2,"outcome":"succeeded","trace_id":"trace_recover_1"}
{"msg":"event consumed","event_id":"evt_r2",...,"attempt":2,"outcome":"succeeded",
   "trace_id":"trace_recover_2"}
{"msg":"stream state at shutdown","metrics":"... ncs_stream_pending{stream=\"probe:recover\"}=0
   ncs_worker_events_total{...outcome=\"succeeded\"...}=2
   ncs_worker_pending_recovered_total{stream=\"probe:recover\"}=2"}
```

这一条链路同时证明了：pending 可见（2→0）、重启恢复已上报并落指标、每条业务日志带
`trace_id`、重试次数可见（`attempt:2`）、死信为 0。另一次三流启动实测显示 `lag`/`length`/`pending`
三个流各自独立上报。

## 建议的 OpenAPI 片段（需您决定是否登记）

按第 4.1 节，B 线的运维端点必须先登记再实现。若您同意，建议在 `api/openapi.yaml` 增加：

```yaml
  /ops/worker/metrics:
    get:
      summary: B 线 Worker 与 Stream 指标快照
      operationId: getWorkerMetrics
      responses:
        '200':
          description: 指标快照
          content:
            application/json:
              schema:
                type: object
                properties:
                  samples:
                    type: array
                    items:
                      type: object
                      properties:
                        name:   { type: string }
                        kind:   { type: string, enum: [counter, gauge] }
                        value:  { type: number }
                        labels: { type: object, additionalProperties: { type: string } }
```

登记后由 A 线在 `internal/httpapi/` 暴露（只需 `Collector` 已有的快照），B 线不改该目录。

## 未包含范围

- **`/metrics` 或运维 HTTP 端点**：见上。当前以日志方式上报，周期由
  `NCS_WORKER_SAMPLE_INTERVAL`（默认 30s）控制。
- **指标导出到外部系统**（Prometheus/OTLP 等）：需要第三方依赖，而共享锁文件需您统一修改；
  本模块只提供进程内注册表与文本快照，导出适配器可作为后续模块。
- **延迟直方图**：当前只有计数与仪表。契约未要求延迟分布，且直方图需要定义分桶边界（属产品决策）。
- **消费结果落库的 PG 实现、设备命令派发、域应用**：均属 A 线或充电桩网关，见 B-04 审批单。
- **CI 仍未覆盖 `backend/`**：建议尽快补齐。

## 风险和回滚

- 已知风险：
  1. **`StreamInspector` 依赖 `XINFO`**：非 Redis 实现（如某些托管服务的兼容层）可能不支持
     `XINFO`，或返回不同的字段集。解析器对缺失字段取默认值并对奇数长度报错，但 `XINFO` 完全不可用时
     采集会持续失败并计入错误计数器——不会影响消费主链路（采集失败只记日志）。
  2. **`pendingScanLimit`（1024）**：`XPENDING` 扩展形式在 `Pending`/`Claim` 中按上限扫描，
     与 B-04 的风险项同源，采样不受影响（`XINFO GROUPS` 直接给 pending 总数）。
  3. **日志量**：周期快照是单行包含全部序列。序列数约为 `3 流 × (3 仪表 + 1 lag + 1 pending + 1 length)`
     量级，单行不会过大；但若后续加入更多标签需重新评估。
  4. **`Collector.Run` 与 Worker 共享进程**：采集是同步 Redis 往返，间隔默认 30s，开销可忽略；
     不设为秒级以下即可（实测 1s 间隔无异常）。
- 回滚方式：`git revert 0ed8641`（第 2 版）、`git revert 1e80890`/`7cbd91f`（第 1 版），
  第 3 版代码 `git revert 0cb9934`。
  未引入依赖、未改 `go.mod`/`go.sum`、未改 OpenAPI、未改迁移 SQL。
- 是否影响旧 C++ 系统：否。

## 提交记录

| 版本 | 基点 | 代码 | 材料 | 说明 |
|---|---|---|---|---|
| 第 1 版 | `7a1e33e` | `271de62` | `83eabdc` | 首次提交（rebase 前，你审查的是 `040a27d` 这一版） |
| 第 2 版 | `a6f4b0a` | `0ed8641` | — | 第 1 次 rebase + 语义对齐 |
| 第 3 版 | `d6b257a` | `0cb9934` | `4c4b8b9` | 第 1 轮审查意见的 6 项修复（第 2 次 rebase） |
| 第 4 版 | `22fd8f6` | `ba0ee34` | `71a1d01` | 第 3 次 rebase 到四次修复后的 B-04 |
| 第 5 版 | `ad4c524` | `553425b` | `90b6fb7` | 第 4 次 rebase 到五次修复后的 B-04 |
| **第 6 版（当前）** | **`b728d8c`** | **`3e4721b`** | 本文件所在提交 | 第 5 次 rebase 到六次修复后的 B-04 |

- 回滚：第 6 版代码 `git revert 3e4721b`；历史版本可用保险分支对照：`b05-pre-rebase`（第 1 版）、
  `b05-pre-rebase3`（第 2 版）、`b05-pre-rebase4`（第 3 版）、`b05-pre-rebase5`（第 4 版）、
  `b05-pre-rebase6`（第 5 版）。
- 分支基点义务：B-04 若再有改动，本分支仍需 rebase。

## 第 2 版验证记录

在 `charging-station-platform-backend-events/backend` 下执行（分支 `b-05-recovery-observability`，
基点 `a6f4b0a`），Redis 版本 `7.0.15`：

```text
命令：gofmt -l .
结果：无输出

命令：go build ./...
结果：通过

命令：go vet ./...
结果：通过，无告警

命令：NCS_REDIS_TEST_ADDR=127.0.0.1:6379 go test -race ./...
结果：全部 ok（含 observability、repository/redis 6.188s、worker 2.093s、cmd/worker）

命令：NCS_REDIS_TEST_ADDR=127.0.0.1:6379 go test -count=20 -race ./internal/repository/redis/ ./internal/worker/ ./internal/observability/ ./internal/event/ ./cmd/worker/
结果：repository/redis ok 99.449s；worker ok 23.093s；observability ok 2.349s；
      event ok 1.074s；cmd/worker ok 1.015s；执行后 db15 dbsize = 0

命令：NCS_REDIS_TEST_ADDR=127.0.0.1:6379 go test -cover ./...
结果：observability 99.0%；repository/redis 86.5%；worker 85.4%；event 79.0%；config 100.0%
```

### 第 2 版过程中修复的两个测试缺陷（仅测试代码）

1. **积压集成测试的确认竞态**：`TestIntegrationRedisConsumesBacklogPublishedBeforeGroupCreation`
   在 handler 已看到全部事件后立刻 `cancel()`，与最后一条 ACK 竞争，间歇性留下 1 条 pending。
   修复：先等待 pending 清零再取消。
2. **阻塞读的时间下界**：原断言要求 400ms 阻塞至少 200ms，但在**多个测试包同时对同一 Redis 施压**时
   间歇失败（实测出现过 78ms 返回），而单独运行该测试 30 次全部通过。我没有查明这个早返回的成因，
   因此**移除该时间断言而不是继续调参**：真正要防的"阻塞读完全不阻塞"已由
   `TestStreamsReadGroupFramesBlockAndNoAck` 用脚本化服务端**确定性地**断言（检查发出的就是
   `BLOCK <ms>`）。测试注释中记录了这一判断，避免以后有人盲目把下界加回来。

### 一处能力损失需要你知悉

BE-B-04 修复后，`cmd/worker` 在缺少领域适配器时**拒绝启动**。因此本模块**无法再用真实二进制演示**
`stream lag`/`pending`/`dead_letter` 快照（第 1 版的那次端到端实测是在修复前的二进制上完成的，其
观测逻辑未变但已无法复现）。第 2 版的观测路径全部由**单元测试与真实 Redis 集成测试**覆盖：

| 新能力 | 覆盖它的测试 |
|---|---|
| `OutcomeLeaseHeld` 独立结果 | `TestOutcomeLeaseHeldIsDistinctFromRetried`、`TestWorkerDoesNotAcknowledgeAnEventHeldByALiveLease` |
| 死信写入被抑制（已完成写入存在） | `TestWorkerReportsSuppressedDeadLetterWrites`、`TestRegistryRecordsSuppressedDeadLettersSeparately` |
| 死信写入被他人持有（结果未知，条目保持 pending） | `OutcomeDeadLetterHeld`；B-04 侧构造 `TestWorkerLeavesTheEntryPendingWhileAnotherConsumerIsWritingTheDeadLetter` |
| 降级桥接 | `TestDegradationSamplerPublishesTheInitialCount`、`...PublishesOnlyDeltas`、`...SplitsFailOpenFromFailClosed`、`...HandlesAModeSwitch`、`...RebaselinesAfterAReset`、`TestCollectorSamplesDegradationWhenConfigured` |
| 启动日志含起点 | `TestWorkerLogsTheConsumerGroupStartPosition` |

## 第 6 版：第五次 rebase（到六次修复后的 BE-B-04）

B-04 第 6 轮修掉的两条链路，都是本模块依赖的东西：**没有 reservation 就没有所有权检查**，**没有声明租约就没有
互斥**。两条都不改变本模块的指标语义，但都会让"被取代的投递必须静默"这条结论失效——所以本版只做复验，并记录
一处与观测相关的对齐。

### 与 B-04 第 6 轮的对齐

| B-04 第 6 轮的新事实 | 本模块的对应 |
|---|---|
| `pipeline` 的普通失败返回也携带 `reservation` | 无需改动；这正是 `OutcomeSuperseded` 能真正出现的前提（此前每次永久失败都会因空 reservation 被拒，静默的理由是错的） |
| 死信声明建立自己的租约，`Begin` 将其视为进行中 | 无需改动：声明互斥期变长意味着 `dead_letter_held`（他人正在写）出现的窗口更短，而"写入失败后立刻可重试"由 `AbortDeadLetter` 保证 |
| `Fail`/`Release` 不再清除声明的租约 | 无需改动 |
| 写入失败后新增 `AbortDeadLetter` 释放权威声明 | `DeadLetterRecorder` 相应新增 `AbortDeadLettered`（接口随 B-04 一起进来，本模块只做透传） |

### 冲突解决

`internal/worker/worker.go` 一处：B-04 新增了 `abortDeadLetter`（其参数为 `deadLetterReason`），本模块的
`finalizeDeadLetter` 已把 reason 收窄为 `deadLetterReason`。合并结果：保留 B-04 的 `abortDeadLetter`，并把它的
参数类型与调用点统一为 `deadLetterReason`（对外仍以字符串传给存储），其余两侧都保留。

### 第 6 版验证记录（rebase 后）

```text
命令：gofmt -l ./cmd ./internal
结果：无输出

命令：go vet ./... / go build ./...
结果：均通过

命令：NCS_REDIS_TEST_ADDR=127.0.0.1:6379 go test -count=1 -race -timeout 300s ./...
结果：全部 ok（observability 1.121s、repository/redis 5.912s、worker 2.098s、cmd/worker 1.011s）

命令：NCS_REDIS_TEST_ADDR=127.0.0.1:6379 go test -count=20 -race -timeout 900s \
      ./internal/repository/redis/ ./internal/worker/ ./internal/observability/ ./internal/event/ ./cmd/worker/
结果：repository/redis ok 99.538s；worker ok 23.013s；observability ok 2.140s；
      event ok 1.060s；cmd/worker ok 1.031s；执行后 db15 dbsize = 0

命令：NCS_REDIS_TEST_ADDR=127.0.0.1:6379 go test -count=1 -cover ./...
结果：observability 99.1%；repository/redis 86.0%；worker 85.8%；event 73.5%；config 100.0%；
      httpapi 83.3%；cmd/worker 25.8%
```

新增测试：`TestWorkerReportsASupersededDeliveryWithoutCountingADeadLetter` 仍覆盖 `superseded` 只上报投递结果；
B-04 第 6 轮的 `TestAPermanentFailureThroughThePipelineCarriesItsReservation` 与本模块的
`TestAStaleDeliveryThroughThePipelineStaysSilentAfterTheEventWasApplied` 一起，保证 `superseded` 是在真实
pipeline 上产生的，而不是靠手工构造。

## 第 5 版：第四次 rebase（到五次修复后的 BE-B-04）

B-04 第 5 轮修掉的是"旧消费者覆盖新消费者成功终态"，而**被错误计入指标的正是这条路径**：一个超出自身租约
的投递会走死信路径，于是它写的死信与它上报的指标都会落到运维面前。本版据此做两件事。

### 新增结果 `superseded`（`OutcomeSuperseded`）

B-04 第 5 轮让这类投递**完全静默**：不写 DLQ、不写记录、不 ACK。静默是传输与记录层面的事实，不是指标层面
的：这种投递必须被看见，因为"处理器跑过了自己的租约"在其他任何序列里都不可见——它意味着租约短于作业、或
消费者比部署假设的慢，而这两件事会在变成重复工作之前先变成告警。

因此 `parkDeadLetter` 现在按四种结果分别上报：

| 停放路径的结果 | 投递结果指标 | 写入计数 | 日志 |
|---|---|---|---|
| `parked`（写出并 ACK） | `dead_lettered` | +1（写完并 ACK 之后） | 按 reason 定级别 |
| `skipped`（已有完成写入） | `dead_lettered` | 不计数（改为"被抑制" +1） | info |
| `deferred`（他人正在写） | `dead_letter_held` | 不计数 | warn |
| `superseded`（事件已属于更新代次） | **`superseded`** | **不计数** | warn |

**被取代的投递不再计入死信**，这正是审查意见指出的问题。

### 与 B-04 第 5 轮的对齐

| B-04 第 5 轮的新事实 | 本模块的对应 |
|---|---|
| 死信决定改为存储上的两阶段（`BeginDeadLetter` 原子声明 + `FinalizeDeadLetter` 比较并交换） | `DeadLetterRecorder` 改为对应两个方法；`deadLetter` 的第 1 步与第 5 步分别调用它们，`false`/`superseded` 时直接返回 `deadLetterSuperseded`，不上报写入计数、不 ACK |
| 被取代的投递必须完全静默 | 新增 `OutcomeSuperseded` 并只上报投递结果；不新增任何"已停放"计数 |
| 缺少死信记录器时拒绝消费 | `checkReadyToConsume` 从 `Run` 移到 **`Prepare`**：runner 走的是 `Prepare` + `Serve`，放在 `Run` 里会被绕过。`cmd/worker/main_test.go` 与三个既有测试已相应接线记录器 |

### 冲突解决

1. `internal/worker/worker.go`（两处，均为"两侧都保留"）：B-04 把停放路径改成"三态声明 + 两阶段记录 + 四种
   结果"，本模块把"上报时机"改成"动作成功后才上报"、并让 `parkDeadLetter` 统一负责日志与结果上报。合并
   结果：保留 B-04 的四结果结构，`DeadLetterSuppressed` 与 `EventDeadLettered` 都在 ACK 之后上报，
   `superseded` 只上报投递结果。
2. `cmd/worker/main_test.go`、`internal/worker/observability_test.go`、`internal/worker/recovery_review_test.go`：
   这些测试运行 Worker/Runner，因此需要接线死信记录器（新规则），已逐一补齐。

### 第 5 版验证记录（rebase 后）

```text
命令：gofmt -l ./cmd ./internal
结果：无输出

命令：go vet ./... / go build ./...
结果：均通过

命令：NCS_REDIS_TEST_ADDR=127.0.0.1:6379 go test -count=1 -race -timeout 300s ./...
结果：全部 ok（observability 1.099s、repository/redis 6.020s、worker 2.110s、cmd/worker 1.017s）

命令：NCS_REDIS_TEST_ADDR=127.0.0.1:6379 go test -count=20 -race -timeout 900s \
      ./internal/repository/redis/ ./internal/worker/ ./internal/observability/ ./internal/event/ ./cmd/worker/
结果：repository/redis ok 100.177s；worker ok 22.997s；observability ok 2.358s；
      event ok 1.107s；cmd/worker ok 1.049s；执行后 db15 dbsize = 0

命令：NCS_REDIS_TEST_ADDR=127.0.0.1:6379 go test -count=1 -cover ./...
结果：observability 99.1%；repository/redis 86.0%；worker 85.8%；event 78.7%；config 100.0%；
      httpapi 83.3%；cmd/worker 25.8%
```

新增测试：`TestWorkerReportsASupersededDeliveryWithoutCountingADeadLetter`（断言结果是 `superseded`、且死信
计数与被抑制作数都为 0）。

## 第 4 版：第三次 rebase（到四次修复后的 BE-B-04）

第 1 轮审查的 6 项修复已经在"第 3 版"一节里；本版是把它 rebase 到 **B-04 第 4 轮修复后**的顶端，并让观测层
跟上 B-04 新的可靠性语义。B-04 第 4 轮修掉的四项里有 **两项正好是本模块要观测的东西**，因此这里不是单纯的
冲突解决，而是补上"实现了但不可观测"的部分。

### B-04 第 4 轮带来的新事实，以及本模块的对应改动

| B-04 的新事实 | 本模块的对应 |
|---|---|
| 死信声明改为三态（`taken`/`written`/`writing`），`writing` 时**不记录终态、不 ACK** | 新增结果 `OutcomeDeadLetterHeld`（`dead_letter_held`）：与 `dead_lettered` 区分，因为事件**尚未**被停放，另一方的写入仍可能失败；也与 `lease_held` 区分，因为被持有的是**停放写入**而不是处理预留 |
| `parkDeadLetter` 承担"停放路径实际达成了什么" | `parkDeadLetter` 同时成为本模块唯一的停放上报点：`parked`/`skipped` 上报 `dead_lettered`，`deferred` 上报 `dead_letter_held`；写入计数仍在死信路径内部、写完并 ACK 之后才 +1 |
| 新增 `ConsumptionStore.Release` + `OutcomeAbandoned`（等待 Guard 不再消耗重试预算） | 无需改动：本模块只上报 Worker 给出的结果，预算语义的变化对指标无影响，但文档已同步 |
| 新增 `StreamConfig.StartID` 透传 | 无需改动；`cmd/worker` 的 `servePipeline` 与 `SetOnReady` 结构与 B-04 的改动无重叠 |

### 冲突解决（两处，均为"两侧都保留"）

1. `internal/worker/worker.go`：B-04 把死信路径改成"三态声明 + 返回结果"，本模块把"上报时机"改成"动作成功后
   才上报"。合并结果：保留三态分支，把 `DeadLetterSuppressed` 与 `EventDeadLettered` 都放在 ACK 之后，
   并让 `parkDeadLetter` 统一负责三态各自的日志与结果上报（原先三个调用点各写一份日志，现在一处）。
2. `internal/repository/redis/streams_client_integration_test.go`、`internal/worker/integration_redis_test.go`：
   两处测试缺陷的修复在 B-04 第 4 轮已经先落了一次（原因相同），因此这里保留 B-04 版本，避免同一处出现两份
   措辞不同的注释。

另有两处 `settings.go` / `cmd/worker/main.go` 的机械冲突（字段对齐、注释），按"两侧都保留"处理。

### 第 4 版验证记录（rebase 后）

```text
命令：gofmt -l ./cmd ./internal
结果：无输出

命令：go vet ./... / go build ./...
结果：均通过

命令：NCS_REDIS_TEST_ADDR=127.0.0.1:6379 go test -count=1 -race -timeout 300s ./...
结果：全部 ok（observability 1.096s、repository/redis 6.022s、worker 2.140s、cmd/worker 1.016s）；
      执行后 db15 dbsize = 0

命令：NCS_REDIS_TEST_ADDR=127.0.0.1:6379 go test -count=20 -race -timeout 900s \
      ./internal/repository/redis/ ./internal/worker/ ./internal/observability/ ./internal/event/ ./cmd/worker/
结果：repository/redis ok 99.057s；worker ok 23.104s；observability ok 2.406s；
      event ok 1.088s；cmd/worker ok 1.049s；执行后 db15 dbsize = 0

命令：NCS_REDIS_TEST_ADDR=127.0.0.1:6379 go test -count=1 -cover ./...
结果：observability 99.1%；repository/redis 86.0%；worker 85.0%；event 79.3%；config 100.0%；
      httpapi 83.3%；cmd/worker 25.8%
```

## 第 3 版：第 1 轮审查意见的修复

第 1 轮审查的 6 项意见已全部修复。第 3 版只改 B 线自有文件，未新增依赖，未触碰
`go.mod`/`go.sum`/`api/`/A 线目录。

| # | 意见 | 修复 | 反向验证 |
|---|---|---|---|
| 1 | P0 报告循环的生命周期：Worker 失败后进程无法退出 | 抽出 `servePipeline`，采样循环改用自己派生、可主动取消的 context；消费结束后 `stopReporting()` 并等待其退出 | 改回"使用信号 context"的修复前形状后 `TestPipelineReturnsWhenAWorkerFailsWithoutASignal` 超时失败（10s 未返回） |
| 2 | P1 指标在动作真正成功之前上报 | 成功/重复分支改为**先 ACK 再上报**；死信分支改为**先写完 DLQ+记录+ACK 再报**写入计数；守卫命中分支改为**只**报"被抑制"，不再同时报写入 | 把三处上报调回 ACK 之前，两个新测试分别失败（观察到 `[succeeded]`、`dead_lettered=1`） |
| 3 | P1 消费组不存在时把 lag 报成 0；启动快照早于消费组创建 | 组不存在时不发布 lag（删除序列），新增 `ncs_stream_group_missing` 仪表；新增 `Runner.SetOnReady`，启动快照改在**全部消费组就绪之后**采样 | 见下方"修复 3 的接口调整"与新增测试 |
| 4 | P1 无法计算的 lag 保留旧值 | 新增 `Registry.DeleteGauge` + `Collector.clearLag`：lag 不可知时删除该序列 | `TestCollectorClearsALagGaugeThatCanNoLongerBeComputed`、`TestCollectorClearsLagWhenTheGroupDisappears` |
| 5 | P1 标签基数可由流量或配置决定 | `boundedEventTypeLabel` 收敛 event_type；死信原因改为闭集类型 `deadLetterReason`，自由文本移入 `dead_letter_detail`；attempt 标签上界由 `MaxAttempts` 改为常量 20（`20+`） | `TestWorkerBoundsTheEventTypeLabel`、`TestWorkerKeepsFreeTextOutOfTheDeadLetterReason`、`TestAttemptLabelIsBoundedByAConstant` |
| 6 | P2 退出快照复用了周期快照 | 退出时用 5s 超时的独立 context **重新采样** | 去掉重新采样后 `TestPipelineResamplesStreamStateAtShutdown` 失败（采样次数 0） |

### 修复 1：报告循环不再依赖信号

修复前的形状是：采样循环使用进程信号 context，并在 `runner.Run` 返回后等待它结束。Worker 失败只取消
runner 的 context，不触达信号，因此那个等待永不返回——进程既不消费也不退出，supervisor 无法区分它与
挂死。现在运行期被抽成 `servePipeline`，采样循环持有自己派生并可主动取消的 context，消费结束即由
`stopReporting()` 结束并等待 `reportingDone`。

`servePipeline` 是 `package main` 里的函数而不是内联在 `main()` 中，就是为了这条意见可被测试：
`main()` 的其余部分是进程装配并以 `os.Exit` 结束，无法在测试里断言。

### 修复 2：先完成，再计数

指标是对"发生了什么"的断言，只能在断言成真之后发出：

- 成功与重复两个分支：**ACK 成功之后**才上报。此前先上报，ACK 失败会把同一条投递退回重试，于是同一
  个事件被记成"成功"又被记成"重复"，成功率虚高而没有多干任何活。
- 死信分支：`ncs_worker_dead_lettered_total` 现在只在**DLQ 已写入、消费记录已写、源条目已 ACK** 之后
  才 +1。此前先计数，写入或 ACK 失败后重试会把同一个事件计两次，停放积压看起来是实际的两倍。
- 守卫命中（写入被跳过）分支：**只**报 `ncs_worker_dead_letter_suppressed_total`，不同时报写入计数。
  两者是相反的事实，同时上报等于把"没写"也算成"写了"。

### 修复 3：lag 只在可信时发布，启动快照在就绪之后

消费组不存在时没有"落后多少"这个量：流里已有的条目尚未被任何组投递，是否会被消费取决于建组起点。
旧实现把它报成 `lag=0`，于是"首次部署、流里堆满未消费事件"看起来完全健康。现在该状态上报
`ncs_stream_group_missing=1`，并且**不发布** `ncs_stream_lag`；`pending=0` 与 `length` 照常上报，
运维仍能看到积压确实存在于流里。

**修复 3 的接口调整（B 线自有包，需你知悉）：**

- `Worker` 拆出 `Prepare`（Ping → EnsureGroup → 首次 pending 恢复）与 `Serve`（消费循环），`Run` 仍是
  两者的组合，语义不变；
- `Runner` 先准备**全部**流，再调用 `SetOnReady`，之后才开始消费。因此"就绪"意味着"每个消费组都已存在
  且没有任何流在被消费"；
- `Prepare` 不再吞掉因取消产生的错误——等待就绪的一方不该在未就绪时被告知就绪；"关闭期间被取消"由
  `Worker.Run` 与 `Runner.Run` 识别为**干净停止**而非启动失败（`TestRunnerTreatsACancelledPreparationAsACleanStop`）。

### 修复 5：基数由契约决定，不由流量或配置决定

- `event_type` 来自信封，而信封只要求它非空：任何能发消息的人都能造出无限多个取值。非冻结清单的取值
  现在一律记为 `"unknown"`。
- 死信原因此前是自由文本（`"invalid_event: " + err.Error()`），同样可被发布者影响。现在 `reason` 是闭集
  类型，错误文本作为 `dead_letter_detail` 写入死信，运维要读的原文一条不少。
- attempt 标签此前以 `MaxAttempts` 为"上界"，但那是**配置项**：把它当上界等于让基数由部署配置决定。
  现在上界是常量 20，超过即 `"20+"`（到第 20 次尝试仍是问题，与预算具体是多少无关）。

### 第 3 版新增测试

| 文件 | 数量 | 覆盖 |
|---|---:|---|
| `internal/observability/collector_review_test.go` | 7 | 组缺失时不报 lag / 组存在时正常报 / 不可知时清除旧值 / 组消失时清除 / 采样失败保留上一读数 / `DeleteGauge` 只删指定序列 / attempt 标签上界 |
| `internal/worker/recovery_review_test.go` | 7 | ACK 失败不报成功、ACK 失败不计死信、被抑制不重复计数、event_type 收敛（含经 Router 被拒后停放的路径）、自由文本不入标签、就绪在所有组就绪之后、准备失败报错且不报就绪、取消的准备视为干净停止 |
| `cmd/worker/main_test.go`（既有文件，本版**新增** 2 个；原有 2 个"领域适配器未接线则拒绝启动"的测试保持不变） | 2 | Worker 失败后报告循环随之结束（进程可退出）、退出快照是新采样 |

三个文件均包含反向验证：每条断言对应的修复被临时回退后测试必须失败（上表最后一列记录了实际观察到的
失败输出）。已同步修正因语义变化而需要改动的既有测试：`collector_test.go`（组缺失时不再断言 lag=0）、
`observability_test.go`、`reliability_test.go`、`worker_test.go`（死信原因标签 + `dead_letter_detail`）。

### 第 3 版验证记录

在 `charging-station-platform-backend-events/backend` 下执行（分支 `b-05-recovery-observability`，
基点 `d6b257a`，代码提交 `0cb9934`），Redis 版本 `7.0.15`：

```text
命令：gofmt -l ./cmd ./internal
结果：无输出

命令：go vet ./...
结果：通过，无告警

命令：go build ./...
结果：通过

命令：NCS_REDIS_TEST_ADDR=127.0.0.1:6379 go test -count=1 -race -timeout 300s ./...
结果：全部 ok（cmd/worker 1.015s、observability 1.096s、repository/redis 6.004s、worker 2.113s，
      config、event、httpapi 均 ok）

命令：NCS_REDIS_TEST_ADDR=127.0.0.1:6379 go test -count=20 -race -timeout 900s \
      ./internal/repository/redis/ ./internal/worker/ ./internal/observability/ ./internal/event/ ./cmd/worker/
结果：repository/redis ok 100.431s；worker ok 22.771s；observability ok 2.148s；
      event ok 1.045s；cmd/worker ok 1.026s；执行后 db15 dbsize = 0

命令：NCS_REDIS_TEST_ADDR=127.0.0.1:6379 go test -count=1 -cover ./...
结果：observability 99.1%；repository/redis 86.5%；worker 84.4%；event 78.3%；config 100.0%；
      httpapi 83.3%；cmd/worker 10.8%（本版新增的 servePipeline 测试带来）
```

一次说明：上述 20 次重复运行**曾被我自己中断过一次**（在写文档的同时启动，随后发现源码仍在改动，
结果不可用），中断运行留下的 1 个集成测试流随后被单独复跑（`-count=3`）确认为测试自身会清理、
`dbsize` 回到 0。表中数值来自**冻结代码后的完整重跑**。

## 审批结论

```text
状态：PENDING
审批人：
审批时间：
修改要求：
```
