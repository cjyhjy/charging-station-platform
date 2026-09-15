# 迁移模块审批单：BE-I-01 后端闭环集成（跨线）

本文件按 `docs/migration/module-approval-template.md` 填写。

**状态说明（第 6 版）：七项交付已实现并验证；第 4 版修复第 3 版审查的 5 项，第 5 版修复并发幂等穿透，
第 6 版修复模拟网关状态机的两条提前终止分支；模块口径按审查结论收窄为"事件基础设施与 RESTART 命令闭环"。** 第 1 版（`20caddf`）只有文档、没有代码，被退回；第 2 版
修正了四处技术错误并冻结了集成人员的五项决定；第 3 版是**实现 + 验证记录**。

实现提交（分支 `codex/migration/i-01-closed-loop-integration`，基于含 A 线 `3105659` 与 B 线 `be0472d` 的基线）：

| 提交 | 内容 |
|---|---|
| `233a809` | 迁移 0006（消费记录 + outbox 索引，含 down）、PG `OutboxSource` + advisory lock、PG `ConsumptionStore` + 契约测试、`cmd/outbox-publisher` |
| `6615f65` | 订单/命令 Applier、HTTP Dispatcher、`cmd/mock-gateway`、`cmd/worker` 正式接线 |
| `a9897bb` | 修复闭环验证发现的真实缺陷：charge Applier 重复应用已提交的事务 |
| `48f7827` | 重复投递测试（真实 PG 消费记录 + 真实 Redis Stream） |
| `828f848` | 第 3 版审查的 5 项修复：命令结果决定充电桩状态、发布锁存活检查、验证脚本防误连门禁与口径收窄、模拟网关真实幂等 |
| 本次提交 | 第 4 版审查的并发幂等缺陷：模拟网关的 claim 与记录写入改为同一临界区 |

本版**不含**任何 `approval-log.md` 的修改（决定 5：由审批方在合并门禁更新）。

送审形态的硬性要求（集成人员指定，逐条对应）：

- Go/SQL 实现（七项全部）；
- 正式运行接线（`cmd/worker`、`cmd/outbox-publisher` 可启动并工作）；
- 真实 PostgreSQL + Redis 的全链路结果；
- 重复投递测试（同一事件重复投递不重复应用）；
- 并发发布测试（两个发布者不会同时取得同一 outbox 行）；
- `kill -9` 恢复测试（重启后 pending 被恢复且不重复应用）；
- `go test -race`、`go vet`、`go build` 的完整输出记录。

`approval-log.md` 由审批方在合并门禁更新；开发者只提交验证材料，**不自行填写任何结论**（本版已撤回第 1 版对
该文件的那次编辑）。

## 基本信息

- 模块编号：BE-I-01
- 模块名称：后端闭环集成
- 开发线：**跨线**（A 线 + B 线同时改动）
- 开发分支：`codex/migration/i-01-closed-loop-integration`（**单一分支**，集成人员决定；跨线前缀 `i-` 的格式见
  `directory-and-contract-definition.md` 第 4 节）
- 基线提交：**已在本分支内合入两线最终成果**（集成人员决定 1 的合并，待集成分支收下后再 rebase）：
  - A 线最终提交 `3105659`（合一提交见下）
  - B 线最终链 `be0472d`
  - 合并结果：**无冲突**，`backend/` 同时包含 A 线的 `internal/{admin,auth,order,station,repository/postgres}`、
    `cmd/api`、迁移 `0001`～`0005` 与 B 线的 `internal/{event,observability,repository/redis,worker}`、`cmd/worker`。
- 目标集成分支：`codex/migration-integration`

### 为什么需要这个模块

P0 闭环目前停在"各模块各自通过"的状态。下列缺口都不是任何单一开发线能独立关闭的，因此单独立项：

| 缺口 | 现状 |
|---|---|
| `cmd/worker` 的 Worker 入口 | `domainAdapters()` 固定返回错误，进程**拒绝启动**（这是 B-04 第 1 轮审查后的刻意行为，不是缺陷） |
| 消费记录 | 使用内存 `ConsumptionStore`，进程重启即丢失，**不构成生产幂等** |
| Outbox | B-03 只有 `event.OutboxSource` 抽象，没有 PostgreSQL 实现，也没有发布循环接入运行进程 |
| 订单与命令结果 | `ChargeEventApplier`、`CommandResultApplier` 没有 PostgreSQL 实现 |
| 设备命令 | `CommandDispatcher` 没有真实或可验收的实现 |
| `CHARGER_COMMAND_COMPLETED` | 尚无"产生 → 消费 → 状态回写"的完整链路 |
| 审批日志 | `approval-log.md` 未记录 A-03～A-05、B-04～B-05 的结论；**按决定 5 由审批方在合并门禁更新**，开发者只提交验证材料 |

## 交付范围（固定为七项）

范围由集成人员冻结，任何一项的增加或替换都应先改本文件。

1. **PostgreSQL `OutboxSource` 与独立发布进程**：实现 `event.OutboxSource`
   （`ListUnpublished(ctx, limit)`、`MarkPublished(ctx, id, at)`），并新增**独立进程**
   `cmd/outbox-publisher`（集成人员决定 2）。顺序是**先发布到 Redis Stream，成功后标记 published**：

   ```text
   ListUnpublished(limit) → 逐条 XADD → MarkPublished(id, published_at)
   ```

   - 绝不允许先标记：标记成功而发布失败等于**丢事件**，而重复发布是可被消除的；
   - **发布成功但标记失败**（进程崩溃、数据库瞬断）会产生重复发布，这是允许的：重复由事件的 `event_id`
     与消费者的消费记录共同消除，消费者侧已有对应保证；
   - **单活保证来自 PostgreSQL advisory lock，而不是这个接口**。`ListUnpublished` 返回后数据库事务/行锁已经
     释放，两个发布者完全可能取到同一行——接口本身不构成"claim"。因此发布进程启动时必须先取
     `pg_try_advisory_lock(<固定 key>)`（会话级）：取不到就退出或以只读方式等待，绝不并发发布。
   - 被否决的替代方案：把 `OutboxSource` 扩展为 `Claim/Finalize/Release`（带 owner/lease/token）。它更严格，
     但要改已批准的 B-03 契约，本期不采用；若将来需要多发布者并行，再按新契约走审批。
2. **PostgreSQL `ConsumptionStore`**：实现 `internal/event/consumption.go` 冻结的接口，替换内存实现：
   `Begin`、`Complete`、`Fail`、`Release`、`BeginDeadLetter`、`FinalizeDeadLetter`、`AbortDeadLetter`。
   语义要点（均为 B-04 六轮审查中确立的验收点，实现方不得自行简化）：
   - `Begin` 必须区分 `reserved`/`already_consumed`/`in_progress`，并把 `DEAD_LETTERING` 视为进行中；
   - `Release`（等待他人而放弃）**不得**推进尝试次数；`Fail` 才推进；
   - `BeginDeadLetter` 只能授予当前 owner，且必须建立租约；`FinalizeDeadLetter` 是**比较并交换**，
     返回 `false` 时调用方必须完全静默（不写记录、不 ACK）；
   - `AbortDeadLetter` 仅由声明持有者调用，把声明置回失败态并释放租约。
3. **订单事件与命令结果 Applier**：实现 `worker.ChargeEventApplier`
   （`Apply(ctx, event, attempt) error`）与 `worker.CommandResultApplier`
   （`ApplyCommandResult(ctx, event, attempt) error`）。订单状态推进必须发生在 PostgreSQL 事务中，
   不由前端或事件直接指定目标状态；账务金额用整数分；时间用 UTC。
4. **首版可验收的设备 `CommandDispatcher` / 模拟网关**：实现
   `worker.CommandDispatcher`（`Dispatch(ctx, ChargerCommand, attempt) error`）。首版允许是模拟网关，
   但必须可验收：明确的请求/响应契约、超时与失败语义、重试幂等，以及能够真实产生
   `CHARGER_COMMAND_COMPLETED` 的路径。**它不宣称支持真实设备协议**（见"非目标"）。
5. **正式接线 `cmd/worker`**：注册上述实现，删除内存消费记录占位与"缺少领域适配器即拒绝启动"的占位分支。
   接线后启动日志不得再出现 "using the in-memory consumption store"，且进程能对真实 PostgreSQL + Redis 工作。
   Worker 与发布进程相互独立，可以分别扩容（集成人员决定 2）。
6. **闭环验证**：API 下单 → PG Outbox → Redis Stream → Worker → PG 状态回写 → ACK/Pending/死信，端到端可复现。
7. **审批日志**：本模块只**提交验证材料**；`approval-log.md` 的登记由审批方在合并门禁完成（集成人员决定 5，
   开发者不得自行填写 `APPROVED` 或代为登记其他模块的结论）。

## 变更范围与所有权

| 内容 | 归属 | 说明 |
|---|---|---|
| `backend/internal/repository/postgres/`（OutboxSource、ConsumptionStore、订单/命令仓储） | A 线 | 本模块的主要实现；另新增 `RecordChargerCommandResult`（见"新增/修改的 A 线接口"） |
| `backend/cmd/mock-gateway/`、`backend/scripts/` | B 线（本模块新建） | 开发用模拟网关与闭环验证脚本 |
| `backend/migrations/`（消费记录表、唯一约束与索引、Outbox 索引） | A 线 | 若需新迁移，随本模块一起提交并附回滚脚本 |
| `backend/internal/httpapi/`（下单、命令请求入口） | A 线 | 仅在链路验证需要时补，不新增未登记端点 |
| `backend/cmd/worker/`、`backend/internal/worker/`（接线与适配） | B 线 | 移除占位、注册实现 |
| `backend/internal/event/`、`backend/internal/repository/redis/` | B 线 | 契约**不改**；确需扩展先更新本审批单 |
| `backend/go.mod`、`backend/go.sum`、`api/openapi.yaml`、`docs/migration/approval-log.md` | 集成人员 | 依赖与契约文件不越线修改 |
| 真实设备协议实现（Modbus/OCPP 等）、充电桩网关 | 网关方 | **不在本模块** |

明确未修改：`docs/migration/backend-parallel-development.md` 的模块顺序（本模块的增补已由集成人员确认）、
已批准模块的审批单内容。

## 交付物（第 3 版实际落地的文件）

| 文件 | 内容 |
|---|---|
| `backend/migrations/0006_event_consumptions.sql` | 消费记录表（状态机与 Go 类型一一对应）、租约/终态索引、outbox 稳定排序索引 |
| `backend/migrations/down/0006_event_consumptions.down.sql` | down 脚本（放在子目录，嵌入集不含它，因此不会被当成同版本迁移重复应用） |
| `backend/internal/repository/postgres/consumption.go` | PostgreSQL `ConsumptionStore`：`Begin` 三态、`Release` 不计尝试、死信两阶段（`BeginDeadLetter` 声明 + `FinalizeDeadLetter` CAS + `AbortDeadLetter`） |
| `backend/internal/repository/postgres/consumption_contract_test.go` | 契约测试：同一批场景**同时**跑内存实现与 PG 实现，任何语义分歧即失败 |
| `backend/internal/repository/postgres/outbox.go` | PostgreSQL `OutboxSource`（先发布后标记、条件标记幂等）+ PostgreSQL advisory lock |
| `backend/internal/repository/postgres/outbox_integration_test.go` | 真实 PG 测试：到期/未到期筛选、稳定排序、limit、条件标记、发布与重试、单活锁 |
| `backend/cmd/outbox-publisher/main.go` | 独立发布进程：持锁发布、无锁 standby、失败下一轮重试、优雅退出 |
| `backend/cmd/worker/adapters.go` | 订单事件 Applier（通知语义）、命令结果 Applier、HTTP Dispatcher（超时/重试/永久失败分类）、命令结果落库 |
| `backend/cmd/worker/duplicate_delivery_test.go` | 重复投递：同一事件投递两次只应用一次 |
| `backend/cmd/mock-gateway/main.go` | 开发用 HTTP 模拟网关（非设备协议，启动即告警，每次都标记响应） |
| `backend/scripts/verify-closed-loop.sh` | 四进程闭环验证脚本 |
| `backend/cmd/worker/main.go` | 正式接线：PG 消费记录、订单域、必填网关地址；移除内存占位与"缺适配器即拒绝启动"占位 |

## 契约和数据

### 新增或修改 API

**默认不新增 HTTP 接口。** 若链路验证需要运维端点（例如一键触发发布循环，或读取消费记录状态），必须先登记到
`api/openapi.yaml`（集成人员）再由 A 线实现，并按第 4.1 节流程走。

### PostgreSQL 迁移

**按集成人员决定 4：使用新的顺序迁移文件，提供 up/down，不修改已审批的历史迁移**
（`0001_init.sql`～`0005_order_tariff_snapshot.sql` 保持原样）。预计集中在两处：

- **消费记录**：事件唯一键（`event_id`）、owner、尝试次数、状态（含 `DEAD_LETTERING`）、租约到期时间、
  终态原因、流与 consumer 坐标；唯一约束是"重复投递不重复应用"的最后一道保证，必须有测试。
- **Outbox**：未发布行的取用索引（状态 + 时间），以及发布者隔离所需的列或锁策略。

迁移必须满足既有验收口径：空库可执行、重复执行不破坏数据、关键唯一约束有测试。

### Redis Key/Stream

**不新增、不改名。** 本模块只使用已冻结的流与键
（`ncs:stream:order-event`、`ncs:stream:charge-event`、`ncs:stream:charger-command`、`ncs:stream:dead-letter`，
以及 `ncs:idempotency:*` 系列）。若实现过程中需要新键，先更新本审批单。

### 幂等和并发策略

- **权威在 PostgreSQL**：消费记录是重复投递的最终判据；Redis 幂等声明只是加速。
- **尝试次数由存储计数**：`Release` 不计、`Fail` 计；传输投递次数（XCLAIM 放大）只用于诊断。
- **死信两阶段**：`BeginDeadLetter`（原子声明 + 租约）→ 写 DLQ → `FinalizeDeadLetter`（CAS）；
  写入失败用 `AbortDeadLetter` 归还声明。旧代次必须静默。
- **发布顺序（修正第 1 版的错误写法）**：**先发布，后标记**。发布成功而标记失败时允许重复发布，重复由
  `event_id` 唯一性与消费者的消费记录消除；反过来"先标记后发布"会在崩溃时丢事件，因此被禁止（详见交付范围第 1 项）。
- **发布单活**：由 `cmd/outbox-publisher` 持有的 PostgreSQL advisory lock 保证同一时刻只有一个发布者，
  因为冻结的 `OutboxSource` 接口（`ListUnpublished`/`MarkPublished`）本身不是 claim 协议，无法阻止两个发布者
  取到同一行。

## 验证记录（第 3 版）

### 交付前的全量检查

```text
环境：NCS_TEST_PG_DSN=postgres://ncs_test@127.0.0.1:55439/ncs_a03?sslmode=disable
      NCS_REDIS_TEST_ADDR=127.0.0.1:6379 NCS_REDIS_TEST_DB=14

命令：gofmt -l ./cmd ./internal
结果：无输出

命令：go build ./...
结果：通过

命令：go vet ./...
结果：通过，无告警

命令：go test -count=1 -race ./...
结果：全部 ok —— admin 1.509s、auth 3.371s、cmd/outbox-publisher 1.527s、cmd/worker 1.728s、
      config 1.010s、event 1.013s、httpapi 1.011s、observability 1.086s、order 1.518s、
      repository/postgres 3.830s、repository/redis 5.977s、station 1.489s、worker 2.101s
```

### 真实 PostgreSQL + Redis 全链路（第 6 项）

`backend/scripts/verify-closed-loop.sh` 启动**四个真实进程**（`cmd/api`、`cmd/outbox-publisher`、
`cmd/worker`、`cmd/mock-gateway`），通过 HTTP API 驱动业务流，并在数据库、Redis 与日志三处断言：

```text
命令：NCS_POSTGRES_DSN=... NCS_REDIS_ADDR=127.0.0.1:6379 NCS_REDIS_DB=14 \
      bash backend/scripts/verify-closed-loop.sh
结果：PASS（关键输出）

  order charger: 28, command charger: 29
  order created: ORD20260915020903e179f5f8
  device command issued: CMD2026091502090320679611
  command CMD2026091502090320679611 completed and the charger was released
  stream order-event: 0 pending for order-event-workers
  stream charge-event: 0 pending for charge-event-workers
  stream charger-command: 0 pending for charger-command-workers
  second publisher stood by; the advisory lock kept publishing single-active
  entry delivered to a consumer that died: pending=1
  entry reclaimed and acknowledged after the crash: records=1, pending=0
  charger 1: event result=FAILED, status=FAULT (not IDLE), completion consumed
  publisher batches: 8
  worker consumed:   9
  PASS: verified with real processes against real PostgreSQL and Redis

同时断言的否定项：worker 日志中不含 "in-memory consumption store"，也不含 "domain adapters are not wired"
（占位实现确实被移除了，而不是换了个名字）；死信流长度必须为 0；本次运行的消费记录不得出现
FAILED / DEAD_LETTERING / DEAD_LETTERED；三个消费者组在结束时 pending 必须为 0。

**脚本运行的调用方式（第 4 版起需要显式开关）**：

```text
NCS_E2E_ALLOW_DESTRUCTIVE=true NCS_E2E_ALLOWED_DATABASE=ncs_a03 \
NCS_POSTGRES_DSN=... NCS_REDIS_ADDR=127.0.0.1:6379 NCS_REDIS_DB=14 \
    backend/scripts/verify-closed-loop.sh
```

`ncs_a03` 不含 test/dev/ci/scratch 字样，因此必须用 `NCS_E2E_ALLOWED_DATABASE` 显式指名（这是刻意的：
库名不像测试库时要求调用者确认一次，比放行更安全）。
```

### 迁移与种子（对应决定 4：新顺序迁移 + up/down + 不改历史迁移）

```text
命令：createdb ncs_i01_empty_check，并以该空库启动 cmd/api（真实迁移 runner）
结果：schema_migrations = 1 2 3 4 5 6；event_consumptions 已建立；/healthz 与 /readyz 均正常
      —— 0006 在空库上可执行

命令：以同一库再次启动 cmd/api（迁移已应用）
结果：正常启动，日志中 error 计数为 0 —— 重复执行不破坏数据

命令：psql -f seeds/dev_seed.sql 连续执行两次
结果：两次均无错误，stations 计数仍为 1（种子按自然键幂等）

命令：DROP DATABASE ncs_i01_empty_check
结果：已删除，实例上仅保留 ncs_a03 —— 验证过程未留下残留库
```

down 脚本位于 `backend/migrations/down/`，故意放在嵌入目录之外：runner 嵌入的是顶层 `*.sql` 且拒绝同版本重复，
放在顶层会被当作同一版本的第二个迁移应用。执行方式已写在脚本头部注释里（先停掉所有写消费记录的进程）。

### 第 4 版：第 3 版审查意见的修复

| # | 意见 | 修复 | 反向验证 |
|---|---|---|---|
| P1 | `RecordChargerCommandResult` 不读 `result`，FAILED 也把充电桩改成 IDLE | 新增 `chargerCommandStatus`：`COMPLETED → IDLE`，其余（FAILED / TIMED_OUT / 未知）→ `FAULT`；**消费侧** `CompleteChargerCommand` 同样带 result，避免把 FAULT 撤销回 IDLE；完成事件携带 result，两侧判断一致 | 回退为"始终写 IDLE"后，`TestChargerCommandResultDecidesTheChargerStatus`、`...IsIdempotent`、`TestCompletingACommandAppliesTheOutcome` 全部失败，输出正是 `charger status = IDLE, want FAULT` |
| P1 | 发布器把"曾经取得锁"当作"仍持锁" | 锁对象新增 `Alive(ctx)`（在**同一专用连接**上 `Ping` + `SELECT 1`；会话级 advisory lock 的寿命等于会话寿命，因此"连接还活着"等价于"锁还在我们手里"），每轮发布前检查；失去锁立即停止发布、归还锁并重新竞争 | 新增 `TestLoopStopsPublishingWhenTheLockIsLost`（锁在运行中被抽走后必须停止发布并能重新取得）与真实 PG 的 `TestIntegrationOutboxPublisherLockReportsLiveness` |
| P1 | 闭环只验证到 `STARTING`，不足以称"完整 P0 闭环" | 脚本改为：驱动 `CREATED → STARTING → CANCELLED`（HTTP 能驱动的最远处），并在结论中**明确写出** `STARTING → CHARGING → STOPPING → COMPLETED` 属 BE-I-02、本脚本不为其背书 | 脚本输出末尾的 SCOPE 声明 |
| P2 | 破坏性脚本没有防误连门禁 | 必须 `NCS_E2E_ALLOW_DESTRUCTIVE=true`；拒绝 Redis DB 0；数据库名必须形如测试库（含 test/dev/ci/scratch/sandbox），否则需 `NCS_E2E_ALLOWED_DATABASE=<name>` 显式指名 | 三种拒绝路径均已实测（无开关退出码 2；库名不符给出指名方法；`NCS_REDIS_DB=0` 被拒） |
| P2 | 模拟网关的幂等记录会被覆盖、attempts 恒为 1 | 按 `command_id` 保存请求指纹（charger+action）、固定响应与真实尝试次数：同 ID 同请求返回原响应并递增 attempts；同 ID 不同请求返回 409 | 新增 `cmd/mock-gateway/main_test.go`：首答、重放计数、四种冲突、结果稳定性、失败充电桩、坏请求 |

**新增的端到端证据（你复现的那条路径已进入脚本）**：脚本第三个充电桩由模拟网关配置为失败，
断言 `完成事件 result=FAILED` 且 `charger.status = FAULT`（不是 IDLE）且该完成事件被消费。

### 第 5 版：第 4 版审查剩余问题的修复（模拟网关并发幂等穿透）

**问题**：`replay()` 的检查与首次写入 `commands` 不是同一个原子操作。两个相同 `command_id` 的请求同时到达时，
都看到"尚无记录"，于是都等待模拟延迟、都执行命令、最后互相覆盖记录；审查实测得到 `handled: 2 / replayed: 0`
且两个响应 `attempts` 都是 1。这正是超时重试的真实并发形态。

**修复**：把"检查 + 占位"合并为同一临界区（`claim`）：

- 首个请求在**锁内**建立 `IN_PROGRESS` 占位并记录请求指纹（charger + action），然后才去等待设备延迟；
- 相同指纹的并发请求**等待首个结果**（`done` 通道）或复用已就绪的结果，两者都会递增 attempts；
- 不同指纹（同 ID 不同 charger/action）→ **409**；
- 请求在设备答复前被取消 → **清除占位、不写任何设备结果**：重试必须能够真正执行命令，任何情况下都不能声称
  设备接受了它没收到的命令；
- 新增 `executions` 计数（只统计真正执行设备工作的次数），测试据此断言"并发重复只执行一次"。

**反向验证**：去掉占位（恢复修复前形状）后，`TestGatewayExecutesAConcurrentDuplicateOnce` 立即失败：

```text
main_test.go:123: device work executed 6 times for one command id, want 1
```

**新增测试**（`cmd/mock-gateway/main_test.go`）：`TestGatewayExecutesAConcurrentDuplicateOnce`（6 个并发重复请求 →
设备工作仅执行 1 次、attempts 互不相同）、`TestGatewayRejectsAConcurrentConflict`（并发不同请求 → 恰好一个 200
与一个 409）、`TestGatewayAbandonsAnOutcomeWhenTheRequesterDisappears`（取消不留伪造结果、重试可执行）。

### 第 6 版：模拟网关状态机的两条提前终止分支

第 5 版引入占位后，状态机有两条提前终止路径没有把占位还回去，审查用黑盒复现了两条：

| 分支 | 现象 | 修复 |
|---|---|---|
| 非法动作 | 新 `command_id` 携带非 `RESTART` 动作：首次 400，**占位残留**，第二次相同请求永久等待 `done` | `execute` 路径上的提前返回必须先 `abandon`（非法动作分支已加），并新增"连续两次非法请求"测试断言两次都立即 400、无残留记录、该 id 之后仍可用 |
| 首请求取消后的等待者 | 首请求取消 → 删除占位并唤醒等待者 → 等待者醒来找不到记录，**伪造 `FAILED` 回执**（HTTP 200） | 处理函数改为 **claim 循环**：等待者被唤醒后**重新 claim**——记录已被删除时它自己成为执行者并取得真实结果；已就绪时走重放路径。**不再存在伪造设备结果的代码路径** |

反向验证（两条都实测）：

```text
去掉非法动作分支的 abandon → TestGatewayRejectsAnUnsupportedActionTwice...
    post command cmd_bad_action: context deadline exceeded   （即"第二次请求超时无响应"）
恢复"等待者伪造 FAILED" → TestGatewayLetsAWaitersReExecute...
    ...; a device that was never reached must not be reported as failed
```

新增测试：`TestGatewayRejectsAnUnsupportedActionTwiceWithoutLeavingAPlaceholder`、
`TestGatewayLetsAWaitersReExecuteWhenTheFirstRequestIsCancelled`（并新增带超时的请求辅助函数，
使"挂起"表现为测试失败而不是阻塞）。

### 三项指定验证

| 要求 | 实现方式 | 结果 |
|---|---|---|
| 重复投递测试 | `cmd/worker/duplicate_delivery_test.go`：同一 `event_id` 投递两次，真实 PG 消费记录 + 真实 Redis Stream，断言领域应用只执行一次、两条投递都被 ACK | PASS（0.08s） |
| 并发发布测试 | ① `internal/repository/postgres/outbox_integration_test.go` 的 `TestIntegrationOutboxPublisherLockIsSingleActive`（真实 advisory lock：第二个发布者取不到、释放后可取、重复释放安全）；② 验证脚本同时启动两个发布进程，断言第二个进入 standby 且从未取得锁 | PASS |
| `kill -9` 恢复测试 | 验证脚本：SIGKILL 掉 worker → 用 `XREADGROUP` 让一个"已死消费者"持有一条 pending 条目 → 重启 worker → 断言该条目被恢复（日志 `recovered pending entries`）、被应用一次（消费记录 SUCCEEDED）、pending 回到 0 | PASS |

### 新增/修改的 A 线接口（跨线最小改动）

| 位置 | 变更 | 理由 |
|---|---|---|
| `internal/repository/postgres/orders.go` | `RecordChargerCommandResult(commandNo, chargerID, action, result, traceID)`：在一个事务里释放充电桩并追加 `CHARGER_COMMAND_COMPLETED`；`CompleteChargerCommand` 改为复用同一段 SQL helper | 命令结果事件必须与它描述的状态变更同时落库。**没有它，"设备结果 → 事件 → 消费 → 状态回写"这条链无法闭合**：事件要么与状态不一致，要么无人产生 |
| `internal/order/types.go`、`service.go` | `Store`/`Service` 增加同名方法；新增 `EventChargerCommandCompleted` 事件常量 | 供 B 线 dispatcher 调用，保持 A 线状态机是唯一写入方 |

### 两个契约缺口 → 已由集成人员决定新建 BE-I-02（本模块不擅自扩张范围）

1. **充电确认（CHARGE_STARTED / CHARGE_STOPPED）没有设备侧的入口。** 这两个事件由 A 线
   `ConfirmStart`/`ConfirmStop` 在**同一事务里**写状态并落 outbox，因此 Worker 只能把它们**当作通知消费**（本
   模块已如此实现，并且正是闭环验证发现了"重复应用"这个真实缺陷）。但"谁调用 ConfirmStart"目前没有答案：
   没有登记到 OpenAPI 的网关回调端点。命令路径上我用 dispatcher 的同步结果闭合了同类缺口，充电确认路径上是
   设备异步发起的，无法用同样方式绕开。**建议**：由集成人员在 OpenAPI 登记一个网关回执端点（例如
   `POST /internal/charger-events`），A 线实现为调用 `ConfirmStart`/`ConfirmStop`，此后该链路与命令链路同构。
2. **冻结的设备命令动作集只有 `RESTART`。** 充电开始/停止没有对应的设备命令动作（`worker.SupportedCommandActions()`），
   因此"平台命令充电桩开始充电"这一步在契约里尚不存在。本模块不擅自新增动作（属于冻结契约的变更）。

集成人员的结论：新建 **BE-I-02 充电设备控制与回执闭环**，在 H5 业务开发前完成；本模块（I-01）按
"**事件基础设施与 RESTART 命令闭环**"批准口径送审。BE-I-02 的范围与冻结要求已登记在
`docs/migration/backend-parallel-development.md` 与 `docs/migration/be-i-02-charger-control-receipts-approval.md`。

## 风险和回滚（第 3 版补充）

- 已知风险：
  1. **跨线同时改动**：本项目最容易失控的形态。缓解方式是"接口先冻结、两端各自实现、集成分支合并"，
     并且由集成人员指定分支形态；任何一方不得为通过自己的测试而修改对方目录。
  2. **内存→PostgreSQL 切换会让"重复应用"从不可见变为可见**：此前用内存记录掩盖了重复投递的实际影响，
     切换后必须用幂等测试证明，而不是相信设计。
  3. **首版设备网关是模拟的**：必须在部署文档与日志里明确写出，并在 `CHARGER_COMMAND_COMPLETED` 链路上标注
     它是模拟出口，避免被误认为已支持真实设备协议。
  4. **迁移与唯一约束**：消费记录的唯一键若缺失，"重启不重复应用"就无法成立，属于 A 线实现的关键验收点。
  5. **审批日志继续滞后**：第 7 项不是形式工作；结论不落盘，后续模块无法追溯判定依据。
  6. **范围蔓延**：真实设备协议、支付结算、H5、指标导出都有自然"顺手做"的诱惑，本模块不做。
- 新增风险（第 3 版）：
  1. **消费记录表成为强依赖**：没有 `event_consumptions` 表，Worker 拒绝启动（这是刻意的）。因此迁移 0006
     必须先于新版本进程上线；回滚顺序相反（先停进程，再决定是否执行 down 脚本）。
  2. **命令结果事件只在状态真正变化时产生**：`RecordChargerCommandResult` 在充电桩未被 RESTARTING/OCCUPIED
     持有时不写事件（避免为无事发生的回执制造噪声），因此"命令完成但无事件"是正常结果之一，不是丢事件。
  3. **dispatcher 与域写入不是原子的**：设备已答复但结果落库失败时，dispatch 会被重试；重试是安全的
     （网关按 command_id 幂等、`RecordChargerCommandResult` 有守卫），代价是一次多余的设备调用。
- 回滚方式（**修正第 1 版：禁止回退到内存消费记录**）：内存 `ConsumptionStore` 没有跨重启的幂等保证，回退到它
  会让同一事件在重启后再次修改订单或账务，因此**不作为生产回滚方案**。正确做法是：
  1. 停止新版本的 Worker 与 Publisher（先停生产者侧，避免边回滚边消费）；
  2. 回退到上一部署版本（`git revert` 本模块提交 + 重新部署上一镜像/二进制）；
  3. 数据库迁移采用**兼容保留**策略：新表与新索引是附加的，旧版本不会读取它们，因此默认不回滚；确需回滚时使用
     随迁移一起提交的 down 脚本，并且只在新版本已完全停止后执行；
  4. 回滚期间积压的 Redis Stream 条目保持 pending，恢复后由既有 Pending 恢复逻辑处理（这正是 B-04/B-05 的保证）。

  本模块不改变 Redis/Stream 契约与事件格式，因此回滚不涉及数据迁移或消息格式转换。
- 是否影响旧 C++ 系统：否。

## 集成人员决定（已冻结，实施时不得自行更改）

第 1 版提交的五项未决问题已由集成人员决定，本节即为约束：

1. **分支**：单一分支 `codex/migration/i-01-closed-loop-integration`。顺序是：先把 A 线最终提交 `3105659`、
   B 线最终链 `be0472d` 合入集成分支，再把本模块 rebase 到新的集成基线。
2. **发布进程独立**：Outbox 使用独立进程 `cmd/outbox-publisher`，通过 PostgreSQL **advisory lock** 保证单活；
   Worker 可独立水平扩容。
3. **模拟网关独立**：采用独立 HTTP 模拟器，并实现真正的 HTTP Dispatcher；**生产配置不得默认启用模拟器**，
   WSL 环境可显式启用。
4. **迁移**：消费记录与 Outbox 索引使用**新的顺序迁移文件**，提供 up/down，不修改已审批的历史迁移。
5. **审批日志**：由审批方在合并门禁更新；开发者只提交验证材料，不自行填写 `APPROVED`，也不代为登记其他模块结论。

未决问题：**无**（第 2 版起）。实施中若发现新的契约问题，按第 1 版的方式提出并等待决定，不擅自变更。

## 本模块的口径（第 4 版，按审查结论收窄）

本模块**可以**被批准为："事件基础设施（PG Outbox + 发布器 + PG 消费记录 + 可观测性接线）与
RESTART 命令闭环"。

本模块**不能**被表述为"后端 P0 完整闭环"：订单状态链路止于 `STARTING → CANCELLED`（HTTP 可驱动范围），
`CHARGING / STOPPING / COMPLETED` 需要设备回执端点与完整的命令动作集，属 BE-I-02。

## 审批结论

```text
状态：APPROVED
审批人：Codex
审批时间：2026-09-15（集成 PR 归档既有终审结论）
批准提交：9cf1b9a
批准范围：PostgreSQL Outbox + 独立 Publisher + PostgreSQL 消费记录 + Redis Streams Worker +
  RESTART 模拟网关命令闭环；不宣称真实网关协议或完整 P0 订单状态链路。
补齐完整 P0 订单闭环的设备回执与 START/STOP 动作由 BE-I-02 单独批准。
```
