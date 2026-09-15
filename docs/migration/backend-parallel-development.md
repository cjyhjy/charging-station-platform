# 后端双线并行开发技术文档

## 1. 文档定位

本文件是 G0 接口和目录定义之后的后端执行规范。工作模式调整为三条线：

    后端线 A：Go API、领域业务、PostgreSQL
    后端线 B：Redis、Redis Streams、Outbox、Worker
    H5 线 C：独立保留，后端 P0 完成前暂停业务开发

当前所有工作目录均位于 WSL，集成目录为：

    /home/penty/projects/charging-station-platform

现有 C++/Qt/Crow/SQLite 代码继续保留为可运行基线。后端迁移采用渐进替换，不在第一阶段删除旧服务。

## 2. 三条工作线

### 2.1 后端线 A：API 与业务核心

目标是先把 PostgreSQL 作为业务事实源，完成 Go API 和核心领域流程。

目录所有权：

    backend/cmd/api/
    backend/internal/config/
    backend/internal/http/
    backend/internal/auth/
    backend/internal/station/
    backend/internal/charger/
    backend/internal/order/
    backend/internal/wallet/
    backend/internal/admin/
    backend/internal/repository/postgres/
    backend/migrations/

负责内容：

- Go API 进程和配置；
- 统一 HTTP 响应、鉴权和请求 ID；
- PostgreSQL 表结构、索引和迁移；
- 用户、管理员、站点、充电桩、订单、钱包；
- 订单状态机和业务幂等；
- 管理端 P0 API；
- 业务单元测试和 PostgreSQL 集成测试。

### 2.2 后端线 B：事件与异步基础设施

目标是建立可靠的异步链路，不把 Redis 当作业务主库。

目录所有权：

    backend/cmd/worker/
    backend/internal/event/
    backend/internal/repository/redis/
    backend/internal/worker/
    backend/internal/observability/
    deploy/redis/

负责内容：

- Redis 连接、缓存、会话、限流和锁；
- Outbox 发布器；
- Redis Streams 发布和消费；
- 消费者组、ACK、Pending 恢复和死信；
- 充电事件、设备命令、订单事件；
- Worker 优雅退出、重试和幂等；
- Redis 故障、重复消息和进程重启测试。

### 2.3 H5 线 C：独立暂停

H5 保留独立 worktree 和分支，但后端 P0 闭环前不进入正式业务开发。

H5 线后续负责：

- Vue 3 + TypeScript + Vite；
- 用户端和管理端页面；
- API 请求封装和 Mock；
- Nginx 静态资源和反向代理；
- 浏览器端和端到端测试。

H5 只能依赖 api/openapi.yaml，不得访问 PostgreSQL、Redis 或 Redis Streams。

## 3. 绝对隔离规则

两条后端线可以同时工作，但不得修改同一类文件。

| 区域 | 后端线 A | 后端线 B | 维护人 |
|---|---:|---:|---|
| backend/cmd/api/ | 可写 | 只读 | A |
| backend/cmd/worker/ | 只读 | 可写 | B |
| backend/internal/auth/ | 可写 | 只读 | A |
| backend/internal/order/ | 可写 | 只读 | A |
| backend/internal/repository/postgres/ | 可写 | 只读 | A |
| backend/migrations/ | 可写 | 只读 | A |
| backend/internal/event/ | 只读 | 可写 | B |
| backend/internal/repository/redis/ | 只读 | 可写 | B |
| backend/internal/worker/ | 只读 | 可写 | B |
| api/openapi.yaml | 提交建议 | 提交建议 | 集成人员 |
| backend/go.mod、backend/go.sum | 提交依赖申请 | 提交依赖申请 | 集成人员 |
| 根目录部署入口 | 提交建议 | 提交建议 | 集成人员 |

backend/go.mod 和 backend/go.sum 是共享锁文件。任何一条线需要新增依赖，先在 PR 中说明理由，由我统一修改，避免依赖文件冲突。

## 4. 后端交互契约

### 4.1 API 契约

唯一来源：

    api/openapi.yaml

A 线实现 API，B 线不直接修改 API 路由。B 线对外提供的健康检查、事件管理和运维接口必须先登记到 OpenAPI，再实现。

### 4.2 业务事件契约

所有事件使用统一结构：

    {
      "event_id": "evt_01",
      "event_type": "CHARGE_STARTED",
      "aggregate_type": "order",
      "aggregate_id": "order_01",
      "occurred_at": "2026-09-14T12:00:00Z",
      "trace_id": "trace_01",
      "payload": {}
    }

事件类型第一阶段固定为：

    ORDER_CREATED
    CHARGE_START_REQUESTED
    CHARGE_STARTED
    CHARGE_STOP_REQUESTED
    CHARGE_STOPPED
    ORDER_COMPLETED
    CHARGER_COMMAND_REQUESTED
    CHARGER_COMMAND_COMPLETED

事件必须具备唯一 event_id。消费者重复收到事件时，必须通过消费记录或业务唯一键保证结果不重复。

### 4.3 PostgreSQL 与 Redis 边界

PostgreSQL 保存：

- 用户和管理员；
- 站点和充电桩；
- 订单和账务；
- 操作日志；
- 幂等记录；
- Outbox 事件；
- 消费结果和必要的审计记录。

Redis 保存：

- 会话；
- 短期缓存；
- 限流计数；
- 分布式锁；
- Redis Streams；
- 消费者组状态。

Redis 丢失后，系统必须能依靠 PostgreSQL 恢复核心业务，不允许只在 Redis 中保存订单最终状态。

## 5. 后端模块顺序

### A 线模块

#### BE-A-01：Go API 脚手架

内容：

- go.mod 初始依赖申请；
- cmd/api；
- 配置加载；
- GET /healthz；
- GET /readyz；
- 优雅退出；
- 请求 ID；
- 统一错误响应。

完成标准：

    go test ./...
    go vet ./...
    go test -race ./...

#### BE-A-02：PostgreSQL 基础迁移

内容：

- 迁移版本表；
- 用户、管理员、站点、充电桩基础表；
- 订单和操作日志表；
- 唯一约束和必要索引；
- 空库初始化和开发种子数据。

完成标准：

- 空库可执行迁移；
- 重复执行不破坏数据；
- 关键唯一约束有测试；
- 所有金额使用整数分；
- 所有时间字段明确使用 UTC。

#### BE-A-03：鉴权与站点域

内容：

- 用户/管理员登录；
- 会话校验；
- 权限校验；
- 站点和充电桩查询；
- Redis 缓存接口只通过 B 线提供的抽象访问。

#### BE-A-04：订单核心域

内容：

- 创建订单；
- 订单状态机；
- 开始/停止充电请求；
- 幂等键；
- 账务金额计算；
- Outbox 记录写入。

订单状态推进必须在 PostgreSQL 事务中完成，不能由前端直接指定目标状态。

#### BE-A-05：管理端 P0 API

内容：

- 站点管理；
- 充电桩管理；
- 用户查询；
- 订单查询；
- 设备命令请求；
- 操作审计。

### B 线模块

#### BE-B-01：Redis 基础适配

内容：

- Redis 连接池；
- 健康检查；
- 缓存读写；
- 会话存储；
- 限流和锁接口；
- 连接失败降级策略。

#### BE-B-02：Redis Streams 与 Worker 脚手架

内容：

- cmd/worker；
- Stream 创建和消费者组；
- 消费循环；
- ACK；
- 优雅退出；
- Pending 查询。

第一阶段 Streams：

    ncs:stream:order-event
    ncs:stream:charge-event
    ncs:stream:charger-command
    ncs:stream:dead-letter

#### BE-B-03：Outbox Publisher

内容：

- 读取 PostgreSQL Outbox；
- 发布到 Redis Stream；
- 发布成功后标记状态；
- 失败重试；
- 发布幂等；
- 批量大小和锁定策略。

该模块依赖 A-02 的 Outbox 表结构，但代码只通过约定接口读取，不能直接修改 A 线的领域逻辑。

#### BE-B-04：充电事件和设备命令 Worker

内容：

- 开始充电事件；
- 停止充电事件；
- 设备重启命令；
- 重试和死信；
- 消费结果落库；
- 重复事件保护。

#### BE-B-05：故障恢复与可观测性

内容：

- Stream lag；
- Pending 数量；
- 重试次数；
- 死信数量；
- Worker 重启恢复；
- 关键业务 trace ID。

### 跨线模块

跨线模块同时触及 A 线和 B 线的实现，因此**不属于任何单一开发线**：它由集成人员指定分支与合并顺序，两端的接口
必须先冻结再各自实现。它不作为任一条线的"顺手改动"开工。

#### BE-I-01：后端闭环集成（事件基础设施与 RESTART 命令闭环）

这是 P0 闭环的最后一个模块。它的目标是让已经批准的各模块在**生产配置下真正连起来**，而不是新增功能：在此之前，
`cmd/worker` 拒绝在没有领域适配器时启动，消费记录用的是内存实现，Outbox 只有抽象没有 PostgreSQL 落地，
设备命令没有可验收的出口。

交付范围固定为七项：

1. **PostgreSQL `OutboxSource` 与发布循环**：实现 `event.OutboxSource`（`ListUnpublished`/`MarkPublished`），
   并把发布循环接入运行进程（`cmd/worker` 或独立的发布进程，由集成人员决定），包含批量大小、锁定与失败保留。
2. **PostgreSQL `ConsumptionStore`**：实现第 4.3 节要求的权威消费记录，替换内存实现，使"进程重启后不重复应用"
   成为真实保证。接口以 `internal/event/consumption.go` 的当前契约为准（`Begin`/`Complete`/`Fail`/`Release`/
   `BeginDeadLetter`/`FinalizeDeadLetter`/`AbortDeadLetter`），其中 `Release` 与 `AbortDeadLetter` 的尝试计数语义
   是审批单里明确的验收点。
3. **订单事件与命令结果 Applier**：实现 `worker.ChargeEventApplier` 与 `worker.CommandResultApplier`，在 PostgreSQL
   事务中推进订单状态机并写入账务/审计，不由前端或事件直接指定目标状态。
4. **首版可验收的设备 Dispatcher / 模拟网关**：实现 `worker.CommandDispatcher`。首版可以是模拟网关，但它必须是
   **可验收的**：有明确的请求/响应契约、失败与超时语义、以及使 `CHARGER_COMMAND_COMPLETED` 能够被真实产生的路径。
   它不宣称支持真实设备协议（见"非目标"）。
5. **正式接线 `cmd/worker`**：注册上述实现，移除内存消费记录占位与"缺少领域适配器即拒绝启动"的占位分支。
6. **闭环验证**：API 下单 → PG Outbox → Redis Stream → Worker → PG 状态回写 → ACK/Pending/死信，端到端可复现。
7. **同步审批日志**：把 A-03～A-05、B-04～B-05 以及本模块的审批结论登记到 `approval-log.md`。

所有权（防止越界的硬规则）：

| 内容 | 归属 |
|---|---|
| `internal/repository/postgres/`（OutboxSource、ConsumptionStore、领域仓储） | A 线 |
| `backend/migrations/`（消费记录表、唯一约束与索引） | A 线 |
| `internal/httpapi/`（下单与命令请求入口） | A 线 |
| `cmd/worker/`、`internal/worker/`（接线与适配） | B 线 |
| `internal/event/`、`internal/repository/redis/`（契约与 Redis 实现） | B 线，如需扩展先改审批单 |
| `go.mod`/`go.sum`、`api/openapi.yaml`、`approval-log.md` | 集成人员 |
| 设备网关协议实现（真实 Modbus/OCPP 等） | 充电桩网关方，**不在本模块** |

验收要点（可执行，不是描述）：

- 完整链路可复现，且订单最终状态由 PostgreSQL 回写决定；
- 同一事件重复投递**不重复应用**（消费记录生效），进程重启后同样成立；
- Worker 被 `kill -9` 后重启，先前 pending 的条目被恢复处理，且不重复应用；
- Redis 不可用时的降级行为与本项目已冻结的策略一致（会话/锁 FailClosed，缓存/限流/幂等 FailOpen）；
- 死信链路可观测：死信条目、消费记录终态、`ncs_stream_dead_letter_length` 三者一致；
- `cmd/worker` 启动日志不再出现内存消费记录占位；
- 差异检查确认没有跨线越界修改。

明确不属于本模块（避免范围蔓延）：真实设备协议实现、H5 线、支付与结算、多区域部署、指标导出到
Prometheus/OTLP、OpenAPI 新端点（需先登记再实现，由集成人员处理）。

开工条件：A-02/A-03/A-04（A-05 可与本模块并行）与 B-01/B-03/B-04/B-05 已合入集成分支；集成人员确认开发分支形态（单分支或
A/B 两分支 + 合并顺序）与依赖改动；两端接口以本文件第 4 节与各审批单为准，冻结后不得单方面变更。

#### BE-I-02：充电设备控制与回执闭环

BE-I-01 交付了事件基础设施与 RESTART 命令闭环，但发现两个契约缺口使"完整 P0 订单闭环"无法成立；
集成人员决定单独立项，并在 H5 业务开发前完成。

交付范围固定为两项：

1. **设备回执端点**：`POST /api/v1/internal/charger-events`（内部接口，需服务鉴权）。载荷至少包含
   `eventId`、`eventType`（`CHARGE_STARTED` | `CHARGE_STOPPED`）、`orderNo`、`chargerId`、`occurredAt`、
   `energyWh`、`meterStartWh`、`meterEndWh`、`traceId`。要求：
   - `eventId` 幂等（重复回执不重复应用）；
   - 校验订单与充电桩的归属关系；
   - 处理器调用既有 `order.Service.ConfirmStart` / `ConfirmStop`，由其在**同一事务**内推进订单状态并写
     Outbox（`CHARGE_STARTED` / `CHARGE_STOPPED` 事件已由该路径产生，B 线 Worker 只按通知消费）。
   端点的 OpenAPI 登记由集成人员完成（先登记再实现，第 4.1 节）。
2. **命令动作集扩展与命令产生路径**：`RESTART`、`START_CHARGING`、`STOP_CHARGING`。**不得用 `RESTART`
   代替开始/停止。** `StartCharging`/`StopCharging` 在**原事务内**追加 `CHARGER_COMMAND_REQUESTED`
   （分别携带 `START_CHARGING` / `STOP_CHARGING` 与 `order_no`），同时保留既有生命周期通知事件——只扩展动作
   枚举不会产生任何命令。命令载荷增加 `order_no`（START/STOP 必填，RESTART 不需要）。动作、载荷、回执与
   失败语义必须同时更新事件契约、Dispatcher、模拟网关与契约测试。

   状态推进与失败语义（已冻结）：命令成功只表示设备接受命令、**不推进订单状态**；`CHARGE_STARTED` 回执负责
   `STARTING → CHARGING`，`CHARGE_STOPPED` 回执负责 `STOPPING → COMPLETED`；START 明确失败 → 订单 `FAILED`
   + 充电桩 `FAULT`；STOP 明确失败 → 订单保持 `STOPPING`（不能假定已停止）+ 充电桩 `FAULT` 并等待恢复；
   超时与网络错误继续重试、**不得伪造设备失败回执**；乱序回执返回 409 并记录审计，不暂存。回执幂等使用
   PostgreSQL `idempotency_records`（key=`eventId`、`request_hash`=载荷摘要），与状态推进、Outbox 写入同事务；
   服务鉴权为 HTTPS + Bearer 服务令牌（`NCS_CHARGER_GATEWAY_TOKEN`，无默认值）。`occurredAt` 是业务事实时间
   （UTC）：`CHARGE_STARTED` 写 `started_at`，`CHARGE_STOPPED` 写 `stopped_at`/`completed_at` 并参与分时计费，
   停止早于开始返回 409，未来偏差超过 5 分钟返回 400。STOP 明确失败后的恢复由后台任务以**新的 command_id**
   重发 `STOP_CHARGING`（有次数上限与退避），达到上限后保持 `STOPPING`/`FAULT` 并告警，**绝不自动释放或结算**。

验收要点：`STARTING → CHARGING → STOPPING → COMPLETED` 可由真实回执驱动并在 PostgreSQL 中回写；
回执重复投递不重复应用；越权/不匹配的订单-充电桩关系被拒绝；三种命令动作各自有成功与失败路径的测试；
闭环验证脚本覆盖完整订单状态链路（届时可去掉"不验证 CHARGING/COMPLETED"的免责声明）。

所有权：`internal/order`（`ConfirmStart`/`ConfirmStop` 事务与状态机）、`internal/repository/postgres`、
`internal/httpapi`、`api/openapi.yaml` 登记属 A 线；`internal/worker`（动作集与回执消费）、`cmd/worker`、
`cmd/mock-gateway` 属 B 线；`api/openapi.yaml` 的登记动作由集成人员执行。

明确不属于本模块：真实设备协议实现（Modbus/OCPP）、结算与支付、H5 线、多区域部署。

## 6. 并行依赖图

    G0 契约冻结
       ├─ BE-A-01 Go API 脚手架
       │    └─ BE-A-02 PostgreSQL
       │         └─ BE-A-03 鉴权/站点
       │              └─ BE-A-04 订单
       │                   └─ BE-A-05 管理端 P0 API
       └─ BE-B-01 Redis 适配
            └─ BE-B-02 Streams Worker
                 └─ BE-B-03 Outbox
                      └─ BE-B-04 事件闭环
                           └─ BE-B-05 恢复与可观测性
                                └─ BE-I-01 后端闭环集成（跨线，两线全部合入后开工）
                                     └─ BE-I-02 充电设备控制与回执闭环（H5 业务开发前完成）

允许的并行关系：

- BE-A-01 与 BE-B-01 可以同时开始；
- BE-A-02 与 BE-B-02 可以同时开始，但 B-02 使用固定事件样例；
- BE-A-03 与 BE-B-03 可以并行，B-03 依赖已冻结的 Outbox 字段；
- BE-A-04 与 BE-B-04 必须先完成事件契约测试，再进行真实联调；
- H5 线 C 暂不进入业务模块；
- BE-I-01 只能在 A 线与 B 线全部合入集成分支后开工，且必须先由集成人员确定"单分支还是两分支"的形态。

禁止的关系：

- B 线不能为了测试直接修改 A 线迁移 SQL；
- A 线不能为了发送事件直接绕过 B 线把 Redis 调用写入订单服务；
- 任意一条线不能复制另一条线的数据库或 Redis 实现；
- 两条线不能同时修改 go.mod、go.sum 和 OpenAPI。

## 7. 分支和工作区

集成区：

    /home/penty/projects/charging-station-platform
    codex/migration-integration

后端线 A：

    /home/penty/projects/charging-station-platform-backend-core
    codex/migration/backend-core/a-01-bootstrap

后端线 B：

    /home/penty/projects/charging-station-platform-backend-events
    codex/migration/backend-events/b-01-stream-bootstrap

H5 线 C：

    /home/penty/projects/charging-station-platform-h5
    codex/migration/h5/c-01-shell
    状态：PAUSED

每条线的模块分支都从最新的 codex/migration-integration 创建。合并前必须同步集成分支，避免后期一次性解决大量冲突。

## 8. 审批流程

每个后端模块都按以下流程执行：

    开发模块
      ↓
    提交模块 PR
      ↓
    我检查 diff、目录边界和依赖
      ↓
    我在 WSL 执行单元、集成、并发和故障测试
      ↓
    APPROVED 或 CHANGES_REQUIRED
      ↓
    只有 APPROVED 才解锁下一模块

A 线审批命令：

    cd /home/penty/projects/charging-station-platform-backend-core/backend
    go test ./...
    go vet ./...
    go test -race ./...

B 线审批命令：

    cd /home/penty/projects/charging-station-platform-backend-events/backend
    go test ./...
    go vet ./...
    go test -race ./...

集成审批命令：

    go test ./...
    go test -race ./...
    redis-cli ping
    pg_isready
    nginx -t

审批重点：

- 事务是否覆盖业务状态和 Outbox；
- 重复请求是否幂等；
- 重复事件是否幂等；
- Redis 故障是否导致主流程不可恢复；
- PostgreSQL 迁移是否可重复执行；
- Stream 消费者是否支持 Pending 恢复；
- 是否修改了另一条线的目录；
- 是否引入未审批的依赖；
- 是否保留旧服务回退能力。

## 9. H5 解锁条件

后端 P0 只有在以下闭环全部通过后，H5 线 C 才解除暂停：

    登录
    → 查询站点
    → 查询充电桩
    → 创建订单
    → 开始充电
    → Redis Stream 产生事件
    → Worker 消费并落库
    → 停止充电
    → 订单完成
    → 查询历史订单

解除 H5 暂停的审批条件：

- A 线 P0 API 契约测试通过；
- B 线事件链路测试通过；
- PostgreSQL、Redis 重启后业务状态可恢复；
- 订单重复提交测试通过；
- 事件重复消费测试通过；
- 集成分支没有未解决冲突；
- 我明确将 H5 状态改为 READY。
