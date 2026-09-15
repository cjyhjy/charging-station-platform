# 迁移模块审批单：BE-B-04 充电事件和设备命令 Worker

第 6 轮提交，响应第 5 轮审查 `CHANGES_REQUIRED` 的 2 项（1 P0 + 1 P1）。

第 1～5 轮内容保留在文末，便于对照。

## 第 6 轮：两项修复

**两项都指向同一个问题：我上一轮的测试是"手工拼装"的，因此没有真正跑过这两条执行链路。** 这是本轮最该被记录的
教训——新增的保护只有在真实路径上被执行才算存在，而我把 `attemptError` 和租约都手工构造了。

### [P0] 1. 普通处理失败没有携带 reservation

**确认属实，而且后果正是上一轮那个 P0 的回归。** `pipeline.go` 中"处理器失败"那条返回路径只填了
`attempt`，漏了 `reservation`：

```go
return &attemptError{attempt: reservation.Attempt, err: err}   // 少了 reservation: reservation
```

于是**每一次永久失败或重试耗尽**进入死信路径时拿到的都是空 reservation，`BeginDeadLetter` 的
owner 检查（`reservation.Owner != ""` 才成立）直接拒绝——看起来"静默"是对的，但拒绝的理由是错的：
不是"事件已属于更新代次"，而是"调用者没带任何身份"。而如果这条路径上的旧消费者带上任何能被误认为
有效的身份，就会绕过 CAS 写出死信并 ACK。**所有权检查必须建立在真实传递的 reservation 上，而不是默认它
存在。**

修复：该返回路径补上 `reservation: reservation`（与 `Complete` 失败那条路径一致）。

**回归测试改为走真实 pipeline**（本轮的重点）：`TestAPermanentFailureThroughThePipelineCarriesItsReservation`
直接调用 `ChargeHandler.HandleDelivery`，断言返回的错误通过 `reservationOfError` 能取到 reservation，且其
`Owner` 与存储记录里的 owner 一致。

`TestAStaleDeliveryThroughThePipelineStaysSilentAfterTheEventWasApplied` 用真实的
`takeoverApplier`（在处理器内部推进时钟使租约过期、再让第二个消费者的真实 pipeline 接管并成功）复现
"旧消费者超租约 + 新消费者成功 + 旧消费者迟到失败"，**不再手工构造 `attemptError`**。

### [P1] 2. `DEAD_LETTERING` 没有互斥期

**确认属实。** `Fail()` 会把 `LeaseUntil` 清零；`BeginDeadLetter()` 只设置 `DEAD_LETTERING` 而没有建立租约；
`Begin()` 又只在 `Outcome == ""` 时检查租约。三者叠加的结果是：死信声明期间**没有任何互斥**，另一消费者可以
立即接管事件并重新执行处理器，而第一个投递还在写 DLQ 与 finalize。

修复：

1. `BeginDeadLetter` 为声明建立租约（`LeaseUntil = now + leaseTTL`）——声明现在有了明确的互斥期；
2. `Begin` 把 `DEAD_LETTERING` 与普通预留一视同仁地视为"进行中"（`inProgressLocked`），租约内返回
   `ReservationInProgress` 且不增加尝试次数；租约过期后才允许接管（这正是"写入过程崩溃"的恢复路径）；
3. `Fail` 与 `Release` **不得清除声明的租约**（它们只处理预留；声明只能被 `FinalizeDeadLetter` 或
   `AbortDeadLetter` 收尾）；
4. **写入失败后的安全释放**：新增 `AbortDeadLetter(reservation, reason)`（CAS：owner 一致且仍是
   `DEAD_LETTERING` 才置回 `FAILED` 并清租约）。Worker 在 `XADD` 失败时同时释放 Redis 侧写入声明与存储侧
   权威声明，于是重试可以立即重新停放，而不必等声明租约过期；释放失败也不影响正确性（租约过期即恢复）。

回归测试：`TestTheDeadLetterClaimHoldsTheEventWhileTheParkIsWritten`（声明期间第二次 `Begin` 必须是
`in_progress` 且不增加尝试次数；`Fail`/`Release` 不得清除声明；租约过期后接管为 attempt+1）、
`TestAFailedDeadLetterWriteGivesTheClaimBack`、`TestAbortingADeadLetterClaimRequiresTheClaimHolder`、
`TestWorkerGivesTheClaimBackWhenTheDeadLetterWriteFails`（写入失败 → 记录不得停在 `DEAD_LETTERING` → 重试成功
停放并 ACK）。

**反向验证**（四处，逐一回退后对应测试必须失败）：

| 回退内容 | 观察到的失败 |
|---|---|
| 去掉 pipeline 返回路径的 `reservation` | `a failed attempt must carry its reservation...` / `applied work must not be parked, got 1 dead letters` |
| 去掉声明建立的租约 | `expected the claim to hold the event, got reserved` |
| `Begin` 不再识别 `DEAD_LETTERING` | 同上 |
| 去掉写入失败后的 `AbortDeadLetter` | `a failed write must not leave the event marked as being parked` |

### 第 6 轮验证记录

在 `charging-station-platform-backend-events/backend` 下执行（分支 `b-04-charge-command-workers`，
基点 `ad4c524`，Redis 版本 `7.0.15`）：

```text
命令：gofmt -l ./cmd ./internal
结果：无输出

命令：go vet ./... / go build ./...
结果：均通过

命令：NCS_REDIS_TEST_ADDR=127.0.0.1:6379 go test -count=1 -race ./...
结果：全部 ok（repository/redis 5.890s、worker 2.100s、event 1.019s、cmd/worker 1.013s）

命令：NCS_REDIS_TEST_ADDR=127.0.0.1:6379 go test -count=20 -race -timeout 900s \
      ./internal/repository/redis/ ./internal/worker/ ./internal/event/ ./cmd/worker/
结果：repository/redis ok 95.487s；worker ok 22.701s；event ok 1.062s；cmd/worker ok 1.014s；
      执行后 db15 dbsize = 0

命令：NCS_REDIS_TEST_ADDR=127.0.0.1:6379 go test -count=1 -cover ./...
结果：repository/redis 86.2%；worker 85.1%；event 73.5%；config 100.0%；httpapi 83.3%；cmd/worker 25.2%
```

四项反向验证的实际失败输出已记在上面的表格里。

### 第 6 轮接口变更（需要 A 线在 PG 实现时对齐）

| 接口 | 变更 | 语义要点 |
|---|---|---|
| `event.ConsumptionStore` | 新增 `AbortDeadLetter(reservation, reason) (bool, error)` | 声明持有者放弃声明：置回 `FAILED` 并清租约。仅在 owner 一致且状态仍为 `DEAD_LETTERING` 时成功 |
| `event.ConsumptionStore` | `Begin` 语义补充 | `DEAD_LETTERING` 视为"进行中"：租约内不得被接管；租约过期后可接管（崩溃恢复） |
| `worker.DeadLetterRecorder` | 新增 `AbortDeadLettered` | 与存储一一对应 |

PG 侧建议：`AbortDeadLetter` 用 `UPDATE ... WHERE event_id=$1 AND owner=$2 AND outcome='DEAD_LETTERING'` 并检查
影响行数；`Begin` 的"进行中"判定需把 `outcome='DEAD_LETTERING' AND lease_until > now()` 与普通预留一并处理。

## 第 5 轮：两项修复

### [P0] 1. 旧消费者可覆盖新消费者的成功终态

**确认属实，而且我第 4 轮的死信状态机只解决了"写入方之间"，没有解决"代次之间"。**

`MemoryConsumptionStore.DeadLetter(record, reason)` 是**无条件写终态**的：它只按 event_id 找记录，不看
调用者是谁。于是下面这条常见时间线会把成功改写成死信：

1. 投递 A 预留事件（`Reservation.Owner = A`，租约 5 分钟）；
2. A 的处理器执行超过租约 → 投递 B 合法接管（`Owner = B`），B 应用成功并写入 `SUCCEEDED`；
3. A 这时才失败（永久错误）→ 走死信路径：写 DLQ 条目、把记录改成 `DEAD_LETTERED`、ACK 源条目。

结果：一个**业务上已经成功**的事件被记为死信，运维会去追一个已完成的事件；而且 A 的 ACK 会删掉 B 可能仍在
使用的那份源条目。

**修复：把"死信决定"变成权威存储上的一次原子声明，并且只有当前代次能取得它。**

| 步骤 | 调用 | 作用 |
|---|---|---|
| 0 | `reservation` 随失败一起从流水线传到 Worker | `attemptError` 新增 `Reservation()`；只有拿到预留才可能证明"我还是当前代次" |
| 1 | `ConsumptionStore.BeginDeadLetter(reservation, reason)` → `granted`/`superseded` | 原子声明 `DEAD_LETTERING`（**非终态**）。只有 `reservation.Owner` 与记录当前 owner 一致、且记录未终态时才 `granted`；否则 `superseded` |
| 2 | `superseded` → **完全静默** | 不写 DLQ、不写记录、不 ACK、不上报任何投递结果 |
| 3 | 写 DLQ + 发布"已写入"（Redis 侧 guard，与第 4 轮的声明状态机同一套） | 仍然负责条目去重 |
| 4 | `ConsumptionStore.FinalizeDeadLetter(reservation, record, reason)` → `bool` | **比较并交换**：只有声明仍是 `DEAD_LETTERING` 且 owner 仍是本人时才写 `DEAD_LETTERED` |
| 5 | `false` → 同样**完全静默**；`true` → 才 ACK、才上报 | 写入期间租约可能已过期、事件可能已被新代次完成 |

三点设计取舍：

- **为什么需要两步**：DLQ 条目写在存储之外，声明与写入之间有一个真实窗口（第 4 步是唯一能发现"事件已经
  易主"的地方）。写入前声明、写入后 CAS，是这个窗口的最小闭合方式。
- **`DEAD_LETTERING` 刻意不是终态**：条目还没写，写入还可能失败；把它当终态会让后续投递跳过一个"两个流里
  都没有"的事件。
- **无预留的调用者按记录判定**：不经过流水线的处理器（例如 Router 因事件类型未知直接返回 Permanent）没有
  预留可证，此时改用"记录已终态或正被他人处理 → 拒绝"，仍能挡住"路由配置变更后旧 Worker 停放别人已应用的
  事件"。这条比预留检查弱，但方向一致。

**反向验证**（三处，逐一回退后对应测试必须失败）：

- 回退 Worker 的第 1/4 步检查 → `applied work must not be parked, got 1 dead letters`；
- 回退存储声明的 owner/终态检查 → `expected the stale attempt to be refused, got granted`；
- 回退 `FinalizeDeadLetter` 的比较交换 → `expected the finalisation to be refused after the event moved on`。

回归测试（含你要求的并发场景）：`TestWorkerStaysSilentWhenANewerAttemptAlreadyAppliedTheEvent`（**旧消费者超出
租约 + 新消费者成功 + 旧消费者迟到失败**：断言记录仍为 `SUCCEEDED`、无 DLQ 条目、源条目保持 pending）、
`TestWorkerStaysSilentWhenANewerAttemptIsStillProcessing`、`TestDeadLetterRecordingRequiresTheCurrentAttempt`、
`TestDeadLetterWithoutAReservationStillRespectsAFinishedEvent`，以及存储侧
`TestMemoryConsumptionStoreRefusesADeadLetterFromASupersededAttempt`、
`...WhileANewerAttemptIsInProgress`、`...RefusesToFinaliseAfterTheEventMovedOn`、`...WithoutAnOwner`、
`...DeadLetteringIsNotTerminal`。

### [P1] 2. 缺少死信记录器时仍允许可靠消费启动

**确认属实，而且这正是 P0 修复所依赖的前提**：没有记录器，Worker 就无法问"我还是当前代次吗"，于是上面
那套保护完全不存在——部署看起来健康，却会静默写坏消费记录。

**修复：记录器改为必需，缺少则拒绝消费。**

- `Worker.Run` 在连接任何东西之前先检查（`checkReadyToConsume`），失败即返回
  `dead-letter recorder is required: ...`；
- 因此 `Runner` 也会在启动阶段失败，而不是"启动了但少了保证"；
- 这也顺带回答了第 3 轮遗留的开放问题（"未配置记录器时是否应拒绝启动"）——按你的意见：**拒绝**。
- `cmd/worker` 一直会接线记录器，生产路径不受影响；受影响的是"用内存记录器裸跑 Worker"的测试，已逐一接线。

反向验证：回退该检查 → `TestWorkerRefusesToConsumeWithoutADeadLetterRecorder` 与
`TestRunnerRefusesToStartWithoutADeadLetterRecorder` 同时失败。回归测试另含 `TestRunnerRefuses...` 中断言
"拒绝发生在任何连接被使用之前"（Ping 计数为 0）。

原来的 `TestDeadLetteringIsOnlyTerminalWhenRecorded` 有一个 "without recorder" 子用例，它固定的正是本轮被否定
的行为（"没有记录器仍可消费，只是终态不落库"）。该子用例已删除并替换为上述拒绝测试，以免文档里留下一个
已经不允许运行的配置。

### 第 5 轮验证记录

在 `charging-station-platform-backend-events/backend` 下执行（分支 `b-04-charge-command-workers`，
基点 `22fd8f6`，Redis 版本 `7.0.15`）：

```text
命令：gofmt -l ./cmd ./internal
结果：无输出

命令：go vet ./... / go build ./...
结果：均通过

命令：NCS_REDIS_TEST_ADDR=127.0.0.1:6379 go test -count=1 -race ./...
结果：全部 ok（repository/redis 4.758s、worker 2.044s、event 1.015s、cmd/worker 1.011s）

命令：NCS_REDIS_TEST_ADDR=127.0.0.1:6379 go test -count=20 -race -timeout 900s \
      ./internal/repository/redis/ ./internal/worker/ ./internal/event/ ./cmd/worker/
结果：repository/redis ok 95.988s；worker ok 22.937s；event ok 1.083s；cmd/worker ok 1.019s；
      执行后 db15 dbsize = 0

命令：NCS_REDIS_TEST_ADDR=127.0.0.1:6379 go test -count=1 -cover ./...
结果：repository/redis 86.2%；worker 85.1%；event 78.7%；config 100.0%；httpapi 83.3%；cmd/worker 25.2%
```

P0 的两层保护与 P1 的启动拒绝都各做了反向验证，观察到的失败输出已记在上面每一节里。

### 第 5 轮接口变更（需要 A 线在 PG 实现时对齐）

| 接口 | 变更 | 语义要点 |
|---|---|---|
| `event.ConsumptionStore` | **移除** `DeadLetter(record, reason)`，**新增** `BeginDeadLetter(reservation, reason) (DeadLetterClaim, error)` 与 `FinalizeDeadLetter(reservation, record, reason) (bool, error)` | 前者是"只有当前代次能取得的原子声明"；后者是比较并交换，返回 `false` 时调用方必须完全静默。PG 侧建议：声明用 `UPDATE ... WHERE event_id=$1 AND owner=$2 AND outcome IS NOT NULL ...` 形式，终态写入用 `UPDATE ... WHERE outcome='DEAD_LETTERING' AND owner=$2` 并检查影响行数 |
| `event.ConsumptionOutcome` | 新增 `OutcomeDeadLettering`（**非终态**） | "已决定停放，条目尚未写出" |
| `event` | 新增 `DeadLetterClaim`（`granted`/`superseded`） | 拒绝表示"事件已属于更新的代次或已终态" |
| `worker.DeadLetterRecorder` | 两个方法取代原来的 `RecordDeadLettered` | 与存储接口一一对应 |
| `worker` | **必需**依赖：`SetDeadLetterRecorder` 未接线时 Worker 拒绝消费 | 缺少它则 P0 保护不存在 |

## 第 4 轮：四项修复

四项均确认属实。第 1 项会让事件永久丢失，第 2、3 项让重试预算被错误消耗或错误判定，第 4 项让文档承诺的能力实际不可用。

### [P0] 1. "正在写入"被当成"已经写入"

**确认属实，且是第 3 轮"修复死信顺序"时留下的盲点。**

原接口只有 `ClaimDeadLetter(...) (token string, claimed bool, err error)`，于是"别人持有声明"（`claimed=false`）
被当作"已存在死信条目"。多 Worker 场景下 A 取得声明后正在执行或被阻塞于 `XADD`，B 接管 pending 后发现声明被
占用 → 记录终态 + ACK；若 A 的 `XADD` 随后失败，则**源条目已被 ACK、DLQ 又没有消息**，事件永久丢失。

**修复：声明必须携带状态，"已写入"必须由完成方显式发布。**

Redis 侧新增 B 线自有的真实实现 `redis.DeadLetterClaims`，用两个键区分两件事：

| 键 | 含义 | TTL |
|---|---|---|
| `ncs:idempotency:dead-letter-write:{event_id}` | **写入权**：仅用于串行化写入者，不代表写成功 | 默认 1 分钟（只需覆盖一次 XADD） |
| `ncs:idempotency:dead-letter-written:{event_id}` | **已完成写入的证据**：唯一可以据此跳过写入的事实 | 默认 7 天（覆盖"写完但 ACK 前崩溃、很久以后才重启"的窗口） |

`Claim` 返回三态，并且**先发布证据、再释放写入权**，因此"取写入权失败"的投递若看到 `writing`，必然意味着写入者
尚未完成；已完成者一定会被看到 `written`：

- `taken` —— 本投递负责写入；
- `written` —— 已完成写入存在，只需补记终态并 ACK（不重复写）；
- `writing` —— 他人正在写，**结果未知**。

Worker 侧：`writing` 返回新的 `deadLetterDeferred` 结果 —— **不记录终态、不 ACK**，条目保持 pending，等声明释放或
过期后重试。这条正是 P0 的修复点：ACK 是唯一不可逆的一步，在不知道别人写没写成功之前绝不能走。

降级方向是刻意选择的：Redis 不可用时按"没写过"处理（仍然写入、可能产生重复死信），绝不按"已写过"处理；
`MarkDeadLetterWritten` 失败会返回错误，但**不使本次停放失败**（条目已经在 DLQ 里，失败只会让后续重试多写一份），
并把失败计入 `idempotency` 能力的降级计数器。重复死信可见可删，丢失事件不可见。

回归测试（Worker 侧）：`TestWorkerLeavesTheEntryPendingWhileAnotherConsumerIsWritingTheDeadLetter`、
`TestWorkerParksTheEventAfterTheOtherConsumerGivesUpTheWrite`、`TestWorkerSkipsTheWriteWhenACompletedOneIsOnRecord`；
（Redis 侧）：`TestDeadLetterClaimsDistinguishAnUnfinishedWriteFromACompletedOne`、
`TestMarkingWrittenReleasesTheClaimAndPublishesTheWrite`、`TestReleasingAWriteClaimRequiresTheOwningToken`、
`TestDeadLetterClaimsPreferWritingAgainWhenRedisIsUnavailable`、`TestDeadLetterClaimMarkersCarryTheConfiguredLifetimes`，
以及两条真实 Redis 集成测试 `TestIntegrationDeadLetterClaimsAgainstRealRedis`、
`TestIntegrationDeadLetterClaimReleaseAgainstRealRedis`。

**反向验证**：把 `writing` 按旧逻辑处理（记录终态 + ACK）后，前两个 Worker 测试立即失败：
`expected the entry to stay pending, got 0 pending`。

### [P1] 2. 等待 Guard 仍然消耗存储侧尝试次数

**确认属实，是第 3 轮那个问题的姊妹问题（同一处预算，另一条路径）。**

第 3 轮已把预算从"传输投递次数"改为"存储的尝试次数"，但"Guard 被占用"和"Guard 调用失败"这两条**处理器根本没
运行**的路径仍然调用 `store.Fail`，而 `Fail` 之后的下一次 `Begin` 会 `Attempts++`。20 次等待之后，第一次真实
业务失败就已经是 attempt 21 > `MaxAttempts(3)` → 直接死信。

**修复：区分"尝试失败"与"未成尝试的放弃"。**

- `event.ConsumptionStore` 新增 `Release(ctx, reservation, reason) error`，实现新增 `event.OutcomeAbandoned`（非终态）。
- `Begin` 接管一个"被放弃"的预留时**不递增**尝试次数，因此下一次 `Begin` 授予的是**同一个**尝试号。
- 流水线的两条 Guard 拒绝路径改用 `Release`；`Fail` 继续用于处理器真正运行并失败的路径。
- 放弃同样有防护：外来 token 不得释放别人的尝试，终态事件不得被放弃（各有测试）。

回归测试：`TestWaitingForTheDuplicateGuardDoesNotSpendTheRetryBudget`（20 次等待后首次真实尝试必须仍是 attempt 1，
且不因预算耗尽而死信）、`TestMemoryConsumptionStoreReleaseDoesNotSpendAnAttempt`（10 次等待仍为 attempt 1，
一次 `Fail` 之后为 attempt 2）、`TestMemoryConsumptionStoreReleaseRefusesAStaleOwner`、
`TestMemoryConsumptionStoreReleaseIgnoresATerminalEvent`；并把
`TestPipelineTreatsAGuardHeldAfterAGrantedReservationAsHeldNotDuplicate` 与
`TestPipelineReleasesTheReservationWhenTheGuardFails` 的断言改为 `ABANDONED` + "下一次 Begin 仍是 attempt 1"。

**反向验证**：把两处 `Release` 改回 `Fail` → `expected the first real attempt to be attempt 1, got 21`。

### [P1] 3. `Complete` 失败未携带 `Reservation.Attempt`

**确认属实。** 原代码在 `store.Complete` 失败时返回普通错误，Worker 因此回退到传输投递次数（可能已被 XCLAIM
放大到几十甚至几百）→ 一次**基础设施写终态失败**就把一个**业务上已经应用成功**的事件死信。

**修复**：`Complete` 失败与处理器失败一样包装 `attemptError{attempt: reservation.Attempt}`。

语义上还有一点值得说明：事件已经应用，所以重试**不会**立刻重放——预留租约仍在，后续投递会以 `ErrLeaseHeld`
被拒并保持 pending；租约到期后按 attempt 2、3 继续，第 3 次才允许停放。这与"事件已经应用过一次"的事实一致。

回归测试：`TestAFailedTerminalWriteIsNotChargedToTheTransportDeliveryCount`（传输计数设为 50、预算 3：断言未死信、
未记为 `DEAD_LETTERED`、条目仍 pending；随后两次真实尝试恰好用尽预算才停放一次）。

**反向验证**：去掉 `attemptError` 包装 → `an infrastructure write failure must not be dead-lettered, got 1 dead letters`。

### [P2] 4. `Config.StartID` 未透传到 `StreamConfig`

**确认属实，且与我自己的文档矛盾。** `Runner` 通过 `StreamConfig.toConfig()` 构造每个 Worker 的 `Config`，而
`StreamConfig` 根本没有 `StartID` 字段，因此经 Runner 启动的进程只能拿到默认 `0-0`。第 2 版审批单与 B-05 审批单都
写过"运维显式把起点配成 `$`"，而那条路径实际不可用。

**修复**：`StreamConfig` 新增 `StartID` 并在 `toConfig()` 透传（空值仍取默认"流的开头"）；`cmd/worker` 的
`-streams` / `NCS_WORKER_STREAMS` 支持 `stream=group@start_id` 第三段，`$` 可显式传入，并拒绝空起点。

回归测试：`TestRunnerPassesTheConfiguredStartPositionToTheWorker`、
`TestRunnerDefaultsTheStartPositionToTheBeginningOfTheStream`、`TestBuildStreamsCarriesTheStartPosition`、
`TestBuildStreamsRejectsAMalformedStartPosition`。

**反向验证**：去掉 `toConfig()` 里的透传 → `expected the group to be created at "$", got "0-0"`。

### 第 4 轮验证记录

在 `charging-station-platform-backend-events/backend` 下执行（分支 `b-04-charge-command-workers`，
基点 `d6b257a`，Redis 版本 `7.0.15`）：

```text
命令：gofmt -l ./cmd ./internal
结果：无输出

命令：go vet ./...
结果：通过，无告警

命令：go build ./...
结果：通过

命令：NCS_REDIS_TEST_ADDR=127.0.0.1:6379 go test -count=1 -race -timeout 300s ./...
结果：全部 ok（repository/redis 5.889s、worker 2.102s、event 1.015s、cmd/worker 1.011s、
      config、httpapi 均 ok）；执行后 db15 dbsize = 0

命令：NCS_REDIS_TEST_ADDR=127.0.0.1:6379 go test -count=20 -race -timeout 900s \
      ./internal/repository/redis/ ./internal/worker/ ./internal/event/ ./cmd/worker/
结果：repository/redis ok 95.858s；worker ok 22.927s；event ok 1.086s；cmd/worker ok 1.025s；
      执行后 db15 dbsize = 0

命令：NCS_REDIS_TEST_ADDR=127.0.0.1:6379 go test -count=1 -cover ./...
结果：repository/redis 86.2%；worker 85.6%；event 79.3%；config 100.0%；httpapi 83.3%；
      cmd/worker 25.2%
```

四项修复各自都做了反向验证（把修复临时回退，确认对应测试失败），观察到的失败输出已记在上面每一节里。

### 修好了两个既有测试缺陷（仅测试代码，需要你知悉）

第 4 轮重复运行（`-count=20`，`go test` 并发跑 4 个包，也就是你复现时最可能用的方式）暴露出两个**既有测试**
的负载敏感问题。两者都不是本模块的业务缺陷，但会让你在复审时看到随机失败，所以本轮一并修好。

**1. 阻塞读的时间下界**：`TestIntegrationStreamsBlockingReadTimesOutCleanly` 间歇失败

```text
--- FAIL: TestIntegrationStreamsBlockingReadTimesOutCleanly (0.13s)
    streams_client_integration_test.go:108: expected the read to block for about 400ms,
    returned after 125.318578ms
```

该断言要求"阻塞读至少阻塞 200ms"。**我没有查明这次早返回的成因**，因此**没有继续调阈值**，而是移除这个时间
下界：阈值贴近请求的 block 时长时，它衡量的是服务端在负载下的调度，而不是被测行为（单独跑 20 次全过）。
真正要防的"阻塞读完全不阻塞"（约 1ms 返回）由 `TestStreamsReadGroupFramesBlockAndNoAck` **确定性地**覆盖：
它对着脚本化服务端断言发出的就是 `BLOCK <ms>` 且参数等于请求值。测试注释里写明了这个判断，避免以后有人
盲目把下界加回来。

**2. 积压消费测试的确认竞态**：`TestIntegrationRedisConsumesBacklogPublishedBeforeGroupCreation` 间歇失败

```text
--- FAIL: TestIntegrationRedisConsumesBacklogPublishedBeforeGroupCreation (0.01s)
    integration_redis_test.go:164: expected no pending entries, got 1
```

原测试在处理器"看到"全部事件后立即 `cancel()`，与最后一条 `ACK` 竞争：处理器返回不等于 ACK 已完成，取消可能
恰好插在两者之间，于是留下 1 条 pending。这是测试自身的竞态，不是 Worker 的问题（Worker 在取消前会跑完当前
命令）。修复：先等待 pending 清零再取消，并在注释里写明原因。

两处改动 B-05 分支此前都做过（原因相同），因此这次顺带消除两处 rebase 冲突。

### 第 4 轮接口变更（需要 A 线在 PG 实现时对齐）

| 接口 | 变更 | 语义要点 |
|---|---|---|
| `event.ConsumptionStore` | 新增 `Release(ctx, reservation, reason) error` | 不记录尝试的释放。**下一次 `Begin` 必须授予相同的尝试号**，否则等待会重新变成消耗预算 |
| `event.ConsumptionOutcome` | 新增 `OutcomeAbandoned`（非终态） | 事件未到达处理器就被放弃，可立即重试 |
| `worker.DeadLetterGuard` | `ClaimDeadLetter` 由 `bool` 改为三态 `DeadLetterClaimState`；新增 `MarkDeadLetterWritten` | `writing` 必须"保持 pending"，只有 `written` 才可跳过写入并 ACK |
| `worker.StreamConfig` | 新增 `StartID` | 空值 = 流的开头 |
| `redis.Capabilities` | 新增 `DeadLetterClaims` + `Settings.DeadLetter` | 新增环境变量 `NCS_REDIS_DEAD_LETTER_WRITE_TTL`、`NCS_REDIS_DEAD_LETTER_WRITTEN_TTL` |

第 3 轮遗留的开放问题（未配置 `DeadLetterRecorder` 时是否应拒绝启动）本轮未决，仍待你的结论。

## 第 3 轮：三项交叉故障路径修复

三项均确认属实，其中前两项都可能让事件**既不在源流、也不在死信流**——比重复或延迟都更糟。

### [P0] 1. DLQ 写入失败后重试会误 ACK

**确认属实，且是我上一轮"修复死信顺序"时引入的缺陷。**

原逻辑：先 `ClaimDeadLetter` 取得 Guard，再执行 `XADD`。若 Guard 成功而 `XADD` 失败，**Guard 从不释放**。
重试时 Guard 已存在 → 跳过写入、记录终态、ACK 源条目 → 事件两个流里都没有。

**修复**：`DeadLetterGuard` 改为 `ClaimDeadLetter` 返回 token + 新增 `ReleaseDeadLetter`；`XADD` 失败时
用该 token 释放声明。对"结果未知"的写入（`ErrUnavailable`）释放会带来重复死信，这是**刻意的取舍**：
重复死信可见且可人工删除，丢失事件不可见。

回归测试：`TestDeadLetterReleasesItsGuardWhenTheParkFails`（先失败写入 → 断言声明已释放、条目仍 pending、
未产生死信；再放开写入 → 断言重试成功停放并 ACK）、
`TestDeadLetterGuardReleaseRequiresTheOwningToken`（外来 token 不得释放声明）。

### [P0] 2. 旧 Guard 会阻断租约接管并导致 ACK

**确认属实。** 消费租约 5 分钟、Guard TTL 10 分钟。进程取得两者后崩溃：5 分钟后新 Worker 合法接管租约，
但旧 Guard 仍在。原逻辑把 Guard 占用返回为 `ErrDuplicate` → Worker ACK → 事件丢失。

**修复：Guard 占用在** `Begin` **已判定 Reserved 之后，一律报 `ErrLeaseHeld`，绝不报 `ErrDuplicate`。**

依据是权威归消费记录：`Begin` 返回 `Reserved` 意味着**不存在其他活跃预留**，因此持有 Guard 的一方此刻
并未在处理该事件（是已死尝试的陈旧声明，或另一个进程的本地存储看不到我们）。`ErrDuplicate` 会让 Worker
ACK，那就等于丢弃一个权威存储刚判定"无人消费过"的事件。报 `ErrLeaseHeld` 则保持 pending，等声明过期后
继续，**不丢**。

另外把 `GuardConfig.TTL` 默认值从 10 分钟对齐到 5 分钟（= 默认租约），并在字段注释中写明"声明不得长于
消费租约"，以减少接管等待时间；但**保证不丢的是行为修复，TTL 对齐只是缩短延迟**。

回归测试：`TestPipelineTreatsAGuardHeldAfterAGrantedReservationAsHeldNotDuplicate`（断言不是
`ErrDuplicate`、未应用、预留已释放）、
`TestWorkerDoesNotAcknowledgeWhenAStaleGuardOutlivesTheLease`（构造"租约已过期但 Guard 仍在"，断言条目
**保持 pending 且未被 ACK**）、`TestPipelineTrustsTheStoreOverTheGuardForAnAlreadyConsumedEvent`
（反向：存储判定已消费时 Guard 不被咨询、直接 ACK）。

### [P1] 3. `ErrLeaseHeld` 实际仍消耗 Redis 投递次数

**确认属实。** Pending 恢复每次 `XCLAIM` 都会增加 Redis 投递计数；租约等待 5 分钟（配 `RetryAfter`
250ms）可累计约 1200 次。原代码注释声称"不计入重试预算"，但 `MaxAttempts` 判断用的是
`delivery.DeliveryCount`——**注释与实现不一致**，接管后的第一次真实瞬时失败会直接死信。

**修复：重试预算改用消费记录自己的尝试次数。**

- `event.Reservation.Attempt` 现在是**存储自己的计数**：首次预留为 1，每次重新预留（失败重试或租约接管）
  +1，与传输报告的次数无关。
- `ConsumptionRecord.Attempt` 保留传输计数，仅用于诊断（它反映 Redis 重投递了多少次）。
- 处理器失败时，流水线把该尝试次数附着在错误上（`attemptError`，`Unwrap` 保持
  `errors.Is`/`errors.As`/永久标记透传）；Worker 用 `attemptOfError` 读取，读不到时回退到传输计数。
- 死信条目记录的也是存储的尝试次数。

回归测试：`TestWorkerRetryBudgetIgnoresTransportReclaims`（把传输计数设为 500、预算 3：断言三次真实尝试
才死信、每次都收到正确 attempt、死信记录为 3）、`TestPipelinePassesTheStoresAttemptCountToApply`、
`TestAttemptOfErrorReadsThroughWrapping`。

### 修复过程中发现并已用测试固定的一个依赖

**死信只有在配置了 `DeadLetterRecorder` 时才会在存储中变成终态**——那是通往存储的唯一通道。没有它时
Guard 仍能防止重复停放条目，但**处理器会被再次执行**。

`cmd/worker` 始终会接线，因此生产路径不受影响。但这是"可选依赖导致语义变化"，我选择用测试把两种行为都
钉住（`TestDeadLetteringIsOnlyTerminalWhenRecorded`），而不是留着不说。如果你希望改为"未接线则拒绝启动"
（与第 1 轮 P0-1 同样的思路），我可以照办——那会改变构造语义，所以我没有擅自决定。

### 第 3 轮验证记录

```text
命令：gofmt -l . / go build ./... / go vet ./...
结果：全部通过

命令：NCS_REDIS_TEST_ADDR=127.0.0.1:6379 go test -race ./...
结果：全部 ok

命令：NCS_REDIS_TEST_ADDR=127.0.0.1:6379 go test -count=20 -race ./internal/repository/redis/ ./internal/worker/ ./internal/event/ ./cmd/worker/
结果：repository/redis ok 100.703s；worker ok 22.616s；event ok 1.086s；cmd/worker ok 1.018s；
      执行后 db15 dbsize = 0
```

### 关于本轮一次"大面积失败"的说明

本轮中途出现过一次 Redis 集成测试大面积失败（大量 `consumer group does not exist`、`pending 得到 2`）。
**那不是缺陷**：一个更早 `-count` 运行遗留的 `redis.test` 进程仍在运行并删除同一批 key，而我此前用
`pgrep -f` 做的进程检查把**自己的命令行**也匹配了进来，所以一直误判为"已清理"。改用 `ps -eo comm`
按进程名核对并 `kill -9` 该残留进程后，同样的测试全部通过。现在验证运行已串行化。

## 基本信息

- 模块编号：BE-B-04
- 开发线：B
- 开发分支：`codex/migration/backend-events/b-04-charge-command-workers`
- 第 1 轮被审查的提交：`e2fe6f6`
- 第 2 轮修复：`11e3377`（材料 `a6f4b0a`）
- 第 3 轮修复：`2ec5390`（完整：`2ec539047ad45962c1e7f89544b860c53c75a04a`）
- 第 4 轮修复：`f2e24e6`（材料 `22fd8f6`）
- 第 5 轮修复：`4a61fe7`（材料 `ad4c524`）
- 第 6 轮修复：`a7f6cd2`（本轮审查对象为 `ad4c524`，即第 5 轮后的分支顶端）



## 基本信息

- 模块编号：BE-B-04
- 开发线：B
- 开发分支：`codex/migration/backend-events/b-04-charge-command-workers`
- 第 1 轮被审查的提交：`e2fe6f6`
- 本轮修复提交：见文末"提交记录"
- 规模：12 个文件修改、4 个文件新增

### 一处流程更正（我的错误）

本轮的 5 项修复最初是在 `b-05-recovery-observability` 分支的工作区里写的（我在创建 B-05 后没有切回 B-04）。
发现后已按正确流程处理：把修复移回 `b-04-charge-command-workers` 分支，并还原 B-05 的工作区，
**B-05 分支未被污染**（仍是你审查时的 `271de62`/`83eabdc`，不含本轮修复）。B-05 日后需要 rebase 到
修好的 B-04 之上。

另外，第 1 轮的 5 项修复**只落在 B-04 分支**，因此其中不包含 B-05 的观测层代码。

## 5 项阻断项的处置

### [P0] 1. Worker 会 ACK 实际未执行的业务

**确认属实，且是本轮最严重的一项。**

`loggingApplier`/`loggingDispatcher` 记录 `applied=false`/`dispatched=false` 却返回 `nil`；
`pipeline.run` 因此判定成功、`store.Complete` 记为 SUCCEEDED、Worker ACK 源条目。结果是充电事件与
设备命令被一个什么都没做的进程消费掉，且**源条目已消失，任何重试都无法找回**。

**修复：进程拒绝启动。** 删除了两个占位实现，新增 `domainAdapters()`；它当前必然返回错误，因为：

- 两个 applier 需要 PostgreSQL 订单域（属 A 线，且订单表刚由 `ee39e9f` 对齐）；
- dispatcher 需要充电桩网关。

我选择"拒绝启动"而不是"返回失败"：返回失败会让事件在重试预算内空转并最终进入死信，而根因是部署
缺件、不是事件本身有问题——那会把整条流的积压全部打成死信。拒绝启动是唯一不破坏数据的选项，也与
契约第 8 节"依赖未就绪"的语义一致。

```
$ NCS_REDIS_DB=15 ./ncs-worker
{"level":"INFO","msg":"redis reachable",...}
{"level":"ERROR","msg":"configure event routing",
 "error":"domain adapters are not wired: the charge event applier and command result applier need
          the PostgreSQL order domain (owned by the A line) and the command dispatcher needs the
          charger gateway"}
```

**直接后果需要你知悉：在 A 线提供 applier、充电桩网关提供 dispatcher 之前，`cmd/worker` 无法运行。**
这是刻意的——"不可运行但正确"优于"可运行但会静默丢失业务"。

回归测试：`TestDomainAdaptersAreNotWired`、`TestBuildRouterRefusesWithoutAdapters`，并断言错误信息
点名 PostgreSQL 与 charger gateway，便于定位。

### [P0] 2. 首次创建消费者组会跳过已有消息

**确认属实。** `EnsureGroup(..., "$")` 把新组定位在流末尾。Outbox 在 API 事务提交后立即发布，因此
"先发布、Worker 后首次启动"是**正常时序**，而这条时序下全部积压会被永久跳过。

**修复：** 新增 `Config.StartID`，默认 `"0-0"`（从流起点消费），经 `Runner`→`Worker`→`EnsureGroup`
传递。想要忽略历史积压的部署可显式设 `"$"`，取舍写在该字段的注释里。

**同时更正我在 B-05 材料中的一处判断错误。** 我在 B-05 里用
`TestIntegrationGroupCreatedAtHeadReportsNoLag` 观察到了这个语义，却把它写成"刻意的、首次部署不回放
全量历史"，并据此认为无需处理。那是**错的**：对 P0 闭环而言这不是设计选择而是数据丢失。该测试本身
仍然有效（它验证适配器在显式 `"$"` 下的真实语义），但结论已在 B-05 材料中标记待更正。

**"先发布、后建组"的真实 Redis 测试（你明确要求）已补齐：**

| 测试 | 断言 |
|---|---|
| `TestIntegrationRedisConsumesBacklogPublishedBeforeGroupCreation` | 先 XADD 5 条、确认组不存在，再启动 Worker → 5 条全部被消费并 ACK，无 pending，无残留 |
| `TestIntegrationRedisStartIDAtHeadSkipsTheBacklog` | 显式 `"$"` 时 0 条被消费，且新建的 `0-0` 组仍能看到这 3 条，证明是"跳过"而非"消费" |
| `TestIntegrationRedisConsumesNewEventsAfterGroupCreation` | 组已存在后发布的正常路径仍然消费 |

**反向验证**（确认测试真能抓到这个缺陷）：把默认起点临时改回 `"$"` 后，
`...ConsumesBacklogPublishedBeforeGroupCreation` 失败并报告：

```text
expected all 5 pre-existing events to be consumed, got 0: map[]
```

即你描述的"全部积压被跳过"。恢复后通过。

### [P0] 3. 消费预留状态会在崩溃后导致事件被误 ACK

**确认属实。** `Begin` 只返回 `bool`，把"已完成"与"仍被其他进程预留"合并成同一个 `false`，而调用方
对二者的正确反应恰好相反。在 `Begin` 之后、`Apply` 之前崩溃，重启后该事件会被当成重复事件 ACK——
**没有任何人执行过它**。

**修复：契约改为返回三态预留结果。**

```go
type ReservationState string
const (
    ReservationReserved        // 本次投递拥有该次尝试，必须执行
    ReservationAlreadyConsumed // 已到终态，必须 ACK 但不得重复执行
    ReservationInProgress      // 有活跃租约持有它，必须「不」ACK、保持 pending
)

type Reservation struct {
    EventID        string
    State          ReservationState
    Owner          string    // 本次预留的所有者，Complete/Fail 必须携带
    Attempt        int
    LeaseExpiresAt time.Time // 租约到期后下一次投递可接管
}

type ConsumptionStore interface {
    Begin(ctx, record) (Reservation, error)
    Complete(ctx, reservation, outcome, detail) error
    Fail(ctx, reservation, reason) error
    DeadLetter(ctx, record, reason) error   // 见第 4 项
}
```

- **租约接管**：租约到期后，下一次投递取得预留并继续处理，因此崩溃是可恢复的而不是永久卡死。
- **所有者校验**：`Complete`/`Fail` 携带 `Owner`，被接管后旧持有者**不能**再把事件标记为已完成，
  其 `Fail` 也变成 no-op，不会干扰新尝试。
- **Worker 行为**：新增 `ErrLeaseHeld` 哨兵；`process` 遇到它时**保持 pending、不 ACK、不计入重试预算**
  （因为并没有失败），等租约到期后由 Pending 恢复接管并执行。

你给出的另一条路径——**把消费记录与领域变更放进同一个 PostgreSQL 事务**——是更强的做法，因为它消除了
这个崩溃窗口而不是描述它。这一点已写入 `ConsumptionStore` 的文档注释，作为 A 线实现时的两个可选项，
并明确推荐后者。我无法自行实现该事务：`backend/internal/repository/postgres/` 属 A 线只读。

回归测试：`TestMemoryConsumptionStoreInProgressIsNotConsumed`、
`...TakesOverAnExpiredLease`、`...RefusesAStaleOwner`、
`TestWorkerDoesNotAcknowledgeAnEventHeldByALiveLease`（断言条目仍 pending 且 handler 未被调用）、
`TestPipelineReportsAHeldEventDistinctlyFromADuplicate`、
`TestWorkerRecoversAnEventWhoseLeaseExpired`（租约内不执行 → 过期后执行并 ACK）。

### [P1] 4. 死信操作顺序不可靠

**确认属实。** 原顺序 `DLQ → ACK → REC` 两处都能致命：ACK 成功而 REC 失败时源条目已不可恢复；DLQ 成功
而 ACK 失败时重试会再写一条死信。

**修复：新顺序 `guard → DLQ → REC → ACK`，并补幂等标识。** 每一步都由"下一步失败会怎样"决定：

1. **guard**：以 event id 为键声明死信写入，且**永不释放**（"该事件已被停放"不会不再成立）。
   新增 `DeadLetterGuard`/`NewDeadLetterGuard`，复用守卫的 compare-and-set，scope 为 `dead-letter`，
   与处理期声明互不冲突。这就是 XADD 本身无法提供的幂等性。
2. **DLQ**：写入，并额外写入 `dead_letter_event_id` 与 `dead_letter_stream_id`，使运维或后续去重任务
   无需再解析流条目 id 即可识别。
3. **REC**：记录终态，使消费记录与停放条目一致。失败则返回错误、源条目保持 pending。
4. **ACK**：最后执行——它是唯一不可逆的步骤，在此之前所有运维需要的东西必须已经持久化。

重试时若 guard 已被持有，则跳过第 2 步、只重做 3 与 4，因此**不会产生重复死信**。

回归测试：`TestDeadLetterIsIdempotentAcrossAnAckFailure`（用ACK 失败一次的流客户端模拟崩溃；
断言两次处理后死信仍只有 1 条、最终 ACK 成功）、`TestDeadLetterRecordsBeforeItAcknowledges`、
`TestDeadLetterDoesNotRecordWhenTheParkFails`（停放失败时**不得**记录终态）。

### [P1] 5. Redis Guard 的 owner token 不包含真正的持有者

**确认属实。** 原 token 为 `"claim:" + scope + ":" + key`，对所有消费者**完全相同**。旧处理超过 TTL
后另一消费者取得声明，旧消费者随后 `Release` 会删掉新消费者的声明——而这正是租约/TTL 机制本来要保护的
场景。

**修复：每次 Claim 生成唯一 owner 并返回，Release 必须携带该 token。**

```go
type DuplicateGuard interface {
    // 非空 token 表示本次调用拥有声明；空 token 表示他人持有。
    Claim(ctx, scope, key string) (token string, err error)
    // 仅当 token 仍拥有该声明时才删除。
    Release(ctx, scope, key, token string) error
}
```

实现改为 `claim := random 16 bytes hex`（复用已有的 `NewTokenSource`，与订单锁同一套机制），
`Release` 用 `CompareAndDelete(key, token)`。`pipeline` 在本次调用内保存 token 并用于失败时释放。

回归测试：`TestGuardMintsAUniqueOwnerPerClaim`、
`TestGuardStaleReleaseDoesNotDeleteTheCurrentClaim`（旧持有者释放后断言新持有者的声明仍在，且新持有者
仍能正常释放自己的声明）。

## 接口变更汇总（供 A 线实现 PG 存储时对齐）

| 契约 | 变更 |
|---|---|
| `event.ConsumptionStore` | `Begin` 改为返回 `Reservation`（三态 + owner + 租约）；`Complete`/`Fail` 需携带 `Reservation`；新增 `DeadLetter` |
| `event.ConsumptionOutcome` | 新增 `Terminal()` |
| `worker.DuplicateGuard` | `Claim` 返回 owner token；`Release` 需携带 token |
| `worker.DeadLetterGuard` | 新增，`ClaimDeadLetter(eventID)` |
| `worker.DeadLetterRecorder` | 入参由 `event.Event` 改为 `event.ConsumptionRecord`（以便记录流坐标） |
| `worker.Config` | 新增 `StartID`（默认 `"0-0"`） |
| `worker.ErrLeaseHeld` | 新增哨兵，与 `ErrDuplicate` 语义相反 |

这些都在 B 线自有包内，改动不影响 A 线与 H5。

## 验证记录

在 `charging-station-platform-backend-events/backend` 下执行（分支 `b-04-charge-command-workers`），
Redis 版本 `7.0.15`：

```text
命令：gofmt -l .
结果：无输出

命令：go vet ./...
结果：通过，无告警

命令：go build ./...
结果：通过

命令：NCS_REDIS_TEST_ADDR=127.0.0.1:6379 go test -race ./...
结果：全部 ok（含 cmd/worker，repository/redis 5.919s，worker 2.108s）

命令：NCS_REDIS_TEST_ADDR=127.0.0.1:6379 go test -count=20 -race ./internal/repository/redis/ ./internal/worker/ ./internal/event/ ./cmd/worker/
结果：repository/redis ok 100.333s；worker ok 22.625s；event ok 1.072s；cmd/worker ok 1.014s
      执行后 db15 dbsize = 0（无残留键）

命令：NCS_REDIS_TEST_ADDR=127.0.0.1:6379 go test -cover ./internal/event/ ./internal/worker/ ./internal/repository/redis/ ./cmd/worker/
结果：event 79.0%；worker 84.6%；repository/redis 86.9%；cmd/worker 3.7%

命令：反向验证 —— 把 defaultGroupStartID 临时改回 "$"
结果：TestIntegrationRedisConsumesBacklogPublishedBeforeGroupCreation 失败，
      报告 "expected all 5 pre-existing events to be consumed, got 0"

命令：git status --porcelain | grep -E "go\.mod|go\.sum|api/|internal/(httpapi|repository/postgres)/"
结果：无匹配
```

- 测试函数：event 21 个、worker 60 个、repository/redis 269 个
- 新增测试：`internal/worker/review2_test.go`（8 个）、`internal/worker/integration_redis_test.go`（3 个
  真实 Redis 测试）、`cmd/worker/main_test.go`（2 个），以及各包内针对新契约的测试

### 验证过程中发现并修复的两个测试缺陷（仅测试代码）

为了拿到"连续 20 次"的证据，我用 `-count=20 -race` 并让多个包并发运行，暴露出两个**测试自身**的缺陷。
二者都不是产品代码问题，但会污染验证结论，因此一并修掉：

1. **`internal/repository/redis/watcher_race_test.go` 的 `awaitReplyDelivery` 会抛硬币**（B-01 引入、
   被 B-04 继承）。`serveScript` 的 defer 在**成功时也会**关闭 `signals.finished`，因此当
   `bytesConsumed` 与 `finished` 几乎同时关闭时，`select` 随机选中其一，测试会以
   "the scripted server stopped before delivering the reply" 失败——而应答其实已经送达。
   修复：只等待 `bytesConsumed`，超时后才用 `finished` 区分"服务端提前退出"与"客户端从未读取"。
   用 `-count=100 -race` 复压该测试通过。

   **这意味着 B-01 第 3 轮的"20 连跑"结论当时带有运气成分**；该缺陷已在本模块修复，请你决定是否需要
   在 B-01 的审批记录上补注（我只改了测试，产品代码未动）。

2. **`TestIntegrationStreamsBlockingReadTimesOutCleanly` 的时间下界过紧**（B-04 引入）。原来的断言是
   `Block=200ms` 下要求 `elapsed >= 150ms`，在一次并发压测中观测到 `127ms` 而失败。阻塞等待由 Redis
   服务端调度，断言贴近请求值时，测试实际依赖的是服务端计时而不是被测行为。修复：把阻塞时间提高到
   400ms、下界放宽到一半（200ms）——这仍然能抓住"BLOCK 未生效"（那种情况约 1ms 返回），但不再依赖
   精确的服务端时序。

## 未包含范围（与第 1 轮一致）

- **消费结果落库仍是内存实现**：`ConsumptionStore` 契约已按本轮要求重写（含租约与三态），但 PostgreSQL
  实现属 A 线。**当前进程重启后消费记录丢失**。启动日志仍明确告警。
- **设备命令派发与域应用不存在**：因此本进程按第 1 项拒绝启动。
- **`/readyz` 接线**（A 线 `httpapi`）。
- **CI 仍未覆盖 `backend/`**：建议尽快补齐，否则以上验证不会在 PR 上自动复现。

## 风险和回滚

- 已知风险：
  1. 第 1 项的后果是 `cmd/worker` 目前不可运行——这是刻意选择，但意味着**本模块在 A 线补齐前无法端到端
     验证**。可靠性语义由单元测试与真实 Redis 集成测试覆盖（含 Worker 对真实流的消费与 ACK）。
  2. 租约默认 5 分钟（`DefaultConsumptionLease`）。若某个 handler 合法耗时超过它，会与接管逻辑冲突。
     A 线实现 PG 存储时必须让租约长于最长 handler。
  3. 死信幂等依赖 Redis 守卫；守卫 FailOpen 时（Redis 故障）仍可能写入重复死信。重复死信可见且可人工
     处理，优于丢失，已在代码注释说明。
  4. `deadLetterGuard` 未接线时（例如自定义调用方）死信写入不再幂等——`SetDeadLetterGuard` 的注释明确
     写明了这一后果；`cmd/worker` 已接线。
- 回滚方式：`git revert <本轮修复提交>`。未引入依赖、未改 `go.mod`/`go.sum`、未改 OpenAPI、未改迁移 SQL。
- 是否影响旧 C++ 系统：否。

## 提交记录

- 第 1 轮（被审查）：`e2fe6f6` + 材料 `7a1e33e`
- 第 2 轮修复：`11e3377`（完整：`11e33778bec9c90275ceb756fea352e632e718dc`）
- 第 3 轮修复：`2ec5390` + 材料 `d6b257a`
- 第 4 轮修复（响应第 3 轮审查）：`f2e24e6` + 材料 `22fd8f6`
- 第 5 轮修复（响应第 4 轮审查）：`4a61fe7` + 材料 `ad4c524`
- 第 6 轮修复（响应第 5 轮审查）：`a7f6cd2` + 材料见本文件所在提交
- 回滚：`git revert a7f6cd2`（第 6 轮）、`git revert 4a61fe7`（第 5 轮）、`git revert f2e24e6`（第 4 轮）、
  `git revert 2ec5390`、`git revert 11e3377`

## 审批结论

```text
状态：PENDING
审批人：
审批时间：
修改要求：
```

---

# 附录：第 1 轮提交内容（保留，便于对照）

## 契约第 5 节的交付映射

| 契约要求 | 交付 | 说明 |
|---|---|---|
| 开始充电事件 | `worker.ChargeHandler` + `ChargeEventApplier` 契约 | 订单域属 A 线，故为契约 |
| 停止充电事件 | 同上（`DefaultChargeEventTypes()` 覆盖 6 类生命周期事件） | |
| 设备重启命令 | `worker.CommandRequestHandler` + `CommandDispatcher` 契约 | 设备协议属网关 |
| 重试和死信 | `worker.Permanent`、`ErrDuplicate`、`MaxAttempts` + Pending 恢复 | |
| 消费结果落库 | `event.ConsumptionStore` 契约 + 内存实现 | PG 实现属 A 线 |
| 重复事件保护 | `redis.Guard`（`ncs:idempotency`）+ 消费记录双层 | |

## 第 1 轮的范围说明

真实 Redis Streams 适配器被纳入本模块（`streams_client.go`），因为重试、Pending 恢复与死信都是 Streams
行为，无法用内存替身验证——这正是 BE-B-01 首轮被退回的原因。若认为该归 BE-B-02，可拆为独立提交。
