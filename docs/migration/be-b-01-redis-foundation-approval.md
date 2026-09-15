# 迁移模块审批单：BE-B-01 剩余范围（Redis 基础适配）

第 4 次提交，响应第 3 轮复审的 P1 连接复用竞态。第 1、2 轮处置记录保留在文末。

本文件按 `docs/migration/module-approval-template.md` 填写。审批人确认后，请将结论补登到
`docs/migration/approval-log.md`（该文件由集成人员维护，本模块未修改）。

## 基本信息

- 模块编号：BE-B-01（逻辑编号）
- 模块名称：Redis 基础适配（连接池、真实客户端、健康检查、缓存、会话、限流、分布式锁、故障降级）
- 开发线：B
- 开发分支：`codex/migration/backend-events/b-01-redis-foundation`
- 基线提交：`ce25734`（`codex/migration-integration`）
- 目标集成分支：`codex/migration-integration`
- 提交哈希：
  - **`ddf5ea9`** `fix(backend-events): wait for the cancellation watcher before releasing`（本轮）
  - `4262690` `fix(backend-events): honour context, classify transport faults, fix pool close`
  - `ea704d6` `feat(backend-events): add real Redis foundation with pooled client`
  - `0d2878e` / `507d845` 审批材料

## 第 3 轮 P1 竞态的处置

### 根因确认

`roundTrip` 启动的取消监听协程与主流程之间存在两个缺陷，均已确认并修复：

1. **返回时未等待监听协程退出。** 原实现只在 defer 中 `close(stopWatcher)`，不等待协程结束。
   若 Redis 已成功返回、同时 context 被取消，协程可能仍在运行；调用方随即
   `pool.Release` 把连接放回池，协程这才执行 `SetDeadline(time.Now())`，把**已被下一个请求取走的
   连接**强制设为立即超时。

2. **取消可能被覆盖。** 协程在 `SetWriteDeadline`／`SetReadDeadline` 之前设置了立即 deadline，
   紧随其后的这两次调用会把它覆盖成正常超时，导致取消直到配置的 `ReadTimeout` 才生效。

### 修复方式

- 引入 `watcherDone`，`roundTrip` 的 defer 改为**等待**协程完全退出：

```go
defer func() {
    close(stopWatcher)
    <-watcherDone
}()
```

  由于函数返回即是 `Release` 的前置条件，这保证了「连接回池时不再有任何协程可能触碰它」。
  `SetDeadline` 只是本地定时器操作，不会阻塞，所以这个等待是有界的。

- 在 `SetWriteDeadline` 与 `SetReadDeadline` **之后各重新检查一次 `ctx.Err()`**，若已取消则立即
  返回 context 错误，从而封住「立即 deadline 被覆盖」的窗口。

- 额外加固：`Release` 在连接回到空闲列表前调用 `SetDeadline(time.Time{})` 清除 deadline。
  `roundTrip` 本就在每次 I/O 前重设 deadline，故这是纵深防御；它把
  「空闲连接不带 deadline」这一池不变量显式化，使后续代码路径不可能继承到过期 deadline。

### 确定性测试

竞态本身是概率性的，因此新测试用 `gatedDeadlineConn` 消除运气成分：该包装只**拦截**取消监听协程
安装的立即 deadline（`Release` 的零值 deadline 与 `roundTrip` 的读写 deadline 直接透传），
因此测试能确切知道协程已进入取消分支并停在那里，从而观察 `roundTrip` 是否等待它。

另一处关键在于**投递顺序**：`net.Pipe` 的 `Write` 仅在对端读取后才返回，因此测试先让服务端写入应答、
等到「客户端已把应答读入缓冲区」的信号后，才释放监听协程。这样应答一定解析成功，不存在「谁先赢」的歧义。

| 测试 | 断言 |
|---|---|
| `TestRoundTripWaitsForTheCancellationWatcherBeforeReturning` | 协程停住期间 `roundTrip` **不得返回**；协程完成计数在返回时为 0；释放协程后返回成功且完成计数为 1（即返回前协程已退出） |
| `TestReusedConnectionIsNotPoisonedByAStaleWatcherDeadline` | 第一次请求结束后，**同一个连接**上立刻发起第二次请求，必须成功、不得超时 |
| `TestPoolReuseAfterAConcurrentCancellationDoesNotTimeout` | 通过真实 `Client`+连接池端到端验证：第一个请求被取消后，第二个请求必须立即成功 |
| `TestRoundTripDoesNotLoseACancellationAcrossADeadlineCall` | 在设置 deadline 的窗口内反复取消，调用必须立即结束，而不是等满 30s `ReadTimeout` |

### 反向验证（确认测试真的能抓到这个缺陷）

临时移除 defer 中的等待后运行：

```text
--- FAIL: TestRoundTripWaitsForTheCancellationWatcherBeforeReturning (0.05s)
    watcher_race_test.go:190: roundTrip returned while the cancellation watcher was still running: {value:PONG err:<nil>}
```

即 `roundTrip` 在监听协程仍在运行时以成功返回 —— 正是复审描述的竞态。恢复修复后该测试通过。
（临时改动已完全还原，未留在任何提交中。）

## 验证记录

在 `charging-station-platform-backend-events/backend` 下执行，Redis 版本 `7.0.15`：

```text
命令：gofmt -l .
结果：无输出

命令：go vet ./...
结果：通过，无告警

命令：NCS_REDIS_TEST_ADDR=127.0.0.1:6379 go test -race ./...
结果：全部 ok，repository/redis 3.049s

命令：NCS_REDIS_TEST_ADDR=127.0.0.1:6379 go test -count=20 -race ./internal/repository/redis/
结果：ok 42.569s；执行后 db15 dbsize = 0（无残留键）

命令：NCS_REDIS_TEST_ADDR=127.0.0.1:6379 go test -cover ./internal/repository/redis/
结果：coverage: 87.7% of statements

命令：git status --porcelain | grep -E "go\.mod|go\.sum|api/|internal/httpapi"
结果：无匹配（未触碰共享锁文件与 A 线目录）
```

- 测试函数：217 个（其中 13 个真实 Redis 集成测试）
- 覆盖率：87.7%

## 未包含范围（与前几轮一致）

- **真实 `StreamClient` 适配器**（属 BE-B-02）。当前 worker 仍不能消费真实 Stream，启动日志明确告警。
- **`/readyz` 接线**（属 A 线 `httpapi`）。
- **PostgreSQL 兜底约束**（属 A 线 BE-A-04）。本模块只在 `lock.go` 中把该约束写成明确要求。
- **连接池无后台探活**：空闲连接被中间设备静默切断时，会按第 2 轮修复的路径报 `ErrUnavailable`
  并降级；彻底消除需 PING 探活（增往返）或重试（会重复非幂等命令）。
- **CI 仍未覆盖 `backend/`**：建议尽快补齐，否则以上验证不会在 PR 上自动复现。

## 风险和回滚

- 回滚方式：`git revert ddf5ea9`（本轮）以及前几轮提交。未引入依赖、未改
  `go.mod`/`go.sum`、未改 OpenAPI、未改迁移 SQL、未新增可执行目标。
- 已知限制：固定窗口限流边界处最多两倍限额（代码注释已说明）；`Degradation.LastError` 保存截断错误
  文本（256 字节），`ConnConfig` 的 `String`/`GoString`/`slog.LogValue` 强制脱敏口令。
- 是否影响旧 C++ 系统：否。

## 第 1 轮阻塞项处置（保留记录）

1. **真实客户端与连接池** —— 实测 `proxy.golang.org:443` 不可达、模块缓存无 Redis 客户端，go-redis
   不可行；改用标准库实现 RESP2 客户端（`resp.go`/`connpool.go`/`client.go`/`scripts.go`），
   不触碰共享锁文件。
2. **限流 INCR/EXPIRE 分离** —— 新增 `IncrWithWindow`，真实实现为单条 `EVAL`，含 `ttl < 0` 自愈分支；
   回归测试断言限流器对 `Incr`/`Expire` 的调用次数为 0。
3. **会话绝对到期** —— `IdleTTL` + `AbsoluteTTL`，绝对到期固化在存储信封；`Refresh` 取
   `min(IdleTTL, 剩余绝对时间)`，永不超过上限，过期即删除。
4. **Policy 运行时接线** —— `SettingsFromEnv` + `NewCapabilities`，`cmd/worker` 已接线并支持
   `NCS_REDIS_REQUIRED` 拒绝启动。
5. **提交哈希** —— `ea704d6`。
6. **锁非唯一保障** —— `lock.go` 写明 Redis 锁是优化而非正确性保障，最终由 PostgreSQL 唯一约束、
   幂等记录、事务状态迁移兜底；`FailOpen` 仅可用于业务层显式检测 `Degraded && !Acquired` 并启用 PG
   兜底之后。

## 第 2 轮 5 项修复处置（保留记录）

1. **context 取消** —— 派发前检查 `ctx.Err()`，并加监听协程强制立即 deadline。**取舍**：最初把
   socket deadline 夹紧到 context deadline，导致两个定时器竞争、结果不确定（`-race` 下间歇失败），
   改为只依赖监听协程，确定性成立。
2. **传输错误包装** —— 确认是**真实功能缺陷**：复用被服务端关闭的连接时错误不带 `ErrUnavailable`，
   跳过降级策略并把裸 socket 错误抛给业务层，**FailOpen 恰在该生效时失效**。四条失败路径统一经
   `wrapUnavailable`；并修正 `isServerSideError` 用 `errors.As` 而非类型断言（原实现会把健康连接
   误判为不可复用并丢弃）。
3. **连接池关闭** —— 容量 1 的通知 channel 只能唤醒一个等待者、其余永久阻塞；关闭期间拨号的连接会被
   交出。改为广播语义 + 拨号返回后复检 `closed`。
4. **密码被 TrimSpace** —— 用户名与密码改为原样读取，地址/数字/时长仍 trim。
5. **过期注释** —— 清理 `Opener`、`ConnConfig`、`Limiter` 中已失效的表述，并 grep 确认无残留。

## 审批结论

```text
状态：PENDING
审批人：
审批时间：
修改要求：
```
