# B-07 钱包持久化收口（PostgreSQL 适配器）评审记录

## 基本信息

- 模块编号：B-07
- 模块名称：钱包与账本持久化（PostgreSQL 适配器收口）
- 开发线：B（基础设施/集成）
- 开发分支：`codex/backend/b-07-wallet-persistence`
- 基线提交：`c54c303`（A-04 `codex/backend/a-04-wallet-payment`）
- 目标集成分支：`develop`（A-04 合入后 `git rebase --onto origin/develop c54c303`）

## 结论摘要

A-04 交付的钱包适配器有两个会在生产里赔钱的缺陷，本次一并修掉并用测试锁住：

1. **P0 并发首次入账丢钱**：钱包行不存在时 `SELECT ... FOR UPDATE` 锁不住任何行，多条事务各自读到 0，再以"绝对值"写回余额，互相覆盖；账本却把每一次入账都记了下来。8 并发首充 100 分的实测结果是 `balance=200`、`ledger_sum=800`、`errors=0`——静默少钱。
2. **P1 幂等窗口之外的重试报 500**：`idempotency_records` 是 24 小时的响应缓存，缓存过期或清理后重试同一个 `Idempotency-Key`，代码会再记一次账本，撞上 `wallet_transactions_idempotency_key_key` 唯一索引，返回 SQLSTATE 23505。已经成功的请求不该因为缓存到期而变成失败。

另有三项收口：`sql.ErrNoRows` 映射为"幂等处理中"而不是"订单不可退款"、退款订单不存在与"无可退款金额"区分开、退款幂等键语义定稿（见下）。

**不新增任何表、不新增迁移（结论：0009 不需要，见"迁移结论"）、不改 `internal/wallet`、不改 `api/openapi.yaml`。**

**待决策一项**：退款目标订单不存在时，A 线把错误映射成 HTTP 状态码的那两行改动归属 A 线文件，需要审批人裁定（见文末"待决策"）。B 线侧已经能区分这个错误，接口层目前会落到 503/code 3。

## 变更范围

- 修改文件：
  - `backend/internal/repository/postgres/wallet.go`（适配器修复）
  - `backend/internal/repository/postgres/integration_test.go`（测试基建：跨进程测试库互斥锁）
- 新增文件：
  - `backend/internal/repository/postgres/wallet_b07_integration_test.go`（B-07 回归矩阵）
  - `backend/internal/repository/postgres/wallet_b07_schema_contract_test.go`（模式契约测试，"不需要 0009"的可执行结论）
- 明确未修改的另一条线目录：
  - `backend/internal/wallet/`（A-04 的端口与 HTTP 层，一行未动）
  - `backend/api/openapi.yaml`
  - `backend/migrations/`（无新增、无修改；`0008` 属 A-01，未出现在本分支）
  - `backend/internal/repository/postgres/` 之外的一切

## 契约和数据

- 新增或修改 API：无（本模块不改接口；退款 404 的映射见"待决策"）
- PostgreSQL 迁移：无。适配器用到的列/索引/约束全部已存在于 `0001`/`0004` 交付的模式中，由 `wallet_b07_schema_contract_test.go` 逐条断言
- Redis Key/Stream：无
- 幂等和并发策略：
  - 钱包行锁：`INSERT ... ON CONFLICT (user_id) DO UPDATE SET user_id = EXCLUDED.user_id RETURNING balance_cents` 一条语句既建行又加锁；行不存在时 `FOR UPDATE` 锁不住的问题被彻底消除
  - 余额算术：`balance_cents = balance_cents + $2 ... RETURNING`，加减在数据库内完成，`RETURNING` 的 after 值直接进账本
  - 响应缓存与账本的事实分工：PostgreSQL 账本 `wallet_transactions.idempotency_key` 是**持久事实**，`idempotency_records` 只是**24 小时响应缓存**；重试先查账本，缓存没了也能答出首次结果
  - 账本键：充值 `topup:<uid>:<key>`；退款 `refund:<orderNo>:<key>`（见"退款幂等键语义"）
  - 用户不存在：外键 23503 → `wallet.ErrWalletNotFound`（404），而不是 500

## 缺陷详情与复现

### P0 并发首次入账丢钱

修复前代码（本分支回退验证时重新构造，见"反验证记录" R1）：

```go
// 行不存在时 FOR UPDATE 锁不住任何东西
err := tx.QueryRowContext(ctx, `SELECT balance_cents FROM wallet_accounts WHERE user_id = $1 FOR UPDATE`, userID).Scan(&balance)
if errors.Is(err, sql.ErrNoRows) { /* 插入 0 行，返回 0 */ }
...
// 再用 Go 里算出的绝对值写回
after := balance + amount
tx.ExecContext(ctx, `UPDATE wallet_accounts SET balance_cents = $2 WHERE user_id = $1`, userID, after)
```

原始实测（8 并发、每笔 100 分、全新用户）：

```text
balance = 200, ledger_sum = 800, ledger_rows = 8, errors = 0
```

即：钱少了 600 分，账本却记了 800 分，而且**没有任何错误**——最坏的一类缺陷。

修复后的实测：

```text
--- PASS: TestB07TopUpConcurrentFirstCreditKeepsEveryCent (0.10s)
```

### P1 幂等窗口之外的重试报 500

原始实测：首次充值成功 → 删除/过期 `idempotency_records` 记录 → 同 key 重试：

```text
ERROR: duplicate key value violates unique constraint "wallet_transactions_idempotency_key_key" (SQLSTATE 23505)
```

修复后：重试从账本读出首次的 `balance_after_cents`，把它写回响应缓存，不二次入账，返回首次结果。

### 必改项逐条状态

| # | 要求 | 状态 | 证据 |
| - | ---- | ---- | ---- |
| 1 | P0：并发首次充值/退款不得丢余额 | 已修 | `TestB07TopUpConcurrentFirstCreditKeepsEveryCent`；R1 反验证 |
| 2 | P1：幂等窗口之后的重放不得 500 | 已修 | `TestB07TopUpReplayAfterCacheExpired`、`TestB07TopUpReplayAfterCachePurged`、`TestB07RefundReplayAfterCachePurged`；R2 反验证 |
| 3 | `sql.ErrNoRows` 映射 | 已改 | 幂等抢占的 `SELECT` 落空 → `wallet.ErrIdempotencyInProgress`（409），不再是"订单不可退款"；**该分支无法确定性触发，见"未验证"** |
| 4 | 退款目标订单不存在 → 404 | **部分**：适配器已区分（`errOrderNotFound`），HTTP 映射待 A 线决策 | `TestB07RefundMissingOrderIsNotRefundable`；R5 反验证 |
| 5 | 退款不同幂等键语义定稿（文档+测试） | 已定稿 | 本文"退款幂等键语义" + `TestB07RefundDifferentKeyAfterRefundIsRejected`；R3 反验证 |

## 退款幂等键语义（定稿）

账本键是**退款请求**的身份，不是订单的身份：

```text
refund:<orderNo>:<Idempotency-Key>
```

- 同一个请求（同订单 + 同 key）重试：无论响应缓存是否还在，都返回**已经发生的那次退款**，不再入账、不再写审计。
- 不同请求（同订单 + 新 key）：走到订单自身状态判断，已被退款的订单返回 `ErrOrderNotRefundable`（409/code 18），**不会**伪装成第二次退款成功。

只用订单号做键会让"第二个新 key 的退款请求"被账本命中而返回成功——对运营账面来说是错误的成功。A-04 的 `TestRefundOrderReversesPayment` 仍然断言"新 key 的第二次退款 → `ErrOrderNotRefundable`"，两者一致。

## 测试矩阵（用户清单 12 项 → 实际测试）

| # | 清单项 | 测试 |
| - | ------ | ---- |
| 1 | 并发首次充值 | `TestB07TopUpConcurrentFirstCreditKeepsEveryCent` |
| 2 | 同 key 并发充值 | `TestB07TopUpConcurrentSameKeyCreditsOnce` |
| 3 | 窗口外重放 | `TestB07TopUpReplayAfterCacheExpired`、`TestB07TopUpReplayAfterCachePurged` |
| 4 | 退款幂等 | `TestB07RefundReplayAfterCachePurged`、`TestB07RefundDifferentKeyAfterRefundIsRejected` |
| 5 | 不可退款订单 | `TestB07RefundUnsettledOrderIsNotRefundable`、`TestB07RefundDifferentKeyAfterRefundIsRejected` |
| 6 | 订单不存在 | `TestB07RefundMissingOrderIsNotRefundable` |
| 7 | 账本余额链 | `TestB07LedgerBalanceChainMatchesWallet` |
| 8 | 余额非负 | `TestB07LedgerBalanceNeverNegative` |
| 9 | 分页与类型过滤 | `TestB07LedgerPaginationAndTypeFilter` |
| 10 | 跨运行唯一键 | 所有新测试的 key/手机号/订单号都带 `uniqueSuffix(t)` 派生后缀；并发用例逐个 key 唯一 |
| 11 | 反验证 | 见"反验证记录" |
| 12 | PG 模式契约测试 | `TestB07WalletSchemaContract`、`TestB07SchemaContractDetectsMissingFacts` |
| 附 | 用户不存在 → 404 | `TestB07UnknownUserIsWalletNotFound` |
| 附 | 幂等作用域按用户隔离 | `TestB07IdempotencyScopeIsPerUser` |

## 反验证记录

每个新测试都必须在"被保护的行为被移除"时失败。移除动作做完立即还原，并在还原后核对 SHA-256。

| # | 移除的行为 | 执行结果（原始输出摘要） |
| - | ---------- | ------------------------ |
| R1 | 加锁 upsert + 数据库内相对加法 → 退回"SELECT FOR UPDATE + 绝对值写回" | `wallet_b07_integration_test.go:124: balance = 200, want 800 (lost credit)` → FAIL（`TestB07LedgerBalanceChainMatchesWallet` 仍 PASS：单线程用例本就不覆盖该缺陷） |
| R2 | `ledgerResult` 账本优先查 → 恒返回"未入账" | 充值：`ERROR: duplicate key value violates unique constraint "wallet_transactions_idempotency_key_key" (SQLSTATE 23505)` → FAIL；退款：`RefundOrder() after the cache was purged error = wallet: order has no settled amount to refund, want the first result` → FAIL |
| R3 | 退款账本键 `refund:<orderNo>:<key>` → `refund:<orderNo>` | `second refund with a new key = <nil>, want ErrOrderNotRefundable` → FAIL（新 key 的退款被静默判为成功，正是键格式要堵的洞）；同 key 重放用例仍 PASS |
| R4 | 订单状态校验（`COMPLETED` 且 `paid_cents > 0`） | `refund of an unsettled order = ERROR: ... violates check constraint "wallet_transactions_amount_cents_check" (SQLSTATE 23514), want ErrOrderNotRefundable` → FAIL；A-04 的 `TestRefundOrderReversesPayment` 同时 FAIL（说明该守卫是共享行为） |
| R5 | 退款订单不存在的区分（`errOrderNotFound`） | `missing order error = sql: no rows in result set, want errOrderNotFound` → FAIL |
| R6 | 外键 23503 → `ErrWalletNotFound` | `TopUp() for an unknown user = ERROR: ... violates foreign key constraint "wallet_accounts_user_id_fkey" (SQLSTATE 23503), want ErrWalletNotFound` → FAIL |
| R7 | 模式契约的可检出性 | 由常驻测试 `TestB07SchemaContractDetectsMissingFacts` 承担：在回滚的事务里删掉唯一索引、CHECK 约束、账本幂等列，契约检查必须逐条报出（PASS 即为"能检出"） |
| R8 | 跨进程测试库互斥锁 | 无锁：同形状的两进程并发 → `Run() migrations error = load applied migrations: ERROR: relation "schema_migrations" does not exist (SQLSTATE 42P01)`，另一个进程 `relation "user_accounts" does not exist`；加锁后同形状两进程 → 双双 `ok` |

## 迁移结论：不需要 0009

`wallet_b07_schema_contract_test.go` 把适配器依赖的数据库事实逐条写成断言，并在本地测试库上跑通（`schema_migrations` 最高版本 7）：

- `wallet_accounts(user_id)` 唯一索引（`ON CONFLICT` 目标；该表**没有**代理主键 `id`，主键即 `user_id`）
- `wallet_accounts.balance_cents` 的 CHECK（非负）
- `wallet_accounts.version` 列（乐观计数）
- `wallet_accounts.user_id → user_accounts(id)` 外键（23503 → 404 的依据）
- `wallet_transactions.idempotency_key` 唯一索引（窗口外重放的安全网）
- `wallet_transactions` 全部读写列、`transaction_type` CHECK 接受 `TOP_UP`/`REFUND`
- `idempotency_records(scope, idempotency_key)` 唯一（键集必须完全一致，多一列就不满足 `ON CONFLICT`）
- `idempotency_records`、`operation_logs`、`charging_orders` 的读写列

结论：钱包模块**不产生任何新的模式需求**，`0009` 不需要新增。该结论是可执行的测试，不是对 `0001_init.sql` 的目视检查。

## 测试基建发现（本模块发现的、不属于钱包逻辑的问题）

1. **同一个测试库被两个测试进程同时使用时会互相打穿**（已在本分支修好并验证）。包内 `TestRunSerializesConcurrentRuns` 与 `TestRunWorksWithSingleConnectionPool` 会 `DROP TABLE ... CASCADE` 重建全部表，另一个进程的任何测试都会看到半重构的模式。修法：`integrationDB` 在测试期间持有会话语义咨询锁 `pg_advisory_lock(0x4E43535F54455354)`，测试结束随连接释放（R8 有正反两侧证据）。选择咨询锁而不是"约定不要并发跑"，是因为约定无法被执行。
2. **残留风险（未修，需集成负责人裁定）**：`cmd/worker` 的重复投递测试也用 `NCS_TEST_PG_DSN` 但不取这把锁，而 `go test ./...` 会并行跑不同包。也就是说 `go test ./...` 仍可能撞上第 1 条的窗口。该测试只读写、不 DROP。彻底修法是让破坏性用例使用一次性数据库/独立 schema（跨包、跨模块），或让 `cmd/worker` 也取同一把锁（需要把锁助手导出或复制，属跨模块改动）。**在修好之前，涉及 PG 的包建议 `-p 1` 串行。**
3. **`TestAdminUserAndOrderLists` 的跨运行脆弱性**（本分支存在，**未在此修**）：`admin_integration_test.go:151` 用 `seeded.OrderNo[3:17]`（14 位时间戳前缀）过滤，断言 `len == 1`。同一秒内产生的任何其它订单都会让它失败。两进程并发实测：

   ```text
   --- FAIL: TestAdminUserAndOrderLists
       admin_integration_test.go:156: orders = ...(2 条订单，含 UserID:91 的 ORD20260915102123c59f6f78)
   ```

   该脆弱性已由 A-01 收口修复（改为按完整订单号过滤 + 单独的前缀断言），B-07 不重复修，避免两条线改同一处造成无谓冲突；A-01 落地后此问题消失。
4. **`ErrNoRows` 分支无法确定性触发**：幂等抢占里"`ON CONFLICT DO NOTHING` 返回 0 行之后 `SELECT` 又查不到"需要另一个事务在那两条语句之间删除并提交该行。按用户要求保留该分支并映射为 409，但没有为它写测试（写了也只是假绿）。这一点如实登记，不用"已测"含混过去。

## 未验证 / 未覆盖

- 幂等抢占的 `sql.ErrNoRows` 分支没有确定性测试（原因见上）。
- HTTP 层的退款 404 没有端到端断言：需要 A 线两行改动（见"待决策"）。
- 单线程用例（账本链、分页、作用域隔离、不可退款）在回退修复后仍然 PASS，属预期：它们保护的是语义不变式，不是并发缺陷本身；因此它们**不**声称覆盖 P0/P1。
- 未做压测级并发（8 路）；更大队列下的表现未测。
- 未验证旧 C++ 系统侧行为（本模块不涉及）。

## 验证命令与原始输出

```text
$ gofmt -l ./cmd ./internal
（无输出）

$ go build ./...
（成功）

$ go vet ./...
（无输出）

$ NCS_TEST_PG_DSN=postgres://ncs_test@127.0.0.1:55439/ncs_a03?sslmode=disable NCS_REDIS_TEST_DB=14 \
  go test -count=1 -race -v -run TestB07 ./internal/repository/postgres
--- PASS: TestB07TopUpConcurrentFirstCreditKeepsEveryCent (0.10s)
--- PASS: TestB07TopUpConcurrentSameKeyCreditsOnce (0.05s)
--- PASS: TestB07TopUpReplayAfterCacheExpired (0.08s)
--- PASS: TestB07TopUpReplayAfterCachePurged (0.08s)
--- PASS: TestB07RefundReplayAfterCachePurged (0.17s)
--- PASS: TestB07RefundDifferentKeyAfterRefundIsRejected (0.13s)
--- PASS: TestB07RefundMissingOrderIsNotRefundable (0.05s)
--- PASS: TestB07RefundUnsettledOrderIsNotRefundable (0.08s)
--- PASS: TestB07UnknownUserIsWalletNotFound (0.03s)
--- PASS: TestB07LedgerBalanceChainMatchesWallet (0.16s)
--- PASS: TestB07LedgerBalanceNeverNegative (0.06s)
--- PASS: TestB07LedgerPaginationAndTypeFilter (0.10s)
--- PASS: TestB07IdempotencyScopeIsPerUser (0.04s)
--- PASS: TestB07WalletSchemaContract (0.05s)
--- PASS: TestB07SchemaContractDetectsMissingFacts (0.08s)
ok  github.com/heguangV/charging-station-platform/backend/internal/repository/postgres  2.867s
```

```text
$ NCS_TEST_PG_DSN=postgres://ncs_test@127.0.0.1:55439/ncs_a03?sslmode=disable \
  NCS_REDIS_TEST_ADDR=127.0.0.1:6379 NCS_REDIS_TEST_DB=14 go test -count=1 -race ./...
?   .../cmd/api                [no test files]
?   .../migrations             [no test files]
ok  .../cmd/mock-gateway       1.955s
ok  .../cmd/outbox-publisher   2.004s
ok  .../cmd/worker             2.359s
ok  .../internal/admin         1.977s
ok  .../internal/auth          4.885s
ok  .../internal/config        1.023s
ok  .../internal/event         1.027s
ok  .../internal/httpapi       1.023s
ok  .../internal/observability 1.165s
ok  .../internal/order         1.998s
ok  .../internal/repository/postgres 8.350s
ok  .../internal/repository/redis    6.074s
ok  .../internal/station       1.745s
ok  .../internal/wallet        1.752s
ok  .../internal/worker        2.096s
```

`internal/wallet` 在本分支上全绿，说明 B 线的适配器改动没有破坏 A-04 的端口契约。

## 风险和回滚

- 已知风险：
  - 退款账本键语义是本次定稿的，若运营侧已有依赖"订单号即键"的对外脚本，需要同步（当前无）。
  - 首次入账路径改成 upsert 后，为不存在的用户建钱包会先撞外键再回退成 404；语义正确，但错误路径多一次往返。
  - 第 2 条测试基建残留风险（`go test ./...` 的 PG 包并行）。
- 回滚方式：`git revert` 本模块提交即可；无迁移、无数据改写，回滚不涉及数据修复。已发生的账本与钱包余额不受回滚影响（修复只影响新写入的算术方式）。
- 是否影响旧 C++ 系统：否。

## 待决策（需审批人裁定）

退款目标订单不存在时要求 404，但把错误映射成状态码的 `writeWalletError` 在 `internal/wallet/http.go`（A-04 的 A 线文件），本模块遵守"不改 `internal/wallet`"的边界，因此没有动它。

- 现状：B 线已能区分（`errOrderNotFound`），但接口层没有对应 case，会落到 `default` → 503/code 3。**这不是 404。**
- 需要的 A 线改动（两行）：`internal/wallet` 增加 `ErrOrderNotFound` 哨兵 + `writeWalletError` 增加一个 `case → 404 / code 4`（或新代码）。B 线把 `errOrderNotFound` 换成该哨兵即可，无需其它改动。
- 备选：接受 409/code 18（与"无可退款金额"同码），放弃 404 语义；不建议，因为调用方无法据此区分"订单不存在"与"订单已退过款"。

请裁定由谁改这两行；裁定后本模块可在半小时内补上端到端断言。

## 审批结论

```text
状态：PENDING
审批人：
审批时间：
修改要求：
```
