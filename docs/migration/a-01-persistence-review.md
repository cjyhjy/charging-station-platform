# A-01 持久化收口（B 线）

> 本轮是 **A-01 的数据库收口**，不是新的 B-07 钱包模块。理由：0008 服务于 A-01 的用户资料、冻结与注销；
> `AccountMutationAdapter` 是 A-01 的 B 线交接物；B-07 的原定义是钱包/账务数据支撑，不能拿来覆盖 A-01 的
> 数据库收口，而 A-04 的钱包表与 `WalletStore` 已经存在，B-07 也不应重复实现。
> 收口完成并登记 OpenAPI 后，A-01 记为 APPROVED；B-07 另行按钱包/账务的真实缺口评估。

范围：A-01「认证和用户账户」的四类新接口（`GET/PUT /me/profile`、`DELETE /me`、`POST /admin/users/{id}/freeze|unfreeze`）
所依赖的 PostgreSQL 侧工作。A 线交付端口（`auth.AccountMutation`）、服务与处理器；**PostgreSQL 适配器属于 B 线**，
正式迁移由 B 线出。本文件记录复核结论、修复项、未修复项（属 A 线或需决策）与验证证据。

- 分支：`codex/backend/a-01-persistence`（**基于 A-01 分支 `codex/backend/a-01-auth` 叠加**）
- A-01 提交：`ac4e407 feat(auth): add user profile, deletion and freeze (BE-A-01)`
- 正式迁移：`backend/migrations/0008_user_profile_and_deletion.sql`（+ `migrations/down/0008_...down.sql`）
- 集成测试：`backend/internal/repository/postgres/account_mutation_integration_test.go`
- 迁移形状测试：`schema_test.go` 的 `TestUserProfileMigrationIsComplete`

合并说明：本分支与 A-01 是**叠加关系**（A-01 先合入 develop 后，本分支用
`git rebase --onto origin/develop ac4e407 codex/backend/a-01-persistence` 即可落到 develop 顶端）。
在此之前本分支不能单独合并；A-01 的 OpenAPI 登记仍待集成人员执行
（提案：`docs/migration/proposals/a-01-openapi-profile-endpoints.md`）。

## 0. 收口结论（第一步：迁移结论核对）

```text
问题 1：0008 是否覆盖 avatar_url、deleted_at？
  是。两列由 0008 添加，且实测存在于测试库：
  avatar_url TEXT NOT NULL DEFAULT ''、deleted_at TIMESTAMPTZ（可空，NULL = 未注销）。

问题 2：是否与既有 user_accounts 结构冲突？
  否。逐条核对既有结构，全部保留且与注销方案相容：
    phone TEXT UNIQUE（可空）+ CHECK (phone IS NOT NULL OR email IS NOT NULL)
        —— 匿名化写成 deleted-{id}@invalid，仍满足 phone 非空与唯一；
    password_hash TEXT NOT NULL —— 注销写空串，满足非空，并由 0008 的约束保证"已注销 ⇒ 已清空"；
    display_name TEXT NOT NULL DEFAULT '' —— 注销写 '已注销用户'；
    status CHECK IN ('ACTIVE','DISABLED') —— 注销与冻结都写 DISABLED，枚举无需扩展；
    另：A-01 代码还用到 email、created_at、updated_at、wallet_accounts 与 admin_accounts，
       这些列/表在 0001 中均已存在（钱包开户由 A-01 的 EnsureUserWithWallet 完成）。

问题 3：是否需要新增字段或索引？
  字段：不需要。A-01 触及的列 = {phone, email, display_name, password_hash, status, created_at,
        updated_at} ∪ {avatar_url, deleted_at}，前一集合来自 0001，后一集合来自 0008。
  索引：需要的那一个已在 0008 里 —— idx_user_accounts_deleted_at (deleted_at) WHERE deleted_at IS NOT NULL。
       读路径全部按 deleted_at IS NULL 过滤，部分索引只在真的存在注销数据时付出代价。

问题 4：是否真的需要 0009？
  不需要。上面四条已由 TestA01SchemaSatisfiesTheAdapterAndTheContract（真实 PostgreSQL）逐条断言：
  列（含类型/可空性/默认值）、0008 的三条约束、既有 phone/email 唯一性与 status 枚举、部分索引、
  以及 wallet_accounts 的必要列；全部通过即说明数据库侧没有缺口。
  该测试还断言最高已应用迁移为 8：一旦将来出现 0009，它会失败并提示结论需要重新评估（而不是默认继续成立）。
```

## 1. 正式迁移 0008

提案（`docs/migration/proposals/a-01-user-profile-migration-0008.sql`）只有两列加一个索引；正式版在提案
之上补了三条约束，理由如下。

```sql
ALTER TABLE user_accounts
    ADD COLUMN IF NOT EXISTS avatar_url TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;

ALTER TABLE user_accounts
    ADD CONSTRAINT user_accounts_deleted_is_disabled
        CHECK (deleted_at IS NULL OR status = 'DISABLED'),
    ADD CONSTRAINT user_accounts_deleted_has_no_credentials
        CHECK (deleted_at IS NULL OR password_hash = ''),
    ADD CONSTRAINT user_accounts_avatar_url_length
        CHECK (char_length(avatar_url) <= 512);

CREATE INDEX IF NOT EXISTS idx_user_accounts_deleted_at
    ON user_accounts (deleted_at) WHERE deleted_at IS NOT NULL;
```

**为什么要多加约束**：注销是"手机号匿名化 + 昵称匿名化 + 清空凭据 + 禁用 + 记录时间"这一组动作的**复合事实**。
适配器用一条 UPDATE 完成全部动作，但表本身不阻止后来任何一条遗漏字段的 UPDATE；漏掉 `status` 会留下一个
"已注销却仍能登录"的账号——这种账号不会报错、不会被看见，直到有人发现它能登录。因此把两条不变量写进数据库：

- 已注销 ⇒ 必须已禁用（`deleted_at IS NULL OR status = 'DISABLED'`）；
- 已注销 ⇒ 凭据必须已清空（`password_hash = ''`）。

第三条是头像地址的**存储上界**：列是 TEXT，契约没有长度限制，没有上界就等于允许任意大小的内容进入用户表。
512 足够任何 CDN 地址；需要更长时用新的递增迁移放宽。

索引保持提案的部分索引形式：读路径全部按 `deleted_at IS NULL` 过滤，未注销行占绝大多数，
部分索引只在真的存在注销数据时才付出代价。

**空库/既有库验证**：迁移由 `integrationDB`（测试）与 `cmd/api` 启动（生产）通过同一个 runner 应用；
测试库 `schema_migrations` 现为 `1..8`，`0008|user_profile_and_deletion.sql`。down 脚本在
`migrations/down/`（不参与嵌入集合），按"先约束后列"的顺序回退，并在文件头写明它会**销毁**头像与注销时间数据、
且无法恢复已匿名化的手机号——这是注销方案的固有性质，不是脚本缺陷。

## 2. SQL 与事务边界复核

### 2.1 已修复（B 线文件：`internal/repository/postgres/accounts.go`）

| # | 问题 | 处理 |
|---|---|---|
| 1 | `UpdateProfile` 在"两个字段都为 nil"时进入 else 分支并解引用 `*update.AvatarURL` → **panic**。HTTP 处理器恰好拒绝了无字段请求，因此这条路径当时不可达——但它是一颗等下一个调用者的地雷（内存实现不 panic，契约也没说"必须至少一个字段"） | 改为显式拒绝：`profile update requires at least one field`，并加测试 |
| 2 | 三个近乎重复的分支语句（昵称+头像 / 仅昵称 / 仅头像），任何一次修改都可能只改到其中一条 | 改为**单条** `UPDATE ... SET display_name = COALESCE($2, display_name), avatar_url = COALESCE($3, avatar_url)`；nil 表示"不改该列"由 COALESCE 表达 |
| 3 | `RETURNING` 列表在四处重复 | 提取 `profileColumns` 常量，避免"某条路径忘了新列" |
| 4 | 适配器把**原始手机号**放进名为 `PhoneMasked` 的字段，遮蔽依赖服务调用 `auth.MaskPhone`；任何未来调用者忘记遮蔽即泄露 PII | 在适配器上写明该契约（"不要把该值未遮蔽地返回给客户端"），并在 §2.3 记为 A 线命名建议（改端口字段名属 A 线） |

### 2.2 事务边界：结论是"单语句即事务"，跨存储边界才是真问题

`auth.AccountMutation` 的文档要求"每个方法必须在实现侧的事务内执行"。本适配器每个方法都是**单条语句**：
在 PostgreSQL 中单条语句本身就是一个事务，整体成功或整体不生效，因此契约由构造满足，而不需要 `BeginTx/Commit`
（那只会多一次往返而不增加原子性）。这一判断写在适配器注释里，而不是留在读者心里。
`DeleteAccount` 的一次性：五个字段在同一条 UPDATE 中改变，配合 0008 的两条不变量，不存在"半注销"状态。

**真正的边界在服务层，而且是跨存储的**：`Service.FreezeUser`/`DeleteAccount` 先改 PostgreSQL（状态/匿名化提交），
再调 `sessions.RevokeAllForUser`（Redis）。两者没有分布式事务，于是有一个明确窗口：

```text
PG 已提交（账号已 DISABLED / 已匿名化）→ Redis 会话撤销失败 → 服务返回错误
结果：账号状态是安全的（不能再登录、不能再开充），但此前签发的会话仍存活，最长到
      idle 30m / absolute 168h；且 auth.Authorize 判的是**会话里的** Status（Redis 缓存值），
      不是数据库里的值。
```

复核到的事实（避免过度断言）：

- **登录**会重新读库并检查状态（`service.go` 中 `user.Status != StatusActive` → 拒绝）→ 冻结后无法再登录 ✅
- **开始充电**会重新读库（订单事务里的 `validateStartEligibility`）→ 冻结用户 `ErrUserFrozen` ✅
  （已由 `TestAccountMutationFreezeBlocksCharging` 断言：冻结后 `StartCharging` 返回 `ErrUserFrozen`，
  订单停在 CREATED；解冻后可正常进入 STARTING）
- **其余已认证接口**（资料、站点/桩查询、订单查询、取消等）在该窗口内仍可用，因为 `Authorize` 只看会话缓存 ✅/⚠️

也就是说：BR-07 的两条硬要求（不能登录、不能开始充电）**不依赖 Redis 撤销成功**；残留风险是"已冻结/已注销的
旧会话还能读到自己的数据一段时间"。这是需要 A 线决策的加固项，不是 B 线适配器能修的问题：

- 方案 A（推荐）：`RequireIdentity` 之后按需重读账号状态，或给会话状态一个短 TTL 的本地缓存；代价是每请求一次查询或缓存。
- 方案 B：把顺序改为"先撤销 Redis，再改 PostgreSQL"——但撤销成功而 PG 失败会让用户莫名被登出且账号仍可用，
  对冻结语义是**更差**的失败模式，因此当前 PG-first 的顺序是合理的。
- 方案 C：状态变更时记一条待撤销任务，由后台重试撤销（最终一致）。复杂度最高，但窗口最小。

### 2.3 复核未修复项（属 A 线或需业务决策）

| # | 发现 | 归属/建议 |
|---|---|---|
| 1 | 端口字段 `ProfileView.PhoneMasked` 实际承载**原始手机号**，遮蔽靠调用方记住 | A 线：改名为 `Phone`，或在适配器内遮蔽（后者会与服务重复遮蔽，`MaskPhone("138****0001")` 会变成 `***`，因此建议改名而非加遮蔽） |
| 2 | `DELETE /me` 在"PG 已注销、Redis 撤销失败"的重试下第二次返回 404（`DeleteAccount` 返回 found=false → `ErrProfileNotFound`），客户端看到"账号不存在"而不是"已注销" | A 线：契约上 404 可辩护，但重试语义建议返回 204（幂等注销）。迁移与适配器无需改动 |
| 3 | `SetFrozen(false)` 无条件写 `ACTIVE`：今天只有"冻结"会写 DISABLED，因此正确；若将来出现第二种禁用原因，这条会把它们一起解禁 | 已写进适配器注释；需要第二种禁用原因时改为记录禁用来源 |
| 4 | 注销发生在**有活跃订单/正在充电**时：会话被撤销，但物理充电仍在继续，订单靠设备回执收尾，账单计入钱包 | 业务策略问题（是否需要先取消/拒绝注销）。迁移与适配器无需改动，但建议在 UC-U-05 文档里明确 |
| 5 | `internal/auth/redis.go` 的 `RevokeAllForUser` 依赖每用户 token 索引（SET + 脚本）——本次未改动 | 已复核为脚本内原子执行；A-01 的服务级测试已覆盖"冻结即撤销"（`profile_test.go` 调 `FreezeUser` 并断言撤销）；未覆盖的是**撤销失败**路径 |
| 6 | **头像长度只在数据库侧有界（512），API 侧不校验**：昵称有 1..20 与纯空白校验（400），头像没有长度/协议校验，超长 URL 会打到 0008 的 CHECK 上变成 **500 而不是 400** | A 线：建议在 `updateProfileRequest` 的校验里加长度（≤512）与方案（http/https）检查，让客户端拿到 400；数据库的 CHECK 作为最后一道防线保留 |

### 2.4 复核中发现并修复的**与本模块相邻**的测试缺陷

`TestAdminUserAndOrderLists` 用订单号的 14 位时间戳前缀做过滤（`OrderNo[3:17]`），并断言"恰好返回 1 条"。
同一秒内创建的任何订单都会命中该前缀，因此该断言取决于"这段时间里别的测试恰好创建了什么"——A-01 持久化用例
开始创建订单后它立刻失败（返回 2 条）。已改为按**完整订单号**过滤并保留"前缀过滤能命中"的独立断言
（`internal/repository/postgres/admin_integration_test.go`，B 线目录）。这是测试写法问题，不是 `ListOrders` 的缺陷。

## 3. PostgreSQL 集成测试

`backend/internal/repository/postgres/account_mutation_integration_test.go`（真实 PostgreSQL，随 runner 应用 0008）：

| 用例 | 断言 |
|---|---|
| `TestAccountMutationProfileRoundTrip` | 读资料（id/昵称/状态/注册时间）；仅昵称、仅头像、两者同时三种更新都落库；**仅头像更新不清空昵称**（COALESCE 语义）；空串清空头像；未知账号 → `ErrProfileNotFound`；并断言"存储返回原始手机号 → `auth.MaskPhone` 得到 `137****xxxx`"这条链路 |
| `TestAccountMutationUpdateRequiresAField` | 无字段更新被拒绝（回归：修复前是 panic），且不改变行 |
| `TestAccountMutationDeletionAnonymizesAndFreesThePhone` | 注销后：手机号 = `deleted-{id}@invalid`、昵称 = `已注销用户`、凭据清空、头像清空、状态 DISABLED、`deleted_at` 有值；读/更新/冻结/再次注销全部不再生效；**用原手机号重新注册会创建全新 ACTIVE 账号**（证明匿名化释放了号码） |
| `TestAccountMutationFreezeBlocksCharging` | 冻结 → DISABLED；`StartCharging` → `ErrUserFrozen` 且订单停在 CREATED；解冻 → 正常进入 STARTING（BR-07 的持久化侧证据） |
| `TestDeletedAccountInvariantsAreEnforcedByTheDatabase` | 直接用 SQL 造"已注销但 ACTIVE""已注销但仍有凭据""头像超 512"三种行，数据库全部拒绝；两个账号注销后占位手机号互不冲突 |
| `TestProfileColumnsSurviveAWalletProvisioning` | 钱包开户/重复开户不会清掉昵称，也不会给活跃账号写上 `deleted_at` |
| `TestUserProfileMigrationIsComplete`（`schema_test.go`） | 0008 必须含两列、三条约束、部分索引，且 up 迁移不得包含 `DROP COLUMN`；down 脚本必须按"约束→索引→列"回退 |
| `TestA01SchemaSatisfiesTheAdapterAndTheContract`（`a01_schema_contract_test.go`） | **第一步结论的可执行形式**：列（类型/可空性/默认值）、0008 三条约束、既有 phone/email 唯一性与 status 枚举、部分索引、wallet_accounts 两列、最高迁移版本 = 8 |
| `TestAccountMutationErrorMapping` | 未知账号 → `ErrProfileNotFound`；**上下文取消不得被误报为"找不到"**（保留 `context.Canceled`）；空更新是调用方错误而非"找不到" |
| `TestFrozenAccountIsDisabledForTheLoginPath` | 跨存储边界可被数据库侧证明的一半：冻结后登录路径用的 `FindUserByAccount` 返回 `DISABLED`；解冻后回到 `ACTIVE` |

### 反向验证（先破坏被保护的行为，确认用例失败）

```text
1) 迁移约束真的生效：在测试库 DROP 掉 0008 的三条约束 → 
   TestDeletedAccountInvariantsAreEnforcedByTheDatabase 失败：
   "the database accepted a deleted account in status ACTIVE"；
   重新加回约束时又被那条脏数据挡住（"check constraint ... is violated by some row"）——
   这恰好证明了约束的作用面：没有它，"已注销但可登录"的行可以存在于任何一次疏忽之后。
2) 适配器的 panic 路径真实存在：恢复复核前的三分支实现 →
   TestAccountMutationUpdateRequiresAField 失败（无字段调用触发 nil 解引用 panic）。
3) 迁移结论测试真的会发现缺口：
   - 删掉 idx_user_accounts_deleted_at → "the deleted_at index is missing: sql: no rows in result set"；
   - 删掉 user_accounts_avatar_url_length 并临时要求一个不存在的列 →
     "user_accounts.column_that_does_not_exist is missing from the schema" 与
     "constraint user_accounts_avatar_url_length is missing"；
   两者各自恢复后测试重新通过 —— 说明"0008 已足够"这个结论是被检查出来的，不是被声明的。
```

## 4. 验证命令与结果

```text
环境：PostgreSQL 16.15（127.0.0.1:55439）、Redis 7（DB 14）、Go 1.22 工具链

命令：gofmt -l ./cmd ./internal
结果：无输出

命令：go build ./... && go vet ./...
结果：通过

命令：NCS_TEST_PG_DSN=... NCS_REDIS_TEST_DB=14 go test -count=1 -race ./...
结果：全部 ok（repository/postgres 5.806s、auth 3.514s、repository/redis 6.075s、
      cmd/worker 2.020s、cmd/outbox-publisher 1.593s、cmd/mock-gateway 1.976s …）

命令：psql -c "SELECT version, name FROM schema_migrations ORDER BY version DESC LIMIT 2"
结果：8 | user_profile_and_deletion.sql、7 | charger_command_outcomes.sql

命令：新用例连续两次运行（固定手机号会撞 UNIQUE）
结果：两次均 ok —— 用例手机号按运行唯一（uniquePhone）
```

## 5. 交付清单

```text
新增：
  backend/migrations/0008_user_profile_and_deletion.sql
  backend/migrations/down/0008_user_profile_and_deletion.down.sql
  backend/internal/repository/postgres/account_mutation_integration_test.go
  backend/internal/repository/postgres/a01_schema_contract_test.go（第一步结论的可执行断言）
  docs/migration/a-01-persistence-review.md（本文件）
修改：
  backend/internal/repository/postgres/accounts.go（适配器复核修复：单语句更新、拒绝空更新、列常量、边界注释）
  backend/internal/repository/postgres/schema_test.go（0008 形状测试）
  backend/internal/repository/postgres/admin_integration_test.go（订单号过滤改为完整号 + 前缀断言）
未修改：
  backend/internal/auth/**（A 线）：端口、服务、处理器、Redis 会话实现均未改动
  api/openapi.yaml（集成人员）：仍待登记 A-01 的四类接口
  docs/migration/approval-log.md（集成人员）

## 6. 收口状态

```text
B 线已完成（本分支）：
  正式迁移 0008 + down 脚本；
  迁移结论核对（0 节，无需 0009，且有可执行断言）；
  AccountMutationAdapter 的 SQL / 事务边界 / 错误映射复核（含 4 项修复、6 项登记）；
  真实 PostgreSQL 集成测试 10 个（含并发、注销、冻结、不变量、错误映射、schema 契约）；
  反向验证 3 组。

仍待集成人员（不属 B 线）：
  1. 把 A-01 的四类接口登记到 api/openapi.yaml（提案：docs/migration/proposals/a-01-openapi-profile-endpoints.md）；
  2. 合入 A-01 分支（codex/backend/a-01-auth）与本叠加分支（git rebase --onto origin/develop ac4e407）；
  3. 更新 approval-log：A-01 记为 APPROVED（用户资料、注销、冻结/解冻 + 0008 收口）。

A 线待办（已登记，不阻塞收口）：
  头像长度/方案的 API 侧校验（400 而不是 500）；ProfileView.PhoneMasked 字段改名；
  DELETE /me 重试返回 204 而非 404；SetFrozen(false) 的禁用来源记录；注销与活跃订单的业务策略。
```
```
