# B-06 上线前四项验证记录

## 基本信息

- 模块编号：B-06（补验，不改模块结论口径）
- 开发线：B
- 开发分支：`codex/backend/b-06-golive-verification`（从已批准的 `04c1f35` 新建，**未改动已批准的 B-06 分支**）
- 触发依据：B-06 第三轮裁定的修改要求——"上线前仍须补齐并实测：真实 rclone 异地上传、第二个 PostgreSQL
  实例上的 PITR 重放、告警规则实际触发、临时数据库创建/恢复/删除全流程。"

## 结论总表

| # | 裁定要求 | 本次结果 | 生产前是否还须重跑 |
| - | -------- | -------- | ------------------ |
| 1 | 真实 rclone 异地上传 | **路径全链路实测通过**（加密→上传→取回→解密→sha256 一致→恢复进一次性库）；目标端为**回环 S3 兼容端点**，不是真实异地对象存储 | **是**：真实 bucket、版本控制、生命周期规则、SSE、专用最小权限凭据 |
| 2 | 第二个 PostgreSQL 实例上的 PITR 重放 | **通过，且首次真正落在批准的 5–15 分钟窗口内**（目标 10.0 分钟前；隔离临时实例；标记前后断言） | **是**：生产实例上再跑一次（本项裁定原文要求） |
| 3 | 告警规则实际触发 | **通过**：12 条规则在真实 Prometheus 中按各自 `for:` 时长依次 firing；另加 promtool 秒级回归、健康态零误报对照、投递链路（Alertmanager→webhook）实测、真实进程指标存在性核对 | **是**：生产监控栈与真实接收器；且必须先补 Alertmanager 配置（见 F6） |
| 4 | 临时数据库创建/恢复/删除全流程 | **通过**：建库→迁移→seed→备份→恢复→断言→删库，残留 0，RTO 0.03 分钟 | 否（该流程已可复现；生产演练仍建议每月一次） |

一句话：**裁定的四项里，三项在本机拿到了可复现证据，第四项（临时库全流程）本就跑通并再次复跑通过；
四项都仍不构成"生产灾备已验证"**，口径与第三轮裁定一致。

## 环境与工具

本机没有 rclone / Prometheus / Alertmanager，且 `proxy.golang.org` 不可达（`goproxy.cn` 可达），
因此工具是用 Go 从模块代理构建的。版本与构建方式如下，便于复现：

| 工具 | 版本 | 构建方式 |
| ---- | ---- | -------- |
| rclone | v1.69.0（模块 tag；`go build` 未注入 ldflags，故 `--version` 显示 `-DEV`） | `GOPROXY=https://goproxy.cn,direct GOSUMDB=off GOTOOLCHAIN=local go install github.com/rclone/rclone@v1.69.0` |
| Prometheus | v0.300.0 模块 tag（= Prometheus 3.0） | 该 tag 的 go.mod 含 `exclude`，`go install pkg@version` 被 Go 拒绝，改为下载模块 zip 后作为主模块 `go build ./cmd/prometheus` |
| promtool | 同上 | 同源 `go build ./cmd/promtool` |
| Alertmanager | v0.28.0 | `go install github.com/prometheus/alertmanager/cmd/alertmanager@v0.28.0` |

- 编排与注入用的脚本、配置、记录目录：`/tmp/ncs-drill/`（`prom/` 监控、`backups/` 备份、`wal/` WAL 归档）。
- 目标端：`rclone serve s3` 提供的本机 S3 兼容端点 `127.0.0.1:8333`，`rclone.conf` 里的远端名为
  批准基线冻结的 `ncs-backup-prod`，bucket 为 `ncs-prod-backup`——**远端名与 bucket 名与批准值一致，
  只是落点在本机回环**。
- 生产凭据与密钥仍按约定放在仓库外：本次用 0600 的 `~/ncs-drill/backup-remote.env`、`backup.key`、
  `rclone.conf` 顶替 `/etc/ncs/*`，脚本本身未因此改动。

## 项目一：真实 rclone 异地上传

### 步骤与原始输出

1) 用交付脚本产生真实备份（`pg_dump` + manifest）：

```text
$ NCS_POSTGRES_DSN=... NCS_BACKUP_DIR=~/ncs-drill/backups bash backend/scripts/backup-postgres.sh --dir ~/ncs-drill/backups
backup:   /home/penty/ncs-drill/backups/ncs_a03-20260915T111546Z.dump
size:     115566 bytes, 0s, 115 objects
sha256:   3c11ae283404451eeccaa9ff155bb1298074067681a19e0298bc5837338ea6d0
```

2) 交付脚本的异地推送（加密 → 上传 → 目标端保留期清理）：

```text
$ NCS_BACKUP_REMOTE_CONFIG=~/ncs-drill/backup-remote.env bash backend/scripts/push-backup-remote.sh --dump <dump>
=== encrypt ncs_a03-20260915T111546Z.dump ===
=== upload to ncs-backup-prod:ncs-prod-backup/daily/2026/09/15/ ===
=== verify the uploaded object against what was sent ===
  S3 bucket ncs-prod-backup path daily/2026/09/15: 0 differences found
  S3 bucket ncs-prod-backup path daily/2026/09/15: 2 matching files
=== retention at the destination (daily 30 days, weekly 12 weeks) ===
pushed 1 encrypted backup(s) to ncs-backup-prod:ncs-prod-backup (daily prefix kept 30 days, weekly prefix kept 12 weeks)
REMOTE_PUSHED=1
```

（其中 `verify the uploaded object` 一段是本次新增的校验步骤，见 F2。）

3) 远端对象与取回解密后的完整性：

```text
$ rclone lsl ncs-backup-prod:ncs-prod-backup
   115584 2026-09-15 19:15:55 daily/2026/09/15/ncs_a03-20260915T111546Z.dump.enc
      181 2026-09-15 19:15:55 daily/2026/09/15/ncs_a03-20260915T111546Z.dump.enc.manifest

本地原文 sha256:      3c11ae283404451eeccaa9ff155bb1298074067681a19e0298bc5837338ea6d0
取回解密后 sha256:    3c11ae283404451eeccaa9ff155bb1298074067681a19e0298bc5837338ea6d0
```

4) **取回的副本真的能恢复**（这是"备份有用"的唯一判据）：用交付脚本把从远端取回、解密后的 dump
恢复进一次性数据库并断言：

```text
$ NCS_RESTORE_JOBS=2 bash backend/scripts/restore-postgres.sh --dump /tmp/ncs-drill/roundtrip/recovered.dump --dsn .../ncs_drill_offsite
restored ncs_drill_offsite from recovered.dump in 0s
RESTORE_SECONDS=0
RESTORED_SCHEMA_VERSION=7
schema_version=7   tables=15   users=35   wallets=35   ledger=4
（随后 DROP DATABASE，残留 ncs_drill_* = 0）
```

5) 失败路径（目标 bucket 未 provisioning）**硬失败、不静默**：

```text
$ bash backend/scripts/push-backup-remote.sh --dump <dump>     # bucket 不存在
ERROR : ... api error NoSuchBucket: The specified bucket does not exist
退出码=1        跑到"保留期清理"步骤的行数=0        （远端无残留对象）
```

### 局限（不得据此宣称"生产异地备份已验证"）

- 目标端是**本机回环**的 S3 兼容端点，没有跨主机/跨地域链路，因此带宽、断点续传、真实网络抖动、
  跨区域延迟与配额都没有被验证。
- bucket 的**版本控制、生命周期规则、服务端加密（SSE）** 是脚本注释里要求"由部署打开"的三项保护，
  本机端点不具备；脚本传给 rclone 的 `--s3-server-side-encryption AES256` 在真实对象存储上是否被接受，
  仍需在真实 bucket 上确认一次。
- 生产上备份角色应为非超级用户（README 有 SQL），本次沿用测试库角色，未验证最小权限下的行为。
- 30 天/12 周的异地保留期是靠 `rclone delete --min-age` 与 bucket 生命周期规则共同保证的；本次只跑到
  "新对象不会被误删"（上传后保留期步骤执行完毕、对象仍在），**没有**等到跨过 30 天窗口去验证删除行为。

## 项目二：第二个实例上的 PITR 重放

### 先修脚本：原来那句"10 分钟 nominal rewind"是不成立的

读代码发现 `NCS_PITR_REWIND_MINUTES` **只用于校验与打印**：恢复目标取的是写入 marker 之后的
`CURRENT_TIMESTAMP`。实测证据（修复前一次运行）：

```text
base backup taken at 11:17:07
recovery target:   2026-09-15 11:17:09.360315+00      ← 与基备只差约 2 秒
approved window: recovery target 5..15 minutes before a chosen point (this run used 10 minutes as its nominal rewind)
```

也就是说：报告声称"按 10 分钟的名义回退"，实际演练的是"恢复到 2 秒前"，而批准的 5–15 分钟窗口
**从未被真正演练过**。修复见 F5（把等待变成真的等，并把测得的年龄写成断言）。

### 修复后的运行（真实窗口，通过）

10 分钟（默认值）：

```text
=== write markers around a chosen target time ===
target time: 2026-09-15 11:18:08.843674+00 (between the two markers)

=== let the target age into the approved window ===
  target is 2s old; waiting 598s more for the 10-minute window
  ...
target is now 10.00 minutes old (approved band: 5..15)

=== PITR drill report ===
result:            PASS
recovery target:   2026-09-15 11:18:08.843674+00
wait for the window: 10.05 minutes (the target aged to the approved band before recovery started)
recovery duration: 610 seconds (temporary instance start to ready)     ← 补丁前口径：把等待算进了回放
archive coverage:  archive holds 3 segment(s) from the base backup at 11:18:06 to now
markers: before_target present, after_target absent (recovery stopped at the target)
approved window: recovered to a target 10.05 minutes old (measured at recovery start, asserted 5..15)
```

5 分钟（窗口下界，并验证报告口径修正后等待与回放分开统计）：

```text
=== PITR drill report ===
result:            PASS
recovery target:   2026-09-15 11:30:13.273817+00
wait for the window: 5.00 minutes (the target aged to the approved band before recovery started)
recovery duration: 3 seconds (temporary instance start to ready)
archive coverage:  archive holds 4 segment(s) from the base backup at 11:30:11 to now
markers: before_target present, after_target absent (recovery stopped at the target)
approved window: recovered to a target 5.00 minutes old (measured at recovery start, asserted 5..15)
```

两次运行都断言"目标之前的 marker 在、之后的 marker 不在"，因此是**真的停在了目标时刻**，
不是"实例起来了"。两次运行结束后均核对：临时实例、临时目录、复制槽全部清理。

### 归档与复制槽

- 交付的归档器可用：`wal-archive.sh --once` 建立流式接收、创建并**回收**槽，未留下会永久钉住 WAL 的空闲槽：

```text
$ NCS_POSTGRES_DSN=... NCS_BACKUP_WAL_DIR=/tmp/ncs-drill/wal bash backend/scripts/wal-archive.sh --once
creating replication slot ncs_wal_archive
smoke check complete: connected as ncs_test@..., slot created and dropped again (the service recreates it)
运行后 ncs_wal_archive 槽: (none)
```

- PITR 演练结束后同样核对过：临时实例、临时目录与复制槽均被清理（断言见文末）。

### 局限

- 源库与恢复实例在**同一台机器**上，回放所需的 WAL 通过本地文件系统 `restore_command='cp ...'` 取得，
  因此没有验证跨主机取 WAL、也没有验证生产实例的 `archive_command`/备份链。
- 恢复实例是 `initdb`/`pg_basebackup` 出来的隔离实例，不是生产规格（参数、磁盘、连接数均不同）。
- 裁定要求"生产实例上仍需一次"，本次**不能**替代它。

## 项目三：告警规则实际触发

### 三层证据

1) 静态与秒级回归 —— 交付规则文件**未改动**（sha256 `15dc114e…` 与仓库一致）：

```text
$ promtool check rules backend/deploy/monitoring/ncs-alerts.yml
SUCCESS: 12 rules found

$ promtool test rules backend/deploy/monitoring/ncs-alerts.test.yml     # 本次新增
SUCCESS
```

新增的 `ncs-alerts.test.yml` 用合成样本覆盖 12 条规则的**阈值边界与 `for:` 时长**（早一分钟必须不报、
到点必须报），并含"完全健康/空库时零告警"对照。反向验证（改副本、不动交付文件）：

```text
把 NCSOutboxBacklogCritical 的 for: 15m 改成 5m    → FAILED（5m 处多出一条不该有的告警）
把 ncs_pg_outbox_unpublished > 100 改成 > 5000     → FAILED，退出码 1
```

2) 实机触发 —— 真实 Prometheus（9099）加载同一份规则，抓取受控故障指标源，规则按各自 `for:`
   依次转入 firing（时间线见文末原始证据）：

| 注入后 | 状态 | 新增 firing |
| ------ | ---- | ----------- |
| ~0s | firing 2 | `NCSDeadLettersPresent`、`NCSDeadLetterWritten`（`for: 0m`） |
| 2m | firing 3 | `NCSDependencyDown`（postgres 与 redis 各一条） |
| 5m | firing 7 | `NCSOutboxBacklogWarning`、`NCSOutboxOldestWarning`、`NCSPublisherStalledCritical`、`NCSWorkerStalledCritical` |
| 10m | firing 11 | `NCSOutboxOldestCritical`、`NCSPublisherNotPublishing`、`NCSWorkerNotConsuming`、`NCSPendingGrowing` |
| 15m | firing 12 | `NCSOutboxBacklogCritical` |

   对照：故障注入前（健康值）12 条规则全部 `inactive`、`health=ok`，无一条误报。

3) 投递链路 —— 规则 firing 若无人收到等于没告警。仓库里**没有任何 Alertmanager 配置**（见 F6），
   因此本次自建了最小配置与 webhook 接收器，实测走通：

```text
Alertmanager 中的告警: ['NCSDeadLetterWritten', 'NCSDeadLettersPresent']
webhook 收到: {"status":"firing","alertname":"NCSDeadLettersPresent","severity":"critical",
              "startsAt":"2026-09-15T11:20:06.731Z","summary":"死信流中存在 3 条未处理事件"}
```

4) 指标存在性 —— stub 只能证明"规则会触发"，还要证明**真实进程确有这些数据源**（否则生产静默不告警）。
   启动 API(9090)/Worker(9091)/Publisher(9092) 三个真实进程并用 Prometheus 抓取，逐条核对规则引用的 8 个指标：

```text
ncs_dependency_up                            api,publisher,worker      dependency=postgres,redis
ncs_pg_outbox_unpublished                    api,publisher,worker
ncs_pg_outbox_oldest_unpublished_seconds     api,publisher,worker
ncs_publisher_last_publish_timestamp_seconds publisher
ncs_worker_last_success_timestamp_seconds    worker
ncs_stream_pending                           api,worker                stream=charge-event,charger-command,order-event
ncs_stream_dead_letter_length                api,publisher,worker
ncs_worker_dead_lettered_total               worker
规则引用但任何进程都没有暴露的指标: 无
```

   真实进程下按规则评估：无告警（系统健康），即**真实数据也不会误报**。

### 局限

- 触发用的是受控故障指标源（受控值），不是"停掉真实 Publisher/Worker"；不过规则表达式、阈值、
  `for` 时长与标签都在真实 Prometheus 里求值，且真实进程的指标存在性已单独核对。
- 监控栈是单机单副本、无 HA、无长期存储、无 external_labels 保留策略；生产拓扑下的行为未验证。
- 接收器是本次自建的 webhook，不是真实邮件/IM/Pager；路由、分组、抑制、静默策略都还没有生产配置。

## 项目四：临时数据库创建/恢复/删除全流程

```text
$ NCS_TEST_PG_DSN=postgres://ncs_test@127.0.0.1:55439/ncs_a03?sslmode=disable bash backend/scripts/drill-backup-restore.sh
instance allows create/drop: ok
orders before backup: 1
restored ncs_drill_restore_20260915111500 from ncs_drill_source_20260915111500-20260915T111508Z.dump in 2s
=== assertions ===
PostgreSQL RTO   <= 60 minutes   0.03 minutes
PostgreSQL RPO   <= 15 minutes   15 minutes (the dump interval; a row written after the dump is confirmed absent)
RPO_MINUTES=15
RTO_MINUTES=0.03
$ psql ... -tAc "SELECT count(*) FROM pg_database WHERE datname LIKE 'ncs_drill_%'"
0
```

即：一次性源库、迁移、seed、备份、备份后写入、恢复到一次性库、逐项断言、两个临时库都删除，
没有残留。**但报告里"RPO 15 minutes"这一行是目标值而不是测量值**（见 F4），不要据此认为
备份额度已被测量为 15 分钟。

## 本次新发现（含已修与待审批）

| 编号 | 发现 | 严重性 | 本次处理 |
| ---- | ---- | ------ | -------- |
| F1 | `backup-postgres.sh` 打印的运维命令 `verify-backup.sh --dump <file>` 无法执行（该脚本只接受 `--dir/--count/--restore`，实测 `unknown argument: --dump`，退出 2） | 中：照抄命令会失败 | **已修**（改为打印可执行的 `--dir` 形式并指向目录） |
| F2 | `push-backup-remote.sh` 上传后不校验远端对象，截断/半写对象在需要它之前不可见 | 高：备份"成功"但不可读 | **已修**（上传后 `rclone check` 比对大小与哈希；反向验证：远端对象截断 → `sizes differ`、退出码 1） |
| F3 | `--s3-no-check-bucket` 下 bucket 未 provisioning 会 `NoSuchBucket` 硬失败 | 无（行为正确） | 仅记录：失败即非零退出并中断，未跑到保留期步骤，远端无残留 |
| F4 | 备份/恢复演练报告的 "RPO ≤ 15 minutes" 是目标值（`rpo_ok` 比较的是目标与目标），不是测量值；dump 的真实丢失上界是一天，15 分钟 RPO 由 WAL 归档保证 | 中：口径夸大 | **已修**（报告改为明确写出"目标值；本演练测量的是 dump 间隔，15 分钟 RPO 由 WAL 归档交付，由 pitr-drill.sh 测量"） |
| F5 | `pitr-drill.sh` 的 `NCS_PITR_REWIND_MINUTES` 只校验与打印，实际恢复目标是"现在减 2 秒"，批准的 5–15 分钟窗口未被演练 | 高：PITR 能力被高估 | **已修**：目标先"变老"到窗口内再恢复，并把测得的年龄写成断言（5–15 分钟），报告改写为实测值 |
| F6 | 仓库只有 12 条告警规则，**没有 Alertmanager 配置/接收器**：规则会 firing，但没有任何投递路径 | 高：告警到不了人 | 已用最小配置+webhook 证明链路可行（配置在 `docs/migration` 记录中给出）；**生产配置与真实接收器仍待部署方补齐** |

F1、F2、F5 属对已批准脚本的修改，仍需审批人确认；F4、F6 是文档与部署配置问题，建议一并处理。

## 仍需在生产完成（不得宣称已完成）

1. 真实异地对象存储：真实 bucket + 版本控制 + 生命周期规则 + SSE + 专用最小权限凭据，以及跨主机链路。
2. 生产实例上的 PITR 一次（含生产 `archive_command`/备份链与跨主机取 WAL）。
3. 生产监控栈上的告警触发一次，并补齐 Alertmanager 路由与真实接收器（F6）。
4. `ncs-alerts.test.yml` 纳入 CI（`promtool test rules` 可在无环境依赖下跑）。
5. 30 天/12 周异地保留策略的窗口验证（需要时间或可控的假时钟）。

## 交付物

分支 `codex/backend/b-06-golive-verification`（基线 `04c1f35`）：

- 修改：
  - `backend/scripts/pitr-drill.sh`（目标真正"变老"到批准窗口内再恢复、测得年龄写成断言、等待与回放分开报告）
  - `backend/scripts/push-backup-remote.sh`（上传后 `rclone check` 校验远端对象）
  - `backend/scripts/backup-postgres.sh`（打印的 verify 命令改为可执行形式，F1）
  - `backend/scripts/drill-backup-restore.sh`（RPO 一行的口径改为"目标值"，F4）
  - `backend/deploy/README.md`（把"规则尚未验证"改成实测结论与仍存生产项）
- 新增：
  - `backend/deploy/monitoring/ncs-alerts.test.yml`（12 条规则的阈值/时长秒级回归，含健康态对照）
  - `backend/scripts/verify-alerts.sh`（一键跑 `promtool check rules` + `promtool test rules`）
  - 本文件
- 未改动：任何 Go 源码、迁移、`api/openapi.yaml`、已批准的 B-06 分支

注：CI（`.github/workflows/ci.yml`）目前没有 Go 或监控作业，`verify-alerts.sh` 需要挂到某个后端检查作业上
（或写进上线检查清单）才会自动生效。

## 原始证据

### 告警 firing 时间线（每 20 秒采样一次，共 35 条；此处列出 firing 数量发生变化的时刻）

故障值注入于 `11:16:54Z`；规则文件的 `for:` 分别为 0m×2、2m×1、5m×4、10m×4、15m×1。

| 采样时刻 (UTC) | firing | pending | 该时刻新转入 firing 的规则 |
| -------------- | ------ | ------- | -------------------------- |
| 11:18:54 | 2 | — | `NCSDeadLettersPresent`、`NCSDeadLetterWritten`（`for: 0m`，注入后约 5 秒） |
| 11:18:54 | 3 | — | `NCSDependencyDown`（postgres/redis 各一条；`for: 2m`，注入后 2m00s） |
| 11:22:02 | 7 | 5 | `NCSOutboxBacklogWarning`、`NCSOutboxOldestWarning`、`NCSPublisherStalledCritical`、`NCSWorkerStalledCritical`（`for: 5m`，注入后 5m08s） |
| 11:27:14 | 11 | 1 | `NCSOutboxOldestCritical`、`NCSPublisherNotPublishing`、`NCSWorkerNotConsuming`、`NCSPendingGrowing`（`for: 10m`，注入后 10m20s） |
| 11:32:00 | 12 | 0 | `NCSOutboxBacklogCritical`（`for: 15m`，注入后 15m06s） |
| 11:32:48 | — | — | 清除故障值 |
| 11:33:05 | 2 | 0 | 10/12 已恢复；余下两条基于窗口的规则待窗口滚过（`increase[5m]`、`deriv[10m]`，属表达式性质） |

原始行（节选，格式为 `时刻 firing=N 列表 | pending=N`）：

```text
11:18:54Z firing=3  NCSDeadLetterWritten,NCSDeadLettersPresent,NCSDependencyDown | pending=9
11:22:02Z firing=7  NCSDeadLetterWritten,NCSDeadLettersPresent,NCSDependencyDown,NCSOutboxBacklogWarning,NCSOutboxOldestWarning,NCSPublisherStalledCritical,NCSWorkerStalledCritical | pending=5
11:27:14Z firing=11 ...,NCSOutboxOldestCritical,NCSPendingGrowing,NCSPublisherNotPublishing,NCSWorkerNotConsuming | pending=1
11:32:00Z firing=12 ...,NCSOutboxBacklogCritical | pending=0
11:33:05Z firing=2  NCSPendingGrowing,NCSDeadLetterWritten | pending=0
```

### 告警恢复投递（webhook 原文，节选）

```text
{"received_at":"2026-09-15T11:32:49Z","status":"resolved","alertname":"NCSDependencyDown","severity":"critical","startsAt":"2026-09-15T11:22:08.611Z","summary":"postgres 不可达（ncs-fault-injection）"}
{"received_at":"2026-09-15T11:32:51Z","status":"resolved","alertname":"NCSPublisherNotPublishing","severity":"warning", ...}
（共 10 条 resolved 投递）
```

### 环境清理核对

```text
pg_replication_slots: (none)      # 演练结束后无遗留复制槽（空闲槽会永久钉住 WAL）
/tmp/ncs-drill/pitr2, pitr3: 已清理
ncs_drill_* 数据库: 0
```

## 审批结论

```text
状态：PENDING
审批人：
审批时间：
修改要求：
```
