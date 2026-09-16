# 旧技术栈退役与 fork 集成验证说明

## 关联信息

本说明记录 `cjyhjy` fork 的独立集成测试分支：整合 Go 后端与 Vue 前端、把旧技术栈移出默认构建与 CI，并给出实际运行结果。
需求基线仍是 [需求规格说明书](../01-requirements-specification.md)；本说明只改变运行技术栈入口，不把任何业务条目标记为完成。

| 项目 | 值 |
| --- | --- |
| fork 分支 | `codex/go-vue-retirement`（fork 独占，不触碰负责人 `develop`） |
| 上游基线 | `heguangV/charging-station-platform` 的 `develop`（本分支为其后 24 个提交） |
| 上游提交 | 退役 PR 由项目负责人发起；合并与分支保护调整不在本分支范围内 |
| 运行入口 | `backend/cmd/{api,worker,outbox-publisher,mock-gateway}`、`apps/{user,admin,dashboard}` |
| 归档目录 | `legacy/`（C++/Crow/Qt/SQLite、旧客户端、设备模拟器、旧 CMake 与旧运维脚本） |

## 变更内容

- 旧技术栈源码整体移入 `legacy/`，不再参与默认构建、CTest 与 CI；`legacy/README.md` 说明其仅为需求映射与历史实现参考。
- CI 改为 Go 门禁（`.github/workflows/ci.yml`）与 Web 门禁（`.github/workflows/web-clients.yml`）：真实 PostgreSQL 18 与 Redis 7.4 服务容器、`-race` 全量测试且零跳过、四进程闭环、备份恢复演练、三个 Web 客户端测试与构建。
- 新增 `scripts/test-backend.sh`：在真实 PostgreSQL/Redis 上运行 `go test -race`，并把“测试被跳过”判定为失败，避免集成测试静默失效。
- 修复本轮验证中发现的缺陷：`internal/auth` 的 Redis 集成测试使用了与全仓库不一致的变量名（`NCS_TEST_REDIS_ADDR`），在 CI 中必然被跳过；`start/stop/cancel/confirm` 这类要求空请求体的端点会被前端发送的 `{}` 触发 400；备份演练的迁移门禁缺少网关令牌、写死的 schema 版本断言以及 GNU 专有的 `stat`/`sha256sum` 依赖。

## 影响与兼容性

- 数据库、错误码、Idempotency-Key 16–128 字符约束与 REST/WebSocket 契约不变；`legacy/` 不参与构建，不需要旧工具链。
- 客户端只经 `/api/v1/*` 取数；三个 Web 应用仍是 npm/pnpm 独立构建，不是 CMake 目标。
- 本地与 CI 均要求可用的 PostgreSQL 与 Redis；缺少时集成测试按跳过处理，而 CI 会因此失败。
- 回滚：本分支为 fork 独占，可不合并；合入上游后如需回滚，revert 退役提交即可回到旧构建入口，数据无迁移破坏性变更。

## 验证结果（2026-09-16，macOS arm64）

环境：Go 1.27.1、Node 26.7.0、pnpm 10.15.1、PostgreSQL 18.6、Redis（本机服务）。测试库独立创建并在本轮结束后可丢弃；未使用任何既有业务库。

| 验证项 | 命令 | 结果 |
| --- | --- | --- |
| Go 构建 | `go build ./...` | 通过 |
| Go 静态检查 | `go vet ./...`、`gofmt -l .` | 通过，gofmt 无输出 |
| Go 单元 + 集成 + 竞态 | `NCS_TEST_PG_DSN=... NCS_REDIS_TEST_ADDR=... ./scripts/test-backend.sh` | 1312 个测试及子用例通过，0 跳过，0 失败（`-race`） |
| PostgreSQL/Redis 集成 | 同上（真实 PG 18.6 + Redis） | 迁移器与仓储、短信原子校验、重复投递、会话隔离等真实执行，无跳过 |
| 四进程充电闭环 | `NCS_E2E_ALLOW_DESTRUCTIVE=true backend/scripts/verify-closed-loop.sh` | 通过：CREATED→STARTING→CHARGING→STOPPING→COMPLETED、设备回执写回、重复回执不重复计费、重启命令闭环、消息排空、Publisher 单活、Worker 崩溃重领 |
| 备份与恢复演练 | `NCS_TEST_PG_DSN=... backend/scripts/drill-backup-restore.sh` | 通过：dump 可读、恢复后 schema 版本 12、备份前订单与钱包在、备份后写入按 RPO 缺失、RTO 0.00 分钟 |
| 车主端测试 | `apps/user: npm run test` | 122/122 |
| 车主端构建 | `apps/user: npm run build` | 通过 |
| 管理端测试 | `apps/admin: npm run test` | 158/158 |
| 管理端构建 | `apps/admin: npm run build` | 通过 |
| 大屏测试与构建 | `apps/dashboard: pnpm run test && pnpm run build` | 13/13，构建通过 |
| 前后端真实联调（浏览器） | Tabbit 浏览器 + 本地 Go 全栈 | 见下 |

### fork CI 运行结果

推送分支 `codex/go-vue-retirement` 后，fork 的两个工作流均通过（不涉及上游 `develop`）：

| 工作流 | 运行 | 结果 |
| --- | --- | --- |
| Go integration | [35137501382](https://github.com/cjyhjy/charging-station-platform/actions/runs/35137501382) | 通过；`go-integration`、`dashboard`、`go-windows-build` 三个作业全部成功 |
| Web clients | [35137501384](https://github.com/cjyhjy/charging-station-platform/actions/runs/35137501384) | 通过；`web (user)`、`web (admin)` 均成功 |

CI 证据（`go-integration` 作业上传的 `go-integration-evidence` 制品）：真实 PostgreSQL 18 与 Redis 7.4 上
`1312 tests/subtests passed, 0 skipped, 0 failed`（20 个包，`-race`）；四进程充电闭环 `PASS`；
备份恢复演练 `RESTORED_SCHEMA_VERSION=12`、`RTO_MINUTES=0.02`。

浏览器联调（真实 Go API + PostgreSQL + Redis + 模拟网关，非 mock 数据）：

1. 车主端验证码登录成功，钱包读取为 50.00 元（来自 `wallet_accounts`）。
2. 站点列表与站点详情显示真实设备（A01 交流 7 kW、B01 直流 120 kW）与价格；未配置腾讯地图 Key 时给出明确降级提示，列表与导航仍可用。
3. 预约生成订单 `ORD2026091618463914792f6b`，开始充电后进入“充电中”，显示设备实时计量（0.10 kWh / 0.11 元，读数时间来自设备）。
4. 停止充电返回结算小票：0.15 kWh、0.17 元、支付状态“待确认”；订单列表同步。
5. 确认支付后钱包余额由 50.00 元变为 49.83 元，欠费 0.00 元，扣款与订单金额一致。
6. 管理端登录成功，总览读取到同一笔业务：今日营收 0.17 元、本月电量 0.15 kWh、今日订单 1 单、注册用户 1、可运营电桩 3。
7. 管理端充电桩页发起远程重启，返回命令编号 `CMD20260916184955a67cf20a`，随后状态为“已成功”，验证管理端→设备命令→回执的完整链路。
8. 管理端用户页余额同步为 49.83 元，手机号按契约脱敏。

## 已确认的迁移缺口（不得视为已完成）

下列能力在旧栈中存在、在 Go/Vue 运行栈中尚未落地，因此退役不等于迁移验收通过：

| 缺口 | 事实 | 影响 |
| --- | --- | --- |
| 运营大屏与 Go 的契约 | 大屏仍调用 `/api/v1/dashboard/{auth/login,auth/logout,summary}`（旧 `dashboard_routes.cpp` 的契约），Go 后端未注册任何 `/api/v1/dashboard/*` 路由 | 大屏无法登录与取数；本机实测“登录失败”。需先决策：为新栈补该契约，或把大屏改接现有 `/api/v1/admin/stats/*` |
| 行政区价格版本 | 旧栈 `createTariff`（按 adcode 的电价/服务费版本、生效区间、变更原因）在 Go 后端与迁移中没有对应实现或表 | 区域定价版本管理缺失；Go 现有的“全量车队电价”不覆盖该需求 |
| 管理端备份界面 | Go 仅有 `backend/scripts/` 下的备份/恢复脚本与 systemd 单元，无管理端界面 | 需界面化或明确降级为运维脚本 |
| 管理员二次验证 | Go 后端与迁移中无 TOTP/二次验证实现 | 管理端登录仅有单因子 |
| 服务端路线规划 | 站点详情页面显式提示“服务端路线规划（A-07）尚未接入 Go 后端” | 大屏/车主端导航只有坐标换算与地图跳转 |
| ML 与大屏业务指标 | 旧栈 `ml/` 与旧大屏指标未迁移 | 预测类需求未实现 |
| 真实设备与外部服务 | 闭环使用模拟网关；未验证真实 Modbus/OCPP 网关 | 设备协议联调未验收 |
| 真实供应商服务 | 未配置真实 LLM、腾讯地图服务端 Key、外部短信与支付 | 供应商实网验证未做 |

同时明确：需求矩阵与 `docs/requirements-traceability.md` 的既有完成状态沿用历史证据（多为 SQLite/Qt/大屏实现），不因本轮 CI 通过而升级为 Go 等价验证结论。

## 环境边界

- 本轮验证是本机与 fork CI 层面的等价验证，不修改上游 `develop`，不做分支保护调整。
- 闭环与演练脚本按设计具有破坏性，只允许指向一次性测试库；`verify-closed-loop.sh` 与 `drill-backup-restore.sh` 均带有库名与显式确认门禁。
- 恢复演练在同一实例内完成，度量的是数据库工作量而非跨主机复制；本部署未启用 WAL 归档，因此未演练 PITR。
