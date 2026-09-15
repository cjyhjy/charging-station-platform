# Go 路由与 OpenAPI 对照矩阵（前后端联调 第 1 阶段 · B 线交付物）

- 分支：`codex/backend/frontend-integration`（基线 `origin/develop` @ `422ee83`）
- 依据：`docs/integration/frontend-go-integration-task-plan.md` 第 5 节（第 1 阶段：B 线输出 Go 路由和 OpenAPI 对照）、B-01、B-02
- 本文件回答的问题：**前端要调的每个接口，Go 是否真的存在、是否已登记 OpenAPI、鉴权与幂等要求是什么、两边是否一致。**
- 状态口径（第 4 节"第二阶段：契约冻结"）：`MATCH` / `FRONTEND_CHANGE` / `BACKEND_CHANGE` / `BLOCKED`。
  只有 `MATCH` 项才能进入页面联调。

## 0. 事实来源（可复现）

| 事实 | 取法 |
| ---- | ---- |
| Go 实际注册的路由与鉴权包装 | `grep -rhoE 'Register\("[^"]+", *[^)]*\)' cmd/api internal --include='*.go' --exclude='*_test.go'`（36 条产品路由 + `order.ChargerEventPath` 常量注册的 `/api/v1/internal/charger-events`） |
| 探测端点 | `internal/httpapi/server.go:84-85`：`/healthz`、`/readyz` 注册在**根路径** |
| OpenAPI 操作 | `python3 -c "import yaml; yaml.safe_load(open('api/openapi.yaml'))"` → 45 个操作 / 38 条路径，`servers: ['/api/v1']`，全局 `security: [bearerAuth]` |
| 实机验证 | `local-stack.sh --seed` 起全栈后 `login ok as the seeded user, 1 station(s) visible` |

## 1. 全局契约（前后端都要按这个来）

| 项 | 约定 | 出处 |
| -- | ---- | ---- |
| 响应信封 | `{success, code, message, data}` | `internal/httpapi` |
| 成功 | `success=true, code=0` | 同上 |
| 错误码 | 0 OK／1 INVALID_ARGUMENT(400)／3 DATABASE_ERROR(503)／4 NOT_FOUND(404)／6 USER_FROZEN(403)／14 IDEMPOTENCY_CONFLICT(409)／18 ORDER_NOT_REFUNDABLE(409)／19 RATE_LIMITED(429)／401 UNAUTHORIZED／403 FORBIDDEN／1001 NOT_READY／1002 METHOD_NOT_ALLOWED／1003 NOT_FOUND（路由级）／1500 INTERNAL | `internal/httpapi/server.go` 常量 |
| 鉴权 | `Authorization: Bearer <accessToken>`；OpenAPI 全局 `bearerAuth`，公开接口逐条 `security: []` | `api/openapi.yaml`、`internal/auth` |
| 管理端权限 | 读：`AUDITOR` 亦可；写：`OPERATOR`/`SUPER_ADMIN`（`RequireAdminWrite`）；普通 USER 不得进入 `/admin/*` | `internal/auth` 中间件 + 各 `Register` 包装 |
| 请求 ID | 客户端可传 `X-Request-ID`，服务端回显并有日志字段；已在 B-06 打通 | `internal/httpapi`、`cmd/api/observability.go` |
| 分页 | 请求 `page`/`pageSize`（≤100）；响应 `data.items[]` + `data.meta{page,pageSize,total}` | `components/parameters/{Page,PageSize}` |
| 金额 | 一律**整数分**（`amountCent`/`balanceCent`/`paidCents` 等），前端不做浮点运算 | `internal/wallet`、`internal/order` |
| 时间 | RFC3339、UTC（服务端 `time.Time` 序列化为 `2026-09-15T11:18:08Z`） | 各响应 DTO |
| 幂等 | 需要幂等的写操作必须带 `Idempotency-Key`（16..128 字符）；重复键**同请求**返回首次结果，**不同请求**返回 409/code 14 | `components/parameters/IdempotencyKey`、`internal/repository/postgres` |
| 设备网关令牌 | **只**出现在网关与 API 之间（`chargerGatewayBearer`）；前端不得持有、日志不得打印 | `api/openapi.yaml`、`deploy/README.md` |

## 2. 对照矩阵

状态含义：`MATCH` = 路径/方法/鉴权/信封一致，可进入联调；`BACKEND_CHANGE` = B 线需改；`FRONTEND_CHANGE` = 前端需改；`BLOCKED` = 依赖未实现能力（A-07/Agent）。

| 方法 | 路径（`servers=/api/v1`） | Go 注册与鉴权 | OpenAPI | 幂等 | 状态 |
| ---- | ------------------------- | ------------- | ------- | ---- | ---- |
| POST | `/auth/user/login` | `h.UserLogin`，公开 | ✅ | – | MATCH |
| POST | `/auth/user/sms/code` | `h.RequestSMSCode`，公开 | ✅ | – | MATCH |
| POST | `/auth/user/login/sms` | `h.SMSLogin`，公开 | ✅ | – | MATCH |
| POST | `/auth/admin/login` | `h.AdminLogin`，公开 | ✅ | – | MATCH |
| POST | `/auth/logout` | `h.Logout`，Bearer | ✅ | – | MATCH |
| GET | `/me` | `h.RequireIdentity(h.meRoutes)`，Bearer | ✅ | – | MATCH |
| DELETE | `/me` | 同上（同路由多方法） | ✅ | – | MATCH（注销语义见 A-01 遗留项：重试应返回 204） |
| GET | `/me/profile` | `h.RequireIdentity(h.Profile)` | ✅ | – | MATCH |
| PUT | `/me/profile` | 同上 | ✅ | – | MATCH |
| GET | `/stations` | `RequireIdentity(ListStations)` | ✅ | – | MATCH |
| GET | `/stations/{stationId}` | `RequireIdentity(GetStation)` | ✅ | – | MATCH |
| GET | `/stations/{stationId}/reviews` | `RequireIdentity(wall)` | ✅ | – | MATCH |
| GET | `/chargers` | `RequireIdentity(ListChargers)` | ✅ | – | MATCH |
| GET | `/wallet` | `RequireIdentity(view)` | ✅ | – | MATCH |
| POST | `/wallet/top-up` | `RequireIdentity(topUp)` | ✅ | ✅ | MATCH（金额 1..1000000 分） |
| GET | `/wallet/transactions` | `RequireIdentity(transactions)` | ✅ | – | MATCH（分页 + `type` 过滤） |
| POST | `/orders` | `RequireRole(RoleUser, orders)` | ✅ | ✅ | MATCH |
| GET | `/orders` | 同上 | ✅ | – | MATCH（分页 + 状态/站点过滤） |
| GET | `/orders/{orderNo}` | `RequireRole(RoleUser, getOrder)` | ✅ | – | MATCH |
| POST | `/orders/{orderNo}/start` | `RequireRole(RoleUser, startCharging)` | ✅ | ✅ | MATCH |
| POST | `/orders/{orderNo}/stop` | `RequireRole(RoleUser, stopCharging)` | ✅ | ✅ | MATCH |
| POST | `/orders/{orderNo}/cancel` | `RequireRole(RoleUser, cancelOrder)` | ✅ | ✅ | MATCH |
| GET | `/orders/{orderNo}/review` | `RequireIdentity(reviewRoutes)` | ✅ | – | MATCH |
| POST | `/orders/{orderNo}/review` | 同上 | ✅ | – | MATCH |
| POST | `/orders/{orderNo}/appeal` | `RequireIdentity(createAppeal)` | ✅ | – | MATCH |
| GET | `/admin/users` | `RequireRole(RoleAdmin, listUsers)` | ✅ | – | MATCH |
| GET | `/admin/users/{userId}` | `RequireRole(RoleAdmin, userDetail)` | ✅ | – | MATCH |
| POST | `/admin/users/{userId}/freeze` | `RequireAdminWrite(freezeUser)` | ✅ | – | MATCH |
| POST | `/admin/users/{userId}/unfreeze` | `RequireAdminWrite(unfreezeUser)` | ✅ | – | MATCH |
| GET | `/admin/users/{userId}/transactions` | `RequireRole(RoleAdmin, userLedger)` | ✅ | – | MATCH |
| GET | `/admin/orders` | `RequireRole(RoleAdmin, listOrders)` | ✅ | – | MATCH |
| POST | `/admin/orders/{orderNo}/refund` | `RequireAdminWrite(refundOrder)` | ✅ | ✅ | MATCH（订单不存在→404/code 4；已退款→409/code 18） |
| GET | `/admin/stations` | `RequireRole(RoleAdmin, stations)` | ✅ | – | MATCH |
| POST | `/admin/stations` | 同上（同路由多方法） | ✅ | ✅ | MATCH |
| GET | `/admin/chargers` | `RequireRole(RoleAdmin, listChargers)` | ✅ | – | MATCH |
| GET | `/admin/chargers/{chargerId}/tariff` | `h.tariffRoutes` | ✅ | – | MATCH |
| PUT | `/admin/chargers/{chargerId}/tariff` | 同上 | ✅ | – | MATCH |
| POST | `/admin/chargers/{chargerId}/release` | `RequireAdminWrite(forceRelease)` | ✅ | ✅ | MATCH |
| POST | `/admin/chargers/{chargerId}/restart` | `RequireAdminWrite(restartCharger)` | ✅ | ✅ | MATCH |
| GET | `/admin/appeals` | `RequireRole(RoleAdmin, adminListAppeals)` | ✅ | – | MATCH |
| POST | `/admin/appeals/{appealId}/approve` | `RequireAdminWrite(adminApprove)` | ✅ | – | MATCH |
| GET | `/admin/audit` | `RequireRole(RoleAdmin, listAudit)` | ✅ | – | MATCH |
| POST | `/internal/charger-events` | `order.ChargerEventPath` + 网关令牌校验 | ✅（`security: chargerGatewayBearer`） | – | MATCH（**仅网关调用，前端不得调用**） |
| GET | `/healthz` | **根路径** `/healthz`（`internal/httpapi/server.go:84`） | ✅ 但挂在 `servers=/api/v1` 下 | – | **BACKEND_CHANGE** |
| GET | `/readyz` | **根路径** `/readyz`（同文件 85 行） | ✅ 同上 | – | **BACKEND_CHANGE** |
| – | `POST /api/v1/user/agent/chat` | 未实现、未登记 | ❌ | – | **BLOCKED**（A-05/A-07，不得伪装接通） |

**结论**：产品路由共 37 条，OpenAPI 共 45 个操作，除下面两处外一一对应；**没有"Go 有而 OpenAPI 没登记"的接口**，因此"前端依据未登记接口开发"的风险在后端侧为零。

## 3. 差异清单（需要处理的）

### D-1 `BACKEND_CHANGE`：`/healthz`、`/readyz` 的路径前缀不一致 —— **已修**

- 事实：OpenAPI 全局 `servers: [{url: /api/v1}]`，而这两个操作没有 `servers` 覆盖 → 按规格解析出的地址是 `/api/v1/healthz`、`/api/v1/readyz`；Go 实际只在**根路径**提供，且部署口径（nginx、systemd、探针）用的也是根路径。
- 影响：按规格生成的客户端会去调 `/api/v1/healthz` 并拿到 404。
- 处理（已实施并验证）：给这两个操作加 `servers: [{url: /}]`，解析结果回到 `/healthz`、`/readyz`，与 `internal/httpapi/server.go:84-85` 一致；`/metrics` 不登记 OpenAPI（B-06 口径，运维端点不暴露给前端）。

### D-2 `BLOCKED`：Agent 对话接口

- `agent/` 需要 `POST /api/v1/user/agent/chat`，Go 未实现也未登记。按任务文档 A-05：A 线只做请求层与降级 UI，**不得标记为已接通**；B 线在 A-07 或独立 Agent 模块审批后再登记 OpenAPI 并实现。

### D-3 待 A 线清单到位后比对（不预设结论）

A-01 的 `docs/integration/frontend-api-inventory.md` 尚未提交（PR #42 的前端 Web 代码不在本仓库任何 ref 中：`develop` 的 `apps/user`、`apps/admin` 目前仍是 C++ Qt 应用）。拿到清单后逐项回填矩阵的"前端调用点"列，并可能产生新的 `FRONTEND_CHANGE`（例如前端仍在用 C++/SQLite 的路径与响应格式）。

## 4. 本轮已完成的 B 线开工项

### 4.1 `local-stack.sh --seed` 迁移顺序（任务文档 §9 指定）

修复前：seed 在迁移门禁**之前**执行，对一次性空库直接失败——

```text
=== seed development data into ncs_fe_repro ===
psql:.../dev_seed.sql:14: ERROR:  relation "admin_accounts" does not exist
psql:.../dev_seed.sql:18: ERROR:  relation "user_accounts" does not exist
psql:.../dev_seed.sql:25: ERROR:  relation "wallet_accounts" does not exist
psql:.../dev_seed.sql:29: ERROR:  relation "stations" does not exist
```

修复后（顺序调整 + `psql -v ON_ERROR_STOP=1` 让半成功也暴露）：

```text
=== migration gate ===
level=INFO msg="database migrations applied" count=9
=== seed development data into ncs_fe_repro ===
=== start mock gateway, API, publisher and worker ===
=== smoke ===
login ok as the seeded user, 1 station(s) visible
=== local stack is up ===
```

### 4.2 迁移门禁缺少网关令牌（同一次修复中暴露）

`common_env` 没有 `NCS_CHARGER_GATEWAY_TOKEN`，而 `-migrate-only` 跑的就是 API 二进制、启动即校验配置 → 空库上迁移门禁直接失败（`NCS_CHARGER_GATEWAY_TOKEN is required`），脚本第 20 行文档承诺的默认值从未生效。已把令牌并入 `common_env`，由上面的实测结果确认。

## 5. 本轮实测（B-03 部署与 Nginx / B-04 WSL 本地栈）

一次性库 `ncs_fe_nginx` 从空库起栈，命令：

```bash
NCS_POSTGRES_DSN=postgres://.../ncs_fe_nginx bash backend/scripts/local-stack.sh --seed --with-nginx
```

原始输出（节选）：

```text
=== migration gate ===        level=INFO msg="database migrations applied" count=9
=== seed development data into ncs_fe_nginx ===
=== start mock gateway, API, publisher and worker ===
=== smoke ===                 login ok as the seeded user, 1 station(s) visible
=== render and start nginx ===  nginx: configuration file ... syntax is ok
                                configuration file ... test is successful
                                nginx: static=200 /healthz via proxy=200
=== local stack is up ===
```

随后逐条核对 B-03 的验收项（经 Nginx，自签证书）：

| 验收项 | 实测 |
| ------ | ---- |
| H5 静态文件目录 | `GET / -> 200` |
| HTTP → HTTPS 301 | `GET http://localhost:8124/ -> 301 Location=https://localhost/` |
| `/healthz`、`/readyz` 经反代 | 均 `200` |
| `/api/` 反向代理 | 经 Nginx 登录成功（token 43 字符），`/api/v1/stations`、`/api/v1/wallet`、`/api/v1/orders` 均 `200` |
| TLS 自签本地验证 | 全程 `curl -k` 通过（证书由 `local-stack.sh` 现场生成） |
| 请求 ID 透传 | 发 `X-Request-ID: fedcba9876543210`，响应回显 `X-Request-Id: fedcba9876543210` |
| 设备回执来源限制 | 渲染出的 `location = /api/v1/internal/charger-events` 为 `allow 127.0.0.1; deny all;` |
| metrics/readyz 内网限制 | `location = /metrics`、`location = /readyz` 同样 `allow 127.0.0.1; deny all;`（本机回环属允许段故为 200；拒绝路径由 `nginx-render.sh --drill` 证明） |
| 不向前端暴露设备网关令牌 | `dev-gateway-token` 未出现在渲染配置、nginx 访问/错误日志、API/Worker 日志中（`grep` 计数 0） |

清理（任务文档 B-04：测试库必须一次性并在结束后删除）：

```text
一次性库 ncs_fe_repro、ncs_fe_nginx → 已 DROP（pg_database 查询结果为 none）
8080/8123/8124 端口 → 已释放        栈进程 → 无残留
（/tmp/ncs-stack-*/ 保留日志作为证据）
```

## 6. 下一步（B 线）

1. ~~**D-1**：给 OpenAPI 的两个探针操作加 `servers` 覆盖~~ **已完成**（见 D-1），并已核对解析结果；后续把这条解析检查纳入规格门禁脚本。
2. **B-05 联调脚本**：把"创建/清理一次性库 + 迁移 + seed + 起栈 + 生成 token + 核心 smoke + 检查日志是否泄露密钥 + 停止清理"做成一条可重复执行的入口（现在 `local-stack.sh` 需要外部提供 DSN，且停留在前台）。
3. **B-02 核心接口确认**：为 18 个核心接口逐项补齐 handler/错误/权限/真实 PG·Redis 测试与 OpenAPI 对照记录（多数已有测试，本项工作是**逐项登记与补齐缺口**，不是重写）。
4. **B-03 部署与 Nginx**：H5 静态目录、`/api/` 反代、301、自签 TLS、`/healthz`、`/readyz`、`/metrics`、请求 ID 透传、回执来源限制、metrics/readyz 内网限制、令牌不外泄。
5. **B-06 质量门禁**：`go build/vet/test/race`、真实 PG/Redis 测试、OpenAPI YAML 校验、`nginx -t`、`git diff --check`、无密钥与构建产物入库。
