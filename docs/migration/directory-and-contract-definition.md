# 双线迁移：目录、接口与审批基线

## 1. 目标

本目录定义将现有 C++/Qt/Crow/SQLite 项目逐步迁移为：

```text
H5 + Nginx + Go + Redis Streams + Redis + PostgreSQL
```

迁移采用绞杀式路线。现有 `core/`、`apps/`、`server/`、`infrastructure/sqlite/` 和 `ml/` 在新系统完成 P0 闭环前保持可编译、可启动、可回退。

## 2. 双线职责

### A 线：Go 后端与数据基础设施

负责：

- `backend/` 下的 Go API、Worker、领域服务和仓储实现；
- `backend/migrations/` 下的 PostgreSQL 迁移；
- Redis 缓存、会话、幂等键和分布式锁；
- Redis Streams 发布、消费、重试、Pending 和死信；
- 认证、站点、充电桩、订单、钱包、管理端 API；
- 后端单元测试、集成测试、并发和故障恢复测试。

A 线不得修改 `web/`、`nginx/` 和现有 C++ 业务目录。

### B 线：H5 与 Nginx

负责：

- `web/` 下的 Vue 3 + TypeScript + Vite H5；
- 用户端和管理端页面、路由、状态、请求封装和 Mock；
- `nginx/` 下的静态资源、API 反向代理和发布配置；
- 前端 lint、类型检查、构建和浏览器端测试。

B 线不得修改 `backend/` 和 `backend/migrations/`。

### 共享区域

以下文件由集成人员维护，A/B 只提变更建议，不直接覆盖：

- `api/openapi.yaml`；
- `docs/migration/`；
- `.github/workflows/`；
- 根目录部署入口和发布脚本；
- `README.md`。

## 3. 目录边界

```text
api/
└── openapi.yaml                 # P0 HTTP 契约，先冻结后实现

backend/
├── cmd/api/                     # HTTP API 进程
├── cmd/worker/                  # Stream Worker 进程
├── internal/config/             # 配置与环境变量
├── internal/http/               # 路由、中间件、响应封装
├── internal/auth/               # 用户/管理员认证与会话
├── internal/station/            # 站点与充电桩
├── internal/order/              # 订单状态机、幂等与结算
├── internal/wallet/              # 钱包和账务
├── internal/admin/               # 管理端业务
├── internal/event/               # Outbox、Streams、重试与死信
├── internal/repository/          # PostgreSQL/Redis 访问
└── migrations/                   # 递增、可重复验证的 SQL

web/
├── src/api/                     # 仅通过这里访问后端
├── src/router/                  # 用户端/管理端路由
├── src/stores/                  # 登录态与页面状态
├── src/layouts/                 # 用户端/管理端布局
├── src/views/user/              # 用户端页面
├── src/views/admin/             # 管理端页面
├── src/components/              # 可复用组件
├── src/mocks/                   # 契约驱动的 Mock
└── src/styles/                  # 轻量样式和移动端适配

nginx/
├── nginx.conf                   # 本地/生产入口
└── conf.d/                      # API、静态资源和实时接口代理

docs/migration/
├── directory-and-contract-definition.md
├── module-approval-template.md
└── approval-log.md               # 每个模块的审批记录
```

## 4. 分支和工作区

当前 WSL 主工作区：

```text
/home/penty/projects/charging-station-platform
```

建议后续使用两个独立 worktree：

```text
/home/penty/projects/charging-station-platform-backend
/home/penty/projects/charging-station-platform-h5
```

集成分支：

```text
codex/migration-integration
```

模块分支格式：

```text
codex/migration/a-<序号>-<模块名>
codex/migration/b-<序号>-<模块名>
codex/migration/i-<序号>-<模块名>     # 跨线模块（BE-I-*），由集成人员指定基线
```

跨线模块（编号 `BE-I-*`）同时改动 A 线与 B 线的实现，因此不归属任何单一开发线：它从**集成分支的当前顶端**
拉出，其目录所有权在模块审批单里逐项列明，并且只在 A、B 两条线的相关模块全部合入后才开工。

模块分支合并顺序：

```text
模块分支 → 我审批 → codex/migration-integration → 阶段 PR → develop
```

## 5. P0 接口范围

第一阶段只冻结以下业务闭环：

```text
登录 → 查询站点 → 查询充电桩 → 创建订单 → 开始充电
→ 接收充电事件 → 停止充电 → 完成订单 → 查询历史订单
```

管理端 P0 包含：

- 管理员登录；
- 站点和充电桩查询；
- 用户查询；
- 订单查询；
- 设备命令下发；
- 基础运营统计。

地图、评价墙、复杂大屏和 ML 预测作为 P1/P2，不阻塞 P0 切换。

## 6. 通用接口规则

- 所有接口前缀为 `/api/v1`；
- 时间使用 UTC RFC3339 或 Unix 秒，不能混用本地时间；
- 金额使用整数分；
- 写操作支持 `Idempotency-Key`；
- 分页使用 `page` 和 `pageSize`，默认 20，最大 100；
- 401 表示登录态无效，403 表示无权限，409 表示并发冲突，503 表示依赖未就绪；
- API 不向前端暴露 PostgreSQL、Redis 或内部事件字段；
- 订单状态只能由后端状态机推进，前端不能指定最终状态；
- Redis 不作为业务最终数据源；
- 业务事务先写 PostgreSQL 和 Outbox，再由 Publisher 发布 Redis Stream；
- Stream 消费必须支持 ACK、重试、Pending 恢复、幂等和死信。

## 7. Redis 命名基线

缓存和会话：

```text
ncs:session:{session_id}
ncs:auth:rate-limit:{identity}
ncs:station:{station_id}
ncs:charger:{charger_id}
ncs:lock:order:{order_id}
ncs:idempotency:{scope}:{key}
```

Streams：

```text
ncs:stream:order-event
ncs:stream:charge-event
ncs:stream:charger-command
ncs:stream:notification
ncs:stream:dead-letter
```

浏览器不得直连 Redis。H5 只访问 Go API；首版实时状态使用短轮询，后续可增加 Go 提供的 SSE。

## 8. 模块审批门槛

每个模块必须提交：

- 变更文件列表；
- API 或数据库变化；
- 测试命令和完整输出；
- 日志、错误和回滚说明；
- 与另一条线的依赖说明。

A 线最低验证：

```bash
go test ./...
go vet ./...
go test -race ./...
```

B 线最低验证：

```bash
npm ci
npm run lint
npm run typecheck
npm run build
```

集成验证：

```bash
nginx -t
curl http://localhost/healthz
curl http://localhost/api/v1/healthz
PostgreSQL 空库迁移
Redis Streams 重试和幂等测试
```

审批结果只有：

```text
APPROVED
CHANGES_REQUIRED
BLOCKED
```

未获得 `APPROVED`，不能开始该线的下一模块。
