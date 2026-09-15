# Go 后端部署（B-06）

本目录是 Go 后端的部署与运维交付：进程启动方式、Nginx 站点、密钥注入、迁移门禁、指标抓取、
备份恢复和故障演练。运行前的架构背景见 `docs/backend-architecture.md`，契约见 `api/openapi.yaml`。

## 交付内容

| 路径 | 用途 |
|---|---|
| `systemd/ncs-migrate.service` | 迁移门禁（一次性）：应用 `0001..0007` 并核对版本后退出 |
| `systemd/ncs-{api,worker,outbox-publisher}.service` | 三个常驻进程；均 `Requires=ncs-migrate.service` |
| `systemd/ncs-backend.env.example` | 运行环境变量模板（安装为 `/etc/ncs/backend.env`，`chmod 0600`） |
| `Dockerfile` | 四个可执行文件共用的镜像（`--build-arg NCS_TARGET=cmd/api`） |
| `docker-compose.yml` | 可选的一体化编排（含迁移门禁与 mock-gateway profile） |
| `../scripts/local-stack.sh` | 本地联调：一条命令起全栈（可选带 Nginx） |
| `../scripts/nginx-render.sh` | 渲染 Nginx 模板并校验；`--drill` 在本机跑通一遍 |
| `../scripts/backup-postgres.sh` / `restore-postgres.sh` | 备份（自定义格式、30 天保留）与恢复 |
| `../scripts/drill-backup-restore.sh` | **RPO/RTO 实测演练**（必须实际运行，不能只写在文档里） |
| `../scripts/fault-drill.sh` | 依赖故障演练：切断 PostgreSQL/Redis，断言 `/readyz` 翻转与恢复 |
| `../scripts/loadtest-orders.sh` | 并发与重复提交压测（断言一次动作只产生一次效果） |
| `../scripts/verify-closed-loop.sh` | 四进程全链路验证（含 Worker 崩溃恢复、重复投递、Publisher 单活） |
| `../../nginx/ncs-api.conf.template` | Nginx 站点模板（静态、反代、回执来源限制、metrics/readyz 限制） |

## 启动顺序

```text
PostgreSQL / Redis
      ↓
ncs-migrate（一次性）：应用迁移并核对 schema 版本
      ↓
ncs-api（迁移已完成才启动；启动后 migration 仍会幂等地再跑一次）
      ↓
ncs-worker、ncs-outbox-publisher
```

worker 与 publisher **自己不会迁移**：它们启动时会核对 schema 版本，落后就拒绝启动并打印
"run the migration gate first"。这是刻意的——写设备结果的表由迁移引入，带着旧 schema 跑到业务
中途才失败，排查成本远高于启动即失败。

### systemd 安装

```bash
go build -o /usr/local/bin/ncs-api         ./cmd/api
go build -o /usr/local/bin/ncs-worker      ./cmd/worker
go build -o /usr/local/bin/ncs-outbox-publisher ./cmd/outbox-publisher
install -m 0644 backend/deploy/systemd/*.service /etc/systemd/system/
install -m 0640 backend/deploy/systemd/ncs-backend.env.example /etc/ncs/backend.env
chmod 0600 /etc/ncs/backend.env          # 内含设备网关令牌与数据库口令
useradd --system --no-create-home --shell /usr/sbin/nologin ncs
install -d -o ncs -g ncs /var/log/ncs
systemctl daemon-reload
systemctl enable --now ncs-migrate.service ncs-api.service ncs-worker.service ncs-outbox-publisher.service
```

### 容器

```bash
docker build -f backend/deploy/Dockerfile --build-arg NCS_TARGET=cmd/api -t ncs-api:$(git rev-parse --short HEAD) .
# 单进程运行
docker run --rm -e NCS_POSTGRES_DSN=... -e NCS_REDIS_ADDR=... -e NCS_CHARGER_GATEWAY_TOKEN=... ncs-api:dev
# 或者用编排（需要 docker compose 插件）
cd backend/deploy && cp systemd/ncs-backend.env.example .env && docker compose up -d
```

**注意**：验证 B-06 的这台机器上**没有安装 docker compose 插件**（`docker` 可用、`docker-compose`
与 `compose` 子命令不可用），因此 compose 路径只做了结构校验与文档说明，**没有作为验收证据**；
验收以 systemd 与脚本路径为准。使用 compose 前请确认 `docker compose version` 可用。

## 密钥与证书注入

- `NCS_CHARGER_GATEWAY_TOKEN`：设备网关服务令牌，**无默认值**，缺失时 API 拒绝启动；只注入设备
  网关与 Go API，**不得下发给 H5**。
- `NCS_POSTGRES_DSN` 中的口令：生产用密钥管理或受限权限文件（`/etc/ncs/backend.env`，0600）。
- TLS：`NCS_TLS_CERT` / `NCS_TLS_KEY` 由部署环境提供（生产证书），本地演练用
  `backend/scripts/nginx-render.sh --drill` 生成的自签证书。
- 日志中不得输出口令、完整令牌或支付数据；API/Worker/Publisher 的日志只记录 ID 与结果。

## 端口与域名（裁决口径）

```text
公网 HTTPS 443（NCS_HTTPS_PORT）→ Nginx → Go API 127.0.0.1:8080（NCS_API_UPSTREAM）
公网 HTTP  80 （NCS_HTTP_PORT）→ 仅 301 跳转到 HTTPS
H5 静态目录 NCS_STATIC_ROOT
/metrics、/readyz：仅内网/本机（Nginx allow/deny + API 只监听本机）
NCS_PUBLIC_HOST：真实域名由部署环境注入，模板与代码都不写死
```

`/metrics` **不登记 OpenAPI**（内部运维端点）；`/readyz` 保留在 OpenAPI 中但属系统运维接口，
H5 与普通 Agent 都不得调用，也不应把它当作业务健康判断。

## 指标与告警

`GET /metrics`（Prometheus 文本格式，`text/plain; version=0.0.4`）在 API 进程上暴露：

| 指标 | 含义 | 建议告警 |
|---|---|---|
| `ncs_dependency_up{dependency}` | 依赖是否应答（1/0） | `== 0` 持续 1 分钟 |
| `ncs_pg_migrations_version` | 已应用的最高迁移版本 | `< 7` 立即告警（部署不一致） |
| `ncs_pg_outbox_unpublished` | 未发布 Outbox 行数 | 持续增长（Publisher 停摆） |
| `ncs_api_requests_total{method,route,status}` | 按**路由模板**计数的请求 | 5xx 比例 |
| `ncs_api_request_duration_seconds`（histogram） | 请求耗时 | `histogram_quantile(0.95, ...)` |
| `ncs_api_requests_in_flight` | 正在处理的请求 | 长时间高位 |
| `ncs_worker_*`、`ncs_stream_*` | Worker 与 Streams（Worker 进程日志/metrics） | 死信、Pending 增长 |

抓取配置示例（Prometheus）：

```yaml
scrape_configs:
  - job_name: ncs-api
    scheme: http
    static_configs:
      - targets: ["10.99.0.11:8080"]     # 内网地址；公网不可达
  - job_name: ncs-worker
    static_configs:
      - targets: ["10.99.0.12:9101"]     # Worker 的指标入口（若已开放）
```

`ncs_pg_outbox_unpublished` 与 `ncs_pg_migrations_version` 只在 PostgreSQL 可达时更新；数据库不可用时
它们保留上次数值，**判断可用性要看 `ncs_dependency_up`**，不要用这两个 gauge 判断存活。

## 备份与恢复（已批准的基线）

| 项目 | 目标 |
|---|---|
| PostgreSQL 备份保留 | 30 天 |
| PostgreSQL RPO | ≤ 15 分钟 |
| PostgreSQL RTO | ≤ 60 分钟 |
| Redis | 可恢复基础设施，不保存唯一业务事实 |
| 账务与订单 | 必须依靠 PostgreSQL 恢复 |

```bash
# 每 15 分钟一次（RPO = 备份间隔），保留 30 天
NCS_POSTGRES_DSN=... backend/scripts/backup-postgres.sh
# systemd timer 示例
systemd-run --on-calendar="*:0/15" --unit=ncs-backup \
    env NCS_POSTGRES_DSN=... backend/scripts/backup-postgres.sh

# 恢复
backend/scripts/restore-postgres.sh --dump /var/backups/ncs/postgres/<dump> --dsn postgres://.../ncs_restore

# 实测演练（B-06 要求：必须跑一次，不能只写文档）
NCS_TEST_PG_DSN=... backend/scripts/drill-backup-restore.sh
```

RPO 的诚实边界：dump 间隔就是数据丢失上界（≤15 分钟满足目标）；需要更小的 RPO 必须启用 WAL
归档做 PITR，本部署**未启用**，演练报告也会把这条限制打印出来。恢复演练在同一实例上执行，测量的是
数据库工作量而不是跨主机拷贝，报告同样会说明。

## 故障演练

```bash
NCS_TEST_PG_DSN=... backend/scripts/fault-drill.sh      # 切断 PostgreSQL / Redis，断言 /readyz 翻转与恢复
NCS_TEST_PG_DSN=... backend/scripts/verify-closed-loop.sh   # Worker -9 后 Pending 恢复、重复投递、Publisher 单活
NCS_API_URL=... backend/scripts/loadtest-orders.sh      # 并发与重复提交（同一幂等键只产生一次效果）
```

`fault-drill.sh` 用 TCP 切断（本地转发代理）注入故障，而不是停服务：验证机没有 root，且 PostgreSQL
是共享的。从客户端视角，切断与网络分区等价——正是就绪策略要处理的故障形态。真实部署上用
`systemctl stop postgresql` 跑同样的断言。

## 回滚

- 新增文件（本目录、`nginx/ncs-api.conf.template`、脚本）删除即可回滚，不影响业务代码。
- `cmd/api` 的改动是增量：`/metrics` 路由、就绪探针、`-migrate-only` 开关；回退单个提交即恢复原行为
  （就绪重新由启动时的固定标志决定）。
- Nginx：切回上一份站点文件并 `systemctl reload nginx`。
- 数据库：B-06 不新增迁移，无需数据回滚。
