# 前后端完整联调任务分工文档

## 1. 文档目的

本任务用于将前端 PR #42 的 Web 用户端、管理端和 Agent 能力，逐步接入当前 Go 后端，并在 WSL 完成真实联调。

目标不是直接把 PR #42 原样合入，而是先完成接口、认证、数据模型和部署方式的适配，再进行合并审批。

## 2. 当前基线

- 后端基线：develop，当前已包含 A-01～A-06、B-01～B-07 和 B-06 部署能力
- 后端技术栈：Go、PostgreSQL、Redis、Redis Streams、Nginx
- 运行环境：WSL
- 后端迁移版本：0001～0009
- 前端 PR：#42
- PR #42 当前包含：
  - apps/user：用户端 Web
  - apps/admin：管理端 Web
  - agent：AI 出行助手
  - C++ 服务端 PostgreSQL v10 改造
  - SQLite 到 PostgreSQL 的迁移工具
- PR #42 明确不包含 Go 后端和 Redis Streams 适配，因此不能直接视为 Go 后端联调完成。

## 3. 两条并行线路

### A 线：前端 Web 与 Agent 适配

分支：

`codex/frontend/go-api-adapter`

负责人目标：让前端只依赖 Go API，不再依赖 C++ Controller、SQLite 仓储或 C++ 专用响应格式。

允许修改：

- `apps/user/`
- `apps/admin/`
- `apps/shared/`
- `agent/`
- 前端专用环境变量模板和测试

禁止修改：

- `backend/`
- `api/openapi.yaml`
- `nginx/`
- 数据库迁移文件
- Go 后端接口实现

#### A-01 接口盘点

输出文件：

`docs/integration/frontend-api-inventory.md`

逐项记录：

- 调用文件和函数
- HTTP 方法
- 请求路径
- 请求头
- 请求体
- 响应字段
- 错误处理
- 是否依赖 C++/SQLite
- 对应 Go OpenAPI 路径
- 当前状态：已匹配、需改造、后端缺失

验收：

- 用户端、管理端、Agent 的 HTTP 请求全部登记；
- 不允许存在未登记的网络请求；
- 不允许通过源码猜测接口而不记录证据。

#### A-02 API 客户端统一

建立统一请求层，统一处理：

- Go API 基地址；
- `Authorization: Bearer <accessToken>`；
- `Content-Type: application/json`；
- `X-Request-ID`；
- 响应 `success/code/message/data`；
- 401 会话过期；
- 403 权限不足；
- 404 资源不存在；
- 409 业务冲突；
- 429 限流；
- 503 依赖不可用。

禁止在页面组件内重复实现 token、错误码和分页解析。

#### A-03 用户端接入顺序

按以下顺序接通：

1. 登录与会话恢复；
2. 用户资料、冻结状态和注销状态展示；
3. 站点列表与充电桩查询；
4. 钱包余额、充值、流水；
5. 订单创建；
6. START/STOP 充电；
7. 订单状态刷新；
8. 评价；
9. 申诉；
10. 个人页面和异常状态处理。

每完成一项，必须提交：

- 页面截图或录屏；
- 浏览器 Network 请求证据；
- 成功响应样例；
- 失败响应样例；
- 对应自动化测试。

#### A-04 管理端接入顺序

1. 管理员登录；
2. 站点、充电桩和订单列表；
3. 用户详情；
4. 用户冻结/解冻；
5. 费率查询与修改；
6. 强制释放；
7. 用户账务查询；
8. 申诉队列；
9. 申诉审核；
10. 审计查询。

权限验收：

- AUDITOR 只能读；
- OPERATOR 可执行运营写操作；
- 普通 USER 不得访问管理端；
- 前端不得保存数据库密码、设备网关令牌或 Prometheus 凭据。

#### A-05 Agent 适配

当前 Go 后端尚未提供 `POST /api/v1/user/agent/chat`，因此 A 线只完成：

- Agent 请求层抽象；
- 工具调用模型；
- loading、超时、错误和降级 UI；
- 后端地址和鉴权接入；
- 未实现接口的明确提示。

Agent 真正联调需等待 A-07 或单独 Agent 后端模块审批，不得伪装成已接通。

#### A-06 前端质量门禁

必须通过：

- `npm install`；
- `npm run build`；
- 前端单元测试；
- ESLint/TypeScript 检查；
- 无 C++/SQLite 运行时依赖；
- 无硬编码生产域名、密码、token；
- 浏览器控制台无未处理异常；
- 移动端宽度和桌面端宽度均可用。

### B 线：Go API、Nginx 与 WSL 联调

分支：

`codex/backend/frontend-integration`

负责人目标：保证 Go API、OpenAPI、Nginx 和本地全栈环境能够稳定承接 A 线前端。

允许修改：

- `backend/`
- `api/openapi.yaml`
- `nginx/`
- `docs/integration/`
- 联调脚本和测试数据

禁止修改：

- `apps/user/`
- `apps/admin/`
- `apps/shared/`
- `agent/`

#### B-01 OpenAPI 对照

根据 A 线的接口盘点表逐项核对：

- 路径；
- 方法；
- path/query 参数；
- request body；
- response envelope；
- 错误码；
- 分页格式；
- 时间格式；
- 金额单位；
- 权限要求；
- 幂等要求。

缺少接口时，先登记 OpenAPI，再实现 Go 路由，最后补测试。

不得让前端依据未登记接口开发。

#### B-02 核心接口确认

必须确认以下接口在 Go 中真实可用：

- 登录和会话；
- 用户资料；
- 站点与充电桩；
- 钱包余额；
- 充值；
- 钱包流水；
- 订单创建；
- START；
- STOP；
- 订单查询；
- 评价；
- 申诉；
- 管理端用户详情；
- 冻结/解冻；
- 费率；
- 强制释放；
- 审计；
- 申诉审核。

对每个接口至少有：

- Go handler 测试；
- 错误响应测试；
- 权限测试；
- 真实 PostgreSQL 或 Redis 集成测试；
- OpenAPI 对照记录。

#### B-03 部署和 Nginx

完成并验证：

- H5 静态文件目录；
- `/api/` 反向代理；
- HTTP 到 HTTPS 301；
- TLS 自签证书本地验证；
- `/healthz`；
- `/readyz`；
- `/metrics`；
- 请求 ID 透传；
- 设备回执来源限制；
- metrics/readyz 内网限制；
- 不向前端暴露设备网关令牌。

#### B-04 WSL 本地栈

使用一次性 PostgreSQL 数据库和独立 Redis DB：

启动顺序：

1. PostgreSQL；
2. Redis；
3. migration gate；
4. Go API；
5. Mock Gateway；
6. Outbox Publisher；
7. Worker；
8. Nginx；
9. H5。

必须验证：

- 0001～0009 迁移；
- 数据库版本门禁；
- Redis 必需模式；
- API readyz；
- Worker/Pubisher metrics；
- Nginx 配置；
- H5 静态页面；
- API 反代；
- 登录和核心业务链路。

测试数据库必须是一次性库，测试结束后删除。

#### B-05 真实联调脚本

B 线提供以下联调入口：

- 启动全栈；
- 创建和清理测试库；
- 加载测试种子；
- 检查 API/Redis/PG；
- 检查 Nginx；
- 生成登录 token；
- 输出核心 API smoke test；
- 检查日志是否泄露密钥；
- 停止并清理全部进程。

注意：seed 必须在迁移完成后执行。任何脚本不得把 shell 的 `$(date)` 展开依赖交给 systemd。

#### B-06 后端质量门禁

必须通过：

- `go build ./...`；
- `go vet ./...`；
- `go test -count=1 ./...`；
- `go test -count=1 -race ./...`；
- 真实 PostgreSQL 测试；
- 真实 Redis 测试；
- OpenAPI YAML 校验；
- Nginx `nginx -t`；
- `git diff --check`；
- 无密钥、token、env 和构建产物进入 Git。

## 4. 两线交接规则

### 第一阶段：接口盘点

A 线先提交：

`docs/integration/frontend-api-inventory.md`

B 线基于该文件完成接口映射：

`docs/integration/frontend-go-api-matrix.md`

### 第二阶段：契约冻结

B 线完成 OpenAPI 对照后，标记每个接口：

- MATCH：前后端一致；
- FRONTEND_CHANGE：前端需要调整；
- BACKEND_CHANGE：后端需要补齐；
- BLOCKED：依赖 A-07 或外部系统。

只有 MATCH 项才能进入页面联调。

### 第三阶段：按模块接通

每完成一个模块，A/B 两线共同提交联调证据：

- 请求；
- 响应；
- 状态码；
- 数据库变化；
- Redis/Stream 变化；
- 页面结果；
- 失败场景。

### 第四阶段：审批

我按模块审批：

1. 认证；
2. 站点/充电桩；
3. 钱包；
4. 订单；
5. START/STOP；
6. 评价/申诉；
7. 管理端；
8. Nginx/H5；
9. 全量回归。

一个模块未通过，不进入下一个模块的正式合并。

## 5. 推荐并行节奏

| 周期 | A 线 | B 线 |
|---|---|---|
| 第 1 阶段 | 输出前端请求清单 | 输出 Go 路由和 OpenAPI 对照 |
| 第 2 阶段 | 统一 API 客户端和认证 | 修复契约缺口、补 OpenAPI |
| 第 3 阶段 | 用户端接入认证/站点/钱包 | 提供 WSL 全栈和测试数据 |
| 第 4 阶段 | 用户端接入订单/启停 | 验证 Mock Gateway、Worker、Publisher |
| 第 5 阶段 | 管理端接入 | 验证权限、审计、申诉事务 |
| 第 6 阶段 | Agent 适配层 | 评估 A-07 Agent 后端接口 |
| 第 7 阶段 | 浏览器全链路测试 | 后端全量回归和 Nginx 验证 |

## 6. 完整联调验收场景

### 用户链路

1. 用户登录；
2. 查看站点；
3. 查看充电桩；
4. 创建订单；
5. 发起 START；
6. Mock Gateway 返回开始回执；
7. Worker 应用订单状态；
8. 发起 STOP；
9. Mock Gateway 返回停止回执；
10. Worker 完成订单和结算；
11. 查看钱包和流水；
12. 提交评价；
13. 提交申诉。

### 管理链路

1. 管理员登录；
2. 查询用户；
3. 冻结/解冻用户；
4. 查询订单；
5. 查询费率；
6. 修改费率；
7. 强制释放订单；
8. 查询申诉；
9. 审核申诉；
10. 验证退款、订单状态、充电桩状态和审计记录。

### 故障链路

1. Redis 不可用；
2. PostgreSQL 不可用；
3. Worker 重启；
4. Publisher 重启；
5. 重复提交充值；
6. 重复提交 START/STOP；
7. 重复审核申诉；
8. Nginx 访问 metrics/readyz；
9. 未授权设备回执；
10. 前端收到 401、403、404、409、503。

## 7. 合并顺序

1. B 线先合入接口契约和联调脚本；
2. A 线合入 API 客户端和认证适配；
3. B 线确认用户端接口；
4. A 线合入用户端页面；
5. B 线确认订单/钱包/设备闭环；
6. A 线合入管理端；
7. B 线确认管理端权限和事务；
8. A 线提交浏览器全链路证据；
9. B 线执行最终全量回归；
10. 集成人员审批后合并 PR #42。

## 8. 明确禁止事项

- 不得把 PR #42 的 C++ PostgreSQL v10 迁移直接合入 Go develop；
- 不得让前端直接连接 PostgreSQL 或 Redis；
- 不得让前端持有设备网关令牌；
- 不得通过修改 Go API 响应去迁就未登记的前端接口；
- 不得跳过 OpenAPI 登记；
- 不得把测试库当生产库；
- 不得把 Agent 未实现接口标记为已完成；
- 不得在未通过审批前合并下一模块。

## 9. 当前开工任务

A 线立即开始：

- 盘点 `apps/user`、`apps/admin`、`agent` 的全部 HTTP 请求；
- 提交 `frontend-api-inventory.md`；
- 统一前端 API 客户端和 Bearer token 处理。

B 线立即开始：

- 以远端 `develop` 为基线；
- 建立 `frontend-go-api-matrix.md`；
- 核对当前 Go OpenAPI 与前端请求；
- 修复 `local-stack.sh --seed` 的迁移顺序；
- 准备可重复执行的 WSL 联调脚本。

## 10. 交付标准

只有同时满足以下条件，才允许宣布“前后端完整联调完成”：

- 所有前端网络请求均有 OpenAPI 对应；
- 所有核心页面均使用 Go API；
- 无 C++/SQLite 运行时依赖；
- 用户端和管理端核心流程真实跑通；
- START/STOP 设备闭环跑通；
- 钱包和退款账务一致；
- Nginx/H5 通路通过；
- 后端全量测试和 race 测试通过；
- 关键失败场景通过；
- 未实现的 Agent/A-07 能力已明确标记；
- 我完成最终审批。
