# Go 后端双线并行开发线路

## 1. 适用范围

本文档适用于充电桩平台 Go 新后端的独立建设。

本阶段明确：不迁移 C++ 代码，不做 C++ 接口兼容，不处理旧 SQLite 数据，不做历史数据迁移，不维护新旧系统双写或双读。新功能全部以 Go、PostgreSQL、Redis、Redis Streams 和 Nginx 为基础。H5 只通过 OpenAPI 调用 Go 后端。

目标架构：

    H5
      |
    Nginx
      |
    Go API
      |
    PostgreSQL
      |
    Redis / Redis Streams / Worker

## 2. 双线职责

### A 线：业务功能线

负责业务领域、订单、钱包、支付、用户、管理端和业务 API。

A 线可以修改：

- backend/internal/order/
- backend/internal/auth/
- backend/internal/station/
- backend/internal/admin/
- 新增的业务领域目录；
- 对应业务测试；
- 业务服务接口和 DTO。

A 线不得直接修改：

- Redis 底层实现；
- event 的公共事件语义；
- worker 的通用消费机制；
- nginx 生产配置；
- 已冻结的 OpenAPI；
- 其他模块已批准的数据库迁移。

### B 线：基础设施与集成线

负责 PostgreSQL、Redis、Streams、Outbox、Worker、部署、事件契约和集成测试。

B 线可以修改：

- backend/internal/repository/postgres/
- backend/internal/repository/redis/
- backend/internal/event/
- backend/internal/worker/
- backend/cmd/worker/
- backend/cmd/outbox-publisher/
- backend/cmd/mock-gateway/
- backend/migrations/
- nginx/
- 集成测试和联调脚本。

B 线不得：

- 重复实现订单状态机；
- 复制 A 线领域服务；
- 绕过领域服务直接写订单；
- 擅自改变已冻结 OpenAPI；
- 让 Redis 成为订单最终事实源。

## 3. 公共契约

两条线只能通过以下契约协作：

1. HTTP API：api/openapi.yaml；
2. PostgreSQL 表结构和迁移；
3. Go 领域接口；
4. Redis Stream 事件信封；
5. 统一错误码；
6. 幂等键规则；
7. 审批文档。

契约变更必须经过：提出变更、说明影响、更新文档、增加契约测试、两线确认、修改实现。

未经确认，任何一条线不得单方面修改公共接口。

## 4. A 线模块

### A-01：认证和用户账户

负责短信登录、密码登录、管理员登录、会话、用户资料、账号冻结、注销账号、角色和权限。

依赖：B-01 Redis 会话基础。

验收：用户和管理员认证隔离；Bearer 会话可校验和注销；权限错误使用统一响应；Redis 故障行为符合降级策略。

### A-02：站点和充电桩

负责站点查询、站点详情、充电桩查询、站点状态、充电桩状态、筛选和分页。

依赖：B-02 PostgreSQL 基础仓储。

验收：查询结果稳定；分页和筛选契约明确；不暴露数据库字段；查询不修改业务状态。

### A-03：订单和充电流程

负责创建订单、查询订单、取消订单、开始充电、停止充电、订单状态机、设备回执后的状态推进和账单生成。

依赖：A-02、B-02、B-03、B-04、B-05。

验收状态链路：

    CREATED
      -> STARTING
      -> CHARGING
      -> STOPPING
      -> COMPLETED

必须通过状态机、幂等、并发和设备回执测试。

### A-04：钱包、账务和支付

负责钱包账户、充值、消费流水、订单扣款、欠费、部分支付、退款和结算查询。

依赖：A-03、B-02、B-03。

验收：金额统一使用分；订单、钱包、账务流水在同一事务中提交；重复请求不能重复扣款；余额不足结果正确；流水可审计。

### A-05：评价、申诉和售后

负责订单评价、充电桩评价、评价修改、订单申诉、管理员审核和售后记录。

依赖：A-03、A-06。

验收：只有符合状态的订单才能评价；同一订单评价幂等；申诉状态由后端控制；管理员操作有审计记录。

### A-06：管理端业务

负责站点管理、充电桩管理、用户管理、订单管理、费率管理、价格调整、强制释放、设备命令管理和审计日志。

依赖：A-01、A-02、A-03、A-04。

验收：管理员和只读审计角色分离；高风险操作有审计；批量操作支持幂等和部分失败；写操作具备事务边界。

### A-07：扩展业务

负责导航、路线规划、统计、Dashboard、ML 任务接口和 WebSocket 状态推送。

依赖：前置业务模块全部完成。该模块最后开发，不阻塞 P0 闭环。

## 5. B 线模块

### B-01：Go API 和公共配置

负责 API 进程入口、配置加载、健康检查、统一响应、统一错误码、trace ID 和运行参数校验。

验收：配置缺失时安全失败；healthz 和 readyz 行为明确；响应结构统一；日志不泄露密钥。

### B-02：PostgreSQL 数据基础

负责连接池、事务封装、迁移执行器、领域仓储、Outbox、幂等记录、消费记录和账务事务支持。

验收：迁移顺序正确且可重复执行；事务提交和回滚正确；连接池和超时可配置；生产库不会误执行开发种子。

### B-03：Redis 和 Redis Streams

负责 Redis 连接池、会话、缓存、限流、分布式锁、Streams 消费组、Pending、ACK、幂等快速路径和降级策略。

验收：Redis 故障行为正确；不谎报锁已取得；消费组可恢复；重复消息不会造成重复业务写入。

### B-04：Outbox Publisher

负责读取 PostgreSQL Outbox、发布 Redis Streams、advisory lock 单活、成功标记、失败保留和发布日志。

核心链路：

    业务事务提交
      -> Outbox 存在
      -> Publisher 发布
      -> Stream 消费

必须验证 Publisher 重启、失去锁和重复发布。

### B-05：Worker 和设备事件

负责设备命令消费、设备网关调用、START/STOP 回执、ACK、Pending 恢复、重试、死信、命令结果和 Applier 接线。

验收：Worker 重启不丢消息；失败按预算重试；永久失败进入死信；重复事件不重复应用；订单更新在 PostgreSQL 事务中完成。

### B-06：部署、联调和可观测性

负责 Nginx、API/Worker/Publisher 启动方式、本地联调脚本、部署配置、指标、日志、告警、备份恢复、压力测试和故障测试。

验收：H5 可以通过 Nginx 访问 Go API；各进程可独立启动；HTTPS 和密钥注入明确；依赖故障可观测；全链路测试可重复执行。

## 6. 依赖与并行顺序

第一轮：

    B-01
      ├─> B-02 ──> A-02
      └─> B-03 ──> B-04 ──> B-05 ──> A-03

A-01 可在 B-01 和 B-03 完成后并行开发。

第二轮：

    A-03 + B-05
      ├─> A-04 钱包/支付
      ├─> A-05 评价/申诉
      └─> A-06 管理端

B-06 可以与 A-04、A-05、A-06 并行。

第三轮：

    A-07 导航/统计/ML/WebSocket
      -> H5 全流程接入
      -> 生产化验收

## 7. 文件冲突规则

两线可以同时修改各自专属目录、测试文件和审批文档。

必须由集成人员处理：

- backend/go.mod；
- backend/go.sum；
- api/openapi.yaml；
- docs/migration/approval-log.md；
- 已批准的数据库迁移；
- 公共事件类型；
- 公共错误码；
- 公共响应结构；
- Nginx 生产入口。

发生冲突时，先停止堆叠修改，由集成人员统一合并并补充契约测试。

## 8. 分支建议

    codex/backend/a-01-auth
    codex/backend/a-02-station
    codex/backend/a-03-order
    codex/backend/a-04-wallet-payment
    codex/backend/a-05-review-appeal
    codex/backend/a-06-admin
    codex/backend/a-07-extension

    codex/backend/b-01-api-foundation
    codex/backend/b-02-postgres
    codex/backend/b-03-redis-stream
    codex/backend/b-04-outbox-publisher
    codex/backend/b-05-worker-device-events
    codex/backend/b-06-deployment-observability

## 9. 模块审批模式

每个模块交付时必须提供：

1. 分支和提交号；
2. 变更文件清单；
3. 接口和数据库变化；
4. 依赖模块；
5. 测试命令和结果；
6. 失败场景验证；
7. 已知限制；
8. 回滚方式；
9. 待审批事项。

审批结论：

- APPROVED：允许进入依赖模块；
- CHANGES_REQUIRED：退回修复；
- BLOCKED：存在外部依赖或契约未决。

审批前禁止合并到集成分支、开发依赖模块和宣称后端整体完成。

## 10. 完成标准

认定 Go 后端功能开发完成，必须同时满足：

- A-01 至 A-07 通过审批；
- B-01 至 B-06 通过审批；
- OpenAPI 与实际路由一致；
- PostgreSQL 是唯一业务事实源；
- 钱包、支付、订单结算可用；
- START/STOP 真实设备协议完成联调；
- Redis Streams 重试和恢复通过；
- H5 可完成主要业务流程；
- Nginx、HTTPS、密钥、监控和备份完成；
- 单元、集成、联调和故障测试全部通过。

