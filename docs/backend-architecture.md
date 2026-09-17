# 充电桩平台后端架构与实现路径

## 1. 文档定位

本文档描述当前 Go 后端迁移后的目标架构，并以现有代码和 P0 闭环实现为准。后端对外提供稳定的 HTTP/JSON 接口；PostgreSQL 保存业务事实，Redis 承担会话、限流、缓存和分布式协调，Redis Streams 承担异步事件传递。

H5 只依赖 HTTP API，不访问 PostgreSQL、Redis 或 Redis Streams。旧 C++/Crow 服务在迁移期间作为遗留系统保留，通过 Nginx 分流，不与 Go 后端共同写入同一业务状态。

## 2. 技术栈

| 层次 | 技术 | 责任 |
|---|---|---|
| 客户端 | H5 | 用户端和管理端页面，通过 HTTPS 调用 API |
| 网关 | Nginx | TLS 终止、静态资源、API 反向代理、设备回执网络限制 |
| API 服务 | Go 1.22 | REST API、认证、领域服务、参数校验和事务编排 |
| 数据库 | PostgreSQL | 用户、站点、充电桩、订单、账务、Outbox、消费记录等权威数据 |
| 缓存与会话 | Redis | 会话、短信码、限流、缓存、幂等加速、锁 |
| 异步消息 | Redis Streams | 订单事件、设备命令、命令结果的可靠投递 |
| 发布器 | Go 独立进程 | 从 PostgreSQL Outbox 发布到 Redis Streams |
| 消费器 | Go Worker | 消费、ACK、Pending 恢复、重试、死信和业务 Applier |
| 设备联调 | Go mock-gateway | 模拟设备命令和 START/STOP 回执，仅用于开发测试 |
| 契约 | OpenAPI 3.0 | H5、Agent、Mock 和后端共同使用的接口基线 |

Go 后端直接依赖 pgx/v5 和 Go 标准库，保持服务轻量，避免引入不必要的框架层。

## 3. 总体架构

    H5 / 管理端 / Agent
            |
         HTTPS
            |
          Nginx
       +----+----------------------+
       |                           |
   Go API :8080                C++/Crow legacy
       |                           |
       +------------+--------------+
                    |
              PostgreSQL
          (业务事实与 Outbox)
                    |
        +-----------+-----------+
        |                       |
  outbox-publisher          Go Worker
        |                       |
        +------ Redis ----------+
              |       |
          Streams   Session/Cache/Lock
                    |
              Charger Gateway
          (内部 Bearer 回执接口)

核心原则：

1. PostgreSQL 是订单状态、账务、命令结果和消费记录的最终事实源。
2. Redis 故障不能导致订单最终状态只存在于 Redis。
3. Outbox 保证业务事务提交后事件不会因 API 进程退出而丢失。
4. Redis Streams 提供消费组、Pending、ACK、重试和死信能力。
5. 同一业务写操作只能由一个主服务负责，Go 迁移完成后由 Go 作为主写入方。

## 4. Go 后端分层

### 4.1 API 层

目录：backend/internal/httpapi、auth、station、order、admin。

职责：

- 解析 HTTP 请求和 JSON；
- 校验路径参数、查询参数和请求体；
- 读取 Bearer 会话并进行角色校验；
- 校验 Idempotency-Key；
- 将领域错误映射为统一 HTTP 状态码和响应；
- 不直接执行 SQL，不直接操作 Redis 数据结构。

统一响应结构：

    {
      "success": true,
      "code": 0,
      "message": "ok",
      "data": {}
    }

接口字段和枚举以 api/openapi.yaml 为准。当前接口清单见 docs/api-integration.md。

### 4.2 领域服务层

目录：backend/internal/order、auth、station、admin。

职责：

- 实现订单状态机；
- 校验用户、站点、充电桩和订单之间的业务关系；
- 生成设备命令；
- 处理设备 START/STOP 回执；
- 编排账务和订单状态更新；
- 在一个数据库事务中完成状态、幂等记录和 Outbox 写入。

前端不能提交目标状态。订单状态只能由后端根据用户命令和设备事实推进。

### 4.3 持久化层

目录：backend/internal/repository/postgres、backend/migrations。

PostgreSQL 保存：

- 用户、管理员和会话关联数据；
- 站点和充电桩；
- 订单及订单状态；
- 钱包、账务和债务；
- 设备命令及命令结果；
- Outbox 事件；
- event_consumptions 消费记录；
- 幂等键、请求摘要和事务审计字段。

所有迁移按递增版本执行，必须提供可重复执行的 up 迁移和必要的 down 脚本。生产库禁止直接执行开发种子数据。

### 4.4 Redis 与 Streams 层

目录：backend/internal/repository/redis、backend/internal/event、backend/internal/worker。

Redis 用于：

- 会话和登录状态；
- 短信验证码；
- 登录限流；
- 可失效缓存；
- 分布式锁；
- Streams 消费组和 Pending 管理；
- 幂等快速路径。

消费记录的权威判定在 PostgreSQL。Redis 的幂等记录只能作为加速和降载手段，不能替代数据库消费记录。

## 5. 核心业务链路

### 5.1 用户下单到开始充电

    H5
      -> POST /api/v1/orders
      -> Go API
      -> PostgreSQL 事务：
           写订单
           锁定充电桩
           写 Outbox
      -> 返回 CREATED

    H5
      -> POST /api/v1/orders/{orderNo}/start
      -> Go API
      -> PostgreSQL 事务：
           幂等记录
           订单变为 STARTING
           写设备命令和 Outbox
      -> 返回 202 Accepted

    outbox-publisher
      -> 获取 PostgreSQL advisory lock
      -> 读取未发布 Outbox
      -> 写入 Redis Stream
      -> 条件标记 published

    Worker
      -> 读取设备命令
      -> 调用 Charger Gateway
      -> ACK 或进入 Pending/重试/死信

### 5.2 设备回执到订单完成

    Charger Gateway
      -> POST /api/v1/internal/charger-events
         Authorization: Bearer <gateway-token>
      -> Go API 校验事件、时间和设备关系
      -> PostgreSQL 事务：
           eventId 幂等记录
           应用订单事实
           写 CHARGE_STARTED/CHARGE_STOPPED Outbox
      -> 返回回执处理结果

    Worker
      -> 消费订单事件
      -> PostgreSQL Applier：
           校验消费状态
           更新订单、账务和充电桩
           写必要的后续 Outbox
      -> ACK

开始和停止接口返回“命令已接收”，不表示设备已经完成动作。H5 必须通过订单查询或事件状态刷新最终结果。

## 6. 可靠性与一致性设计

### 6.1 Outbox

业务状态和 Outbox 必须在同一 PostgreSQL 事务中提交。发布器采用“先发布、后标记”的策略；重复发布由消费端幂等处理。

发布器使用 PostgreSQL advisory lock 保证单活。Worker 可以水平扩展，但同一消费组中的单条消息只由一个消费者持有处理租约。

### 6.2 幂等

以下操作必须有幂等键：

- 创建订单；
- 开始充电；
- 停止充电；
- 取消订单；
- 管理员重启充电桩；
- 设备事件回执。

相同幂等键和相同请求摘要应重放第一次结果；相同幂等键但请求内容不同必须返回冲突。

### 6.3 Pending、重试和死信

- 处理成功后 ACK；
- 暂时性错误保留 Pending 并按预算重试；
- 超过重试预算进入死信；
- Worker 启动时先恢复历史 Pending，再读取新消息；
- 死信写入采用声明、完成和中止的状态过程，避免并发重复写入；
- Worker 被重启或异常退出后，不得丢失未确认消息。

### 6.4 Redis 降级

默认策略：

- cache：Fail Open；
- rate-limit：Fail Open；
- session：Fail Closed；
- lock：Fail Closed；
- idempotency：按配置执行，但不能在降级时谎报已持有锁或已完成幂等。

订单和账务主流程以 PostgreSQL 为准；Redis 故障不能让系统伪造设备回执或跳过订单状态校验。

## 7. 安全边界

- H5 只持有用户会话令牌；
- 管理员接口必须校验管理员角色；
- 设备回执使用独立的 NCS_CHARGER_GATEWAY_TOKEN；
- 设备令牌不能出现在 H5、前端日志或普通用户响应中；
- 生产环境必须通过 HTTPS；
- Nginx 只允许设备网关来源访问 internal/charger-events；
- PostgreSQL 和 Redis 不直接暴露公网；
- 日志中不得输出密码、完整令牌和敏感支付数据；
- 所有关键请求使用 trace ID 贯穿 API、Outbox、Stream 和 Worker 日志。

## 8. 部署拓扑

建议最小部署单元：

    nginx
      |
      +-- h5 static files
      +-- go-api x 2+
      +-- go-worker x 2+
      +-- outbox-publisher x 1 active
      +-- postgres primary
      +-- redis

扩展策略：

- Go API 无状态部署，可水平扩展；
- Worker 通过 Redis Stream 消费组水平扩展；
- Outbox Publisher 默认单活，故障时由另一实例竞争 advisory lock；
- PostgreSQL 负责持久化和事务，不以 Redis 作为数据库替代；
- Redis 使用持久化和监控配置，故障恢复后由 PostgreSQL Outbox/Pending 继续推进。

## 9. 实现路径

### 阶段一：契约和基础设施

1. 冻结 api/openapi.yaml；
2. 固定统一响应、错误码、认证和幂等规则；
3. 建立 Go API、配置、健康检查和 PostgreSQL 迁移；
4. 建立 Redis 连接池、会话、限流、锁和降级策略；
5. 建立 H5 Mock 和 Agent 客户端生成基线。

验收：OpenAPI 可解析，API 可启动，PostgreSQL 迁移可重复执行，Redis 依赖状态可观测。

### 阶段二：P0 同步业务

1. 用户短信登录和管理员登录；
2. 站点、充电桩查询；
3. 创建、查询、取消订单；
4. 开始和停止命令接口；
5. 管理端站点、充电桩、用户、订单和重启接口。

验收：接口契约测试、权限测试、幂等测试和 PostgreSQL 事务测试通过。

### 阶段三：事件闭环

1. Outbox 写入和独立 Publisher；
2. Redis Streams 消费组、ACK、Pending、重试和死信；
3. 设备命令 Worker；
4. /api/v1/internal/charger-events；
5. START/STOP 回执 Applier；
6. PostgreSQL 消费记录和命令结果落库。

验收：CREATED → STARTING → CHARGING → STOPPING → COMPLETED 全链路可复现，重复事件不重复扣费，Worker 重启不丢消息。

### 阶段四：H5 和旧 C++ 迁移

1. H5 按 OpenAPI 接入 Go API；
2. Nginx 将已迁移路径分流到 Go；
3. C++ 旧路径保留为兼容入口；
4. 只保留一个业务主写入方；
5. 对读接口进行双读比对；
6. 完成历史数据迁移和回滚演练；
7. 关闭已迁移的 C++ 写接口。

验收：H5 主流程不依赖 C++，Go 和旧系统的数据结果完成比对，具备可回滚发布方案。

### 阶段五：生产化

1. 配置密钥管理和 HTTPS；
2. 设置 PostgreSQL 备份、恢复和迁移门禁；
3. 设置 Redis 高可用、容量和淘汰策略；
4. 接入指标、日志、trace 和告警；
5. 执行压力、故障注入、重复投递和恢复演练；
6. 完成真实充电桩协议联调；
7. 逐步下线 C++ 兼容层。

## 10. 当前边界

当前 Go 后端已覆盖 P0 API、PostgreSQL、Redis、Redis Streams、Outbox、Worker 和模拟网关闭环。模拟网关验证不等价于真实设备协议认证；真实生产上线前仍需完成真实网关协议、HTTPS 证书、密钥注入、监控告警、备份恢复和容量压测。

当前不应让客户端调用：

- PostgreSQL、Redis 或 Redis Streams；
- 设备网关内部回执接口；
- 未在 OpenAPI 登记的接口；
- 未在 Go 路由实际实现的订单确认接口。

