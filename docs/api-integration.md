# Go 后端接口对接说明

本文档与当前 Go 后端实现保持一致。客户端和 Agent 应以 api/openapi.yaml 为字段、状态码和响应结构的最终契约，不要根据旧 C++ 接口自行推断路径或字段。

## 服务入口与认证

- Nginx/API 入口：/api/v1
- 用户端和管理端接口：Authorization: Bearer <session-token>
- 设备网关回执接口：Authorization: Bearer <NCS_CHARGER_GATEWAY_TOKEN>
- 设备网关令牌不能下发给 H5。
- 除登录、健康检查外，业务接口都需要有效会话和对应角色。
- 创建订单、开始、停止、取消、管理员重启均须携带长度 16--128 的 Idempotency-Key。

## 当前已实现接口

| 用途 | 方法 | 路径 | 权限 |
|---|---|---|---|
| 存活检查 | GET | /api/v1/healthz | 公开 |
| 依赖就绪检查 | GET | /api/v1/readyz | 公开 |
| 获取短信验证码 | POST | /api/v1/auth/user/sms/code | 公开 |
| 用户短信登录 | POST | /api/v1/auth/user/login/sms | 公开 |
| 用户密码登录 | POST | /api/v1/auth/user/login | 公开/次要入口 |
| 管理员登录 | POST | /api/v1/auth/admin/login | 公开 |
| 注销当前会话 | POST | /api/v1/auth/logout | 用户/管理员 |
| 查询当前身份 | GET | /api/v1/me | 已登录 |
| 查询站点 | GET | /api/v1/stations | 用户 |
| 查询站点详情 | GET | /api/v1/stations/{stationId} | 用户 |
| 查询充电桩 | GET | /api/v1/chargers | 用户 |
| 查询订单 | GET | /api/v1/orders | 用户 |
| 创建订单 | POST | /api/v1/orders | 用户 |
| 查询订单详情 | GET | /api/v1/orders/{orderNo} | 用户 |
| 开始充电 | POST | /api/v1/orders/{orderNo}/start | 用户 |
| 停止充电 | POST | /api/v1/orders/{orderNo}/stop | 用户 |
| 取消订单 | POST | /api/v1/orders/{orderNo}/cancel | 用户 |
| 接收设备回执 | POST | /api/v1/internal/charger-events | 设备网关 |
| 管理员查询站点 | GET | /api/v1/admin/stations | 管理员 |
| 管理员创建站点 | POST | /api/v1/admin/stations | 管理员 |
| 管理员查询充电桩 | GET | /api/v1/admin/chargers | 管理员 |
| 管理员查询用户 | GET | /api/v1/admin/users | 管理员 |
| 管理员查询订单 | GET | /api/v1/admin/orders | 管理员 |
| 管理员重启充电桩 | POST | /api/v1/admin/chargers/{chargerId}/restart | 管理员写权限 |

## P0 业务流程

    短信登录
      -> 查询站点/充电桩
      -> 创建订单
      -> POST start（202，进入异步设备命令流程）
      -> 设备网关 POST charger-events（CHARGE_STARTED）
      -> POST stop（202，进入异步设备命令流程）
      -> 设备网关 POST charger-events（CHARGE_STOPPED）
      -> 查询订单确认最终状态

开始和停止接口返回的是“命令已接收”，不是设备已经完成动作。最终状态必须通过订单查询或后续事件确认，客户端不能自行修改订单状态。

## 请求与响应约定

所有 JSON 响应使用统一结构：

    {
      "success": true,
      "code": 0,
      "message": "ok",
      "data": {}
    }

失败响应仍使用 success=false、业务 code 和 message。常见 HTTP 状态码为 400（参数错误）、401（未登录/令牌无效）、403（权限不足）、404（资源不存在）、409（状态或幂等冲突）、429（限流）和 503（依赖不可用）。字段类型、枚举值、分页参数和事件载荷以 OpenAPI 为准。

## 设备回执约束

POST /api/v1/internal/charger-events 只接受设备网关服务令牌。eventId 是幂等键；相同事件重复投递会重放首次结果，不同内容复用同一 eventId 会返回冲突。H5 和普通 Agent 不得调用此接口。

## 当前明确不提供的接口

当前 Go 后端没有实现“用户手动确认并扣款”的 POST /api/v1/orders/{orderNo}/confirm。该路径已从 OpenAPI 删除，客户端不得调用；订单停止后的状态和账务处理由设备回执、Applier 和后端事务完成。

## 本地联调

后端启动至少需要 PostgreSQL、Redis 和 NCS_CHARGER_GATEWAY_TOKEN：

    export NCS_POSTGRES_DSN="postgres://ncs:ncs@127.0.0.1:5432/ncs_dev"
    export NCS_REDIS_ADDR="127.0.0.1:6379"
    export NCS_CHARGER_GATEWAY_TOKEN="local-gateway-token"
    go run ./cmd/api

开发环境可设置 NCS_SMS_MOCK=true，验证码会出现在短信接口响应中。生产环境必须关闭模拟短信，并通过 HTTPS 和 Nginx 网络白名单保护设备回执入口。

