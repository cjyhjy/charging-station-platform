# 迁移审批记录

| 模块 | 分支 | 状态 | 审批结论 | 备注 |
|---|---|---|---|---|
| G0 | `codex/migration/g0-contract-directory` | APPROVED | APPROVED | OpenAPI 3.0.3 解析通过；目录边界、P0 范围和审批门槛已冻结 |
| BE-A-01 | `codex/migration/backend-core/a-01-bootstrap` | APPROVED | APPROVED | `7d1f96b`；go test、go vet、go test -race、go build 和差异检查通过 |
| BE-A-02 | `codex/migration/backend-core/a-02-postgres` | CHANGES_REQUIRED | PENDING | `78b68b5`；测试通过，但 ID 类型和 Outbox 字段与已冻结契约不一致，已退回修改 |
| BE-B-01 | `codex/migration/backend-events/b-01-stream-bootstrap` | APPROVED | APPROVED | `949b494`；事件、Streams、ACK、Pending、重试、死信和 Worker 竞态测试通过 |
| BE-B-03 | `codex/migration/backend-events/b-03-outbox` | APPROVED | APPROVED | `53699e9`；Outbox 发布抽象、成功标记、失败保留和幂等边界测试通过；真实 Redis/PG 适配未纳入本模块 |
| H5-C-01 | `codex/migration/h5/c-01-shell` | PAUSED | NOT STARTED | 后端 P0 闭环前不启动 H5 业务开发 |
| BE-I-01 | `codex/migration/i-01-closed-loop-integration` | APPROVED | APPROVED | `9cf1b9a`；批准事件基础设施与 RESTART 模拟网关命令闭环，不宣称当时已完成订单 P0 全链路 |
| BE-I-02 | `codex/migration/i-02-charger-control-receipts` | APPROVED | APPROVED | `970e38e`；内部回执、开始/停止设备命令、STOP 恢复及跨日命令结果持久去重；真实 PG/Redis 测试与模拟网关闭环通过，部署前须先应用迁移 0007 |

说明：本表由集成人员维护。早期 BE-A-02、BE-A-03～BE-A-05、BE-B-01（Redis foundation）、
BE-B-04、BE-B-05 的行仍需按最终审批结论逐项追溯补齐；旧行仅是当时的历史状态，不能作为
当前集成代码的审批结论。BE-I-01/02 的批准口径以上两行与各模块审批记录为准。


## 新路线审批登记

旧版 G0/BE-A/BE-B/BE-I 编号保留为历史记录，不再用于新一轮模块审批。新路线采用 A-01 至 A-07、B-01 至 B-07 编号。

| 编号 | 模块 | 当前状态 | 备注 |
|---|---|---|---|
| A-01 | 认证和用户账户 | APPROVED | 认证、资料、冻结和注销已交付；头像 URL 校验列为 P2 加固待办 |
| A-02 | 站点和充电桩 | APPROVED（历史 P0） | Go P0 查询接口已交付 |
| A-03 | 订单和充电流程 | APPROVED（历史 P0） | Go P0 订单与 START/STOP 闭环已交付 |
| A-04 | 钱包、账务和支付 | NOT_STARTED | 下一项业务开发 |
| A-05 | 评价、申诉和售后 | NOT_STARTED | 依赖 A-03、A-06 |
| A-06 | 管理端扩展业务 | NOT_STARTED | 依赖 A-01 至 A-04 |
| A-07 | 导航、统计、ML、WebSocket | NOT_STARTED | 扩展阶段 |
| B-01 | Go API 和公共配置 | APPROVED（历史 P0） | 基础能力已交付 |
| B-02 | PostgreSQL 数据基础 | APPROVED（历史 P0） | 基础连接、迁移、仓储和事务已交付 |
| B-03 | Redis 和 Redis Streams | APPROVED（历史 P0） | Redis、Streams、ACK、Pending 和降级已交付 |
| B-04 | Outbox Publisher | APPROVED（历史 P0） | Publisher 和单活机制已交付 |
| B-05 | Worker 和设备事件 | APPROVED（历史 P0） | Worker、设备回执和重试死信已交付 |
| B-06 | 部署、联调和可观测性 | NOT_STARTED | PR #39 合入 develop 后开工 |
| B-07 | 钱包/账务数据支撑 | NOT_STARTED | 为 A-04 新增，不重复开发 B-02 |

边界裁决：

- backend/cmd/api/ 和 backend/internal/observability/ 归 B 线；
- B-06 不等待 H5 业务页面，以现有静态目录完成通路验收；
- B-06 从 PR #39 合入 develop 后的最新 develop 基线开分支；
- /api/v1/orders/{orderNo}/confirm 维持未登记状态，待 A-04 实现后再登记。
