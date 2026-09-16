# NCS 充电桩管理平台 — Go/Vue 集成测试分支

本分支使用 Go API / Outbox Publisher / Worker、PostgreSQL 18、Redis 7.4，以及 Vue 3 用户端和管理端。
这是用户 fork 中的迁移验证候选，不是已完成全部需求验收的生产版本。

## 运行与验证

准备 Go 1.26、Node.js 22、npm、pnpm 10、PostgreSQL 18 和 Redis 7.4。
服务配置与启动见 [Go 后端说明](backend/README.md) 和 [部署说明](backend/deploy/README.md)。
本地联调使用 `backend/scripts/local-stack.sh`；前端在各自目录执行 `npm ci && npm run dev`。

```bash
./scripts/check.sh
# 仅使用一次性测试 PostgreSQL、独立 Redis 库：
NCS_TEST_PG_DSN='<test DSN>' NCS_REDIS_TEST_ADDR=127.0.0.1:6379 NCS_REDIS_TEST_DB=15 ./scripts/test-backend.sh
(cd apps/user && npm ci && npm run test && npm run build)
(cd apps/admin && npm ci && npm run test && npm run build)
(cd apps/dashboard && pnpm install --frozen-lockfile && pnpm test && pnpm build)
```

## 文档与协作

- [fork 验证、旧栈退役边界与合并步骤](docs/integration/go-vue-retirement.md)
- [需求基线](docs/01-requirements-specification.md)、[需求追踪](docs/requirements-traceability.md)
- [Go OpenAPI](api/openapi.yaml)、[接口对接](docs/api-integration.md)
- [研发流程](docs/development-guide.md)、[安全要求](SECURITY.md)

`legacy/` 保存旧 C++/Crow/Qt/SQLite 源码与测试，仅作迁移对照，不参与当前构建或部署。
大屏仍执行自身测试，但其 Go API 接入未完成验收。历史文档与矩阵中的“完成”不能代表新实现已等价覆盖。
