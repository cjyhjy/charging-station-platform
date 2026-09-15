# A-01 OpenAPI 变更提案（待集成人员确认后合入 api/openapi.yaml）

本提案为 A-01 用户资料范围的契约变更，遵循双线文档 §3 流程：此为“提出变更
+ 更新文档”步骤，两线确认前不得合入冻结契约。

## 1. GET /api/v1/me/profile（用户身份）

响应 data：

    {
      "id": 7,
      "phoneMasked": "138****0001",
      "displayName": "开发用户",
      "avatarUrl": "",
      "status": "ACTIVE",
      "registeredAt": "2026-09-15T12:00:00Z"
    }

## 2. PUT /api/v1/me/profile

请求（至少一个字段）：{ "displayName": "新昵称" (1..20，非纯空白), "avatarUrl": "".. }  
响应：同 GET。错误：400（昵称越界/纯空白、无变更字段）、401、404。

## 3. DELETE /api/v1/me（UC-U-05 申请注销）

204 无内容。语义：撤销全部会话（含当前）、立即匿名化用户名与手机号、
删除登录凭据；匿名业务记录按保留策略留存。错误：401、404。

## 4. POST /api/v1/admin/users/{userId}/freeze 与 /unfreeze

200：{ "id", "status": "DISABLED" | "ACTIVE" }。freeze 立即撤销该用户全部
会话（BR-07：冻结用户不能登录、不能开始充电）。管理员不得冻结自己的账号。
错误：400（自冻结、非法 id）、401、403（只读审计角色）、404。

## 影响说明

- 无既有端点破坏性变更；/me 保持原语义。
- 迁移依赖：0008 提案（avatar_url、deleted_at）。
- 实现状态：auth 包已含端口 AccountMutation + 服务与处理器；PostgreSQL
  仓储适配由 B-02 实现后接线。
