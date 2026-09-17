# A-04 OpenAPI 变更提案（待集成人员确认后合入 api/openapi.yaml）

## 1. GET /api/v1/wallet

响应 data：{ "balanceCent": 10120 }

## 2. POST /api/v1/wallet/top-up

请求（Idempotency-Key 必填）：{ "amountCent": 10000 }（1..1_000_000，即 0.01..10000 元，UC-U-05）  
响应 data：{ "balanceCent": 10120 }。重复请求（同键同体）返回相同余额，不重复入账。  
错误：400（金额越界）、401、409（幂等冲突）、503。

## 3. GET /api/v1/wallet/transactions

参数：page、pageSize、type（TOP_UP|CHARGE|REFUND|ADJUSTMENT，可省略）。  
响应 data：{ "items": [{ "id", "transactionType", "amountCent", "balanceBeforeCent", "balanceAfterCent", "orderNo", "createdAt" }], "meta" }。

## 4. POST /api/v1/admin/orders/{orderNo}/refund

管理员退款（仅 SUPER_ADMIN/OPERATOR）：将已完成订单的已结算金额退回用户钱包。  
请求（Idempotency-Key 必填）：{ "reason": "..." }（可选）。响应 200：{ "balanceCent" }。  
语义：REFUND 流水入账、订单 paid_cents 清零、payment_status 回 PENDING（欠费语义不变）、审计落 operation_logs。  
错误：401、403、404、409（无已结算金额可退）、503。

## 影响说明

- 无既有端点变更；钱包表/流水表结构无新增迁移（0001 已含）。
- 退款为跨域事务：钱包入账 + 订单支付状态回退在同一事务。
