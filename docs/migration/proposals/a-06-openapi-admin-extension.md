# A-06 OpenAPI 变更提案（待集成人员确认后合入 api/openapi.yaml）

## 1. GET /api/v1/admin/chargers/{chargerId}/tariff

响应 data：{ "chargerId", "electricityPriceCentPerKwh", "servicePriceCentPerKwh", "offPeakElectricityPriceCentPerKwh?", "offPeakStartHour?", "offPeakEndHour?" }  
错误：401、403、404。

## 2. PUT /api/v1/admin/chargers/{chargerId}/tariff

请求：{ "electricityPriceCentPerKwh" >=0, "servicePriceCentPerKwh" >=0, "offPeakElectricityPriceCentPerKwh"? >=0, "offPeakStartHour"? 0..23, "offPeakEndHour"? 0..23 }（谷价必须与完整窗口同给；窗口起止不可相等）  
响应：同 GET。审计：tariff.update（含前后值）。  
说明：订单在开始充电时快照费率，运行中订单账单不受调价影响。

## 3. POST /api/v1/admin/chargers/{chargerId}/release（BR-11 强制释放）

请求（Idempotency-Key 必填）：{ "reason": 2..200 字符, "targetStatus": "IDLE" | "DISABLED" }  
响应 200：{ "chargerId", "chargerCode", "orderNo?", "status" }  
语义：仅释放 CREATED/STARTING 订单占用的设备（订单转 CANCELLED）；CHARGING/STOPPING 设备必须先走受控停止流程，直接释放返回 409；STARTING 订单入队 CANCEL_START 撤销命令；审计 charger.force-release。

## 4. GET /api/v1/admin/users/{userId}

响应 data：{ "id", "phone", "displayName", "avatarUrl?", "status", "balanceCent", "registeredAt", "deletedAt?" }（管理端可见完整手机号，SRS 管理端列表）。

## 5. GET /api/v1/admin/users/{userId}/transactions

参数：page、pageSize、type（TOP_UP|CHARGE|REFUND|ADJUSTMENT）。响应：同用户流水（管理视图）。

## 6. GET /api/v1/admin/audit

参数：page、pageSize、actorId、action、resourceType、resourceId。  
响应 data：{ "items": [{ "id", "actorType", "actorId", "action", "resourceType", "resourceId", "requestId?", "payload?", "createdAt" }], "meta" }。

## 影响说明

- 依赖：钱包流水（A-04 已批）、operation_logs（0001 已含）、费率列（0004 已含）。
- 无新增迁移。
