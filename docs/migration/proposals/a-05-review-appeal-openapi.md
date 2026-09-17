# A-05 OpenAPI 变更提案（待集成人员确认后合入 api/openapi.yaml）

## 1. POST /api/v1/orders/{orderNo}/review

用户对自己的已完成订单评价（UC-U-12）：1..5 星 + 1..500 码点评论（去首尾空白）。  
幂等：同内容重试返回首次结果（201）；不同内容 409（错误码 5）。  
仅限：本人订单、COMPLETED、无申诉记录；跨用户按 404 处理。  
响应 201：{ "orderNo", "stars", "comment", "createdAt" }。错误：400/401/404/409。

## 2. GET /api/v1/orders/{orderNo}/review

返回本人评价（404 = 无评价）。

## 3. GET /api/v1/stations/{stationId}/reviews

场站评论墙（UC-U-12）：需登录；分页（page/pageSize）；按时间倒序、服务端限量。  
条目：{ "stars", "comment", "author", "createdAt" }；作者只显示昵称/掩码手机号/已注销用户。  
错误：400（非法 stationId）、401、404。

## 4. POST /api/v1/orders/{orderNo}/appeal

订单申诉（UC-U-09 pt 4）：仅本人 COMPLETED 订单；reason 1..500 码点；同内容幂等、不同内容 409；已有申诉（任何状态）409。  
响应 201：{ "id", "orderNo", "reason", "status": "PENDING", "orderAmountCent", "createdAt" }。错误：400/401/404/409。

## 5. GET /api/v1/admin/appeals

管理端申诉队列（OPERATOR/OWNER），status 过滤（PENDING|APPROVED），分页。  
条目：{ "id", "orderNo", "reason", "status", "orderAmountCent", "orderPaidCent", "createdAt", "decidedAt?" }。错误：401/403。

## 6. POST /api/v1/admin/appeals/{appealId}/approve

管理员审核通过（UC-U-09 pt 5，二次确认）：申诉转 APPROVED、订单转 CANCELLED、已扣金额退回钱包（REFUND 流水）、审计落 operation_logs；重复审核为无操作并返回当前结果。  
响应 200：申诉视图。错误：401/403/404/409。

## 影响说明

- 迁移 0009 提案：order_reviews + order_appeals 表（含唯一索引与 CHECK）。
- 评价修改/删除按 UC-U-12 本期不提供。
- 充电桩评价尚无需求规格，暂缺，待规格确认后另行立项。
