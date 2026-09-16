# Nginx 配置区

Nginx 负责 H5 静态资源、Go API 反向代理和后续 SSE 入口。

浏览器访问 Nginx，浏览器不得直接访问 Go 内部端口、Redis 或 PostgreSQL。

## 交付内容

| 文件 | 用途 |
|---|---|
| `ncs-api.conf.template` | Go 后端站点模板（静态资源、API 反代、设备回执来源限制、metrics/readyz 限制、TLS、trace ID） |
| `../backend/scripts/nginx-render.sh` | 渲染模板并用 `nginx -t` 校验；`--drill` 用 8443/8088 与自签证书在本机跑通一遍 |
| `../backend/scripts/local-stack.sh` | 一键起后端全栈并联调（含 Nginx 通路验证） |

旧 C++ 服务的站点不在本目录：迁移期间两套站点并存，切换由集成人员按发布计划执行。

## 端口与域名（B-06 裁决）

```text
公网 HTTPS：443（NCS_HTTPS_PORT，默认 443；本地演练用 8443）
公网 HTTP： 80 （NCS_HTTP_PORT，仅用于 301 跳转到 HTTPS；本地演练用 8088）
Nginx → Go API：127.0.0.1:8080（NCS_API_UPSTREAM）
Nginx → H5：静态文件目录（NCS_STATIC_ROOT）
Metrics：仅内网/本机（/metrics、/readyz 由 allow/deny 限制）
```

域名不写在代码或模板里，由部署环境注入：

```text
NCS_PUBLIC_HOST=<实际生产域名>
```

模板用占位域名即可完成配置验证；上线前必须由部署环境提供真实域名与证书。

## 渲染（必须给出变量白名单）

```bash
export NCS_PUBLIC_HOST=ncs.example.com
export NCS_HTTP_PORT=80 NCS_HTTPS_PORT=443
export NCS_STATIC_ROOT=/srv/h5
export NCS_API_UPSTREAM=127.0.0.1:8080
export NCS_TLS_CERT=/etc/ncs/tls/ncs.crt NCS_TLS_KEY=/etc/ncs/tls/ncs.key
export NCS_GATEWAY_ALLOW=10.20.30.0/24   # 设备网关来源网段
export NCS_OPS_ALLOW=10.99.0.0/16        # 运维/监控网段（/metrics、/readyz）
export NCS_ACCESS_LOG=/var/log/nginx/ncs-access.log
export NCS_ERROR_LOG=/var/log/nginx/ncs-error.log

envsubst '${NCS_PUBLIC_HOST} ${NCS_HTTP_PORT} ${NCS_HTTPS_PORT} ${NCS_STATIC_ROOT} \
${NCS_API_UPSTREAM} ${NCS_TLS_CERT} ${NCS_TLS_KEY} ${NCS_GATEWAY_ALLOW} ${NCS_OPS_ALLOW} \
${NCS_ACCESS_LOG} ${NCS_ERROR_LOG}' < nginx/ncs-api.conf.template > /etc/nginx/sites-enabled/ncs-api.conf

nginx -t && systemctl reload nginx
```

**不带变量白名单的 `envsubst` 是错的**：它会把 nginx 自己的 `$host`、`$request_id`、`$remote_addr`
等一起当成环境变量替换成空值，`proxy_set_header` 就会少参数、`nginx -t` 直接失败。这一点在 B-06
的实现过程中真实发生过，所以渲染脚本固定使用白名单。

## 设备回执入口的网络限制

`POST /api/v1/internal/charger-events` 能推进订单并触发计费，因此：

- 只允许 `NCS_GATEWAY_ALLOW` 指定的设备网关来源网段访问，其余一律 403；
- 必须经 HTTPS 提供，明文 HTTP 只做 301 跳转；
- `NCS_CHARGER_GATEWAY_TOKEN` 只注入到设备网关与 Go API，**不得下发给 H5 或普通 Agent**；
- 该路径不参与浏览器跨域，不配置 CORS。

`NCS_GATEWAY_ALLOW` 与 `NCS_OPS_ALLOW` 都要按真实网络规划填写，不要用 `0.0.0.0/0`。

## trace ID

模板按与后端一致的规则处理 `X-Request-ID`：客户端给出的值若匹配
`^[A-Za-z0-9._:-]{1,128}$`（同 `backend/internal/httpapi` 的 `validRequestID`）就沿用，否则用
Nginx 生成的 `$request_id`。非法值不会被写进访问日志，也不会进入 Outbox / Stream / Worker 的链路。
