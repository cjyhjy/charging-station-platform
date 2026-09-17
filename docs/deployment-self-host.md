# fork 自托管部署

本文面向 `cjyhjy/charging-station-platform` 的 `codex/go-vue-retirement` 分支。当前可部署范围是
Go API、PostgreSQL、Redis、Outbox Publisher、Vue 车主端和 Vue 管理端。运营大屏尚未迁移到
Go 契约，默认不会启动。

## 最快测试部署

目标主机需要 Git、Docker Engine 与 Compose v2。先克隆分支并生成只保存在服务器上的配置：

```bash
git clone --branch codex/go-vue-retirement https://github.com/cjyhjy/charging-station-platform.git
cd charging-station-platform/backend/deploy
cp .env.example .env
chmod 600 .env
```

至少修改 `.env` 中的数据库口令和设备网关令牌。测试环境可以保留
`NCS_ENV=development` 与 `NCS_SMS_MOCK=true`；这会在登录接口响应中返回模拟验证码，不能用于公开生产环境。

如果 GHCR 包可读取，直接拉取并启动：

```bash
docker compose --profile mock pull
docker compose --profile mock up -d --wait
docker compose --profile mock ps
```

如果包仍是私有包，先使用仅有 `read:packages` 权限的 GitHub token 登录，或者直接执行
`docker compose --profile mock up -d --build --wait` 在主机上构建。

默认仅监听本机：车主端 `127.0.0.1:8088`，管理端 `127.0.0.1:8089`。可以先用 SSH 隧道验收：

```bash
ssh -L 8088:127.0.0.1:8088 -L 8089:127.0.0.1:8089 <server>
```

浏览器访问 `http://127.0.0.1:8088` 和 `http://127.0.0.1:8089`。查看日志和停止方式：

```bash
docker compose --profile mock logs -f api worker outbox-publisher
docker compose --profile mock down
```

## 公开部署门禁

公开访问前必须在两个本机 Web 端口前配置 HTTPS 反向代理并设置真实域名；不要把
`NCS_BIND_ADDRESS` 改成 `0.0.0.0` 后直接暴露明文 HTTP。正式环境还必须：

- 设置 `NCS_ENV=production`、`NCS_SMS_MOCK=false` 和真实短信服务配置；
- 将 `NCS_CHARGER_GATEWAY_URL` 指向真实 HTTPS 设备网关，使用 `--profile worker`，不启动 mock profile；
- 使用独立强口令、受控设备令牌、数据库备份和异地备份；
- 配置腾讯地图、LLM 等服务端凭据；凭据只放服务器 `.env` 或密钥系统；
- 在域名入口限制设备回执、`/metrics` 和 `/readyz` 的来源网络。

根目录 `nginx/ncs-api.conf.template` 与 `backend/scripts/nginx-render.sh` 提供正式 TLS 入口的安全基线。
实际域名、证书路径和设备/运维网段确定后再渲染，证书与私钥不得提交 Git。

## 当前限制

`dashboard-web` 镜像会持续构建用于回归，但其 `/api/v1/dashboard/*` 契约尚未在 Go API 实现，
因此不包含在默认或 mock 部署中。行政区价格版本、管理端备份界面、二次验证、ML 和真实设备协议
仍属于迁移缺口，详见[旧技术栈退役与 fork 集成验证说明](integration/go-vue-retirement.md)。
