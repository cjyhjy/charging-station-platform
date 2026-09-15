# Nginx 配置区

Nginx 负责 H5 静态资源、Go API 反向代理和后续 SSE 入口。

浏览器访问 Nginx，浏览器不得直接访问 Go 内部端口、Redis 或 PostgreSQL。
