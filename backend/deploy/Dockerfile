# NCS Go 后端镜像（api / worker / outbox-publisher / mock-gateway 共用，用 NCS_TARGET 选择）
#
# 构建（仓库根目录）：
#   docker build -f backend/deploy/Dockerfile --build-arg NCS_TARGET=cmd/api -t ncs-api:dev .
#
# 运行时用 alpine：容器内保留 shell，便于编排平台做 HTTP 健康检查；
# 若需最小化攻击面，可把最后一级换成 gcr.io/distroless/static，并把健康检查交给平台探针。
FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY backend/go.mod backend/go.sum ./
RUN go mod download
COPY backend/ ./
ARG NCS_TARGET=cmd/api
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/ncs-app ./${NCS_TARGET}

FROM alpine:3.20
RUN adduser -D -H -u 10001 ncs && apk add --no-cache ca-certificates tzdata
COPY --from=builder /out/ncs-app /usr/local/bin/ncs-app
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/ncs-app"]
