FROM node:22-alpine AS builder

WORKDIR /src
RUN corepack enable
COPY apps/shared ./apps/shared
COPY apps/user/package.json apps/user/package-lock.json ./apps/user/
COPY apps/admin/package.json apps/admin/package-lock.json ./apps/admin/
COPY apps/dashboard/package.json apps/dashboard/pnpm-lock.yaml ./apps/dashboard/

ARG NCS_WEB_APP
RUN case "$NCS_WEB_APP" in \
      user|admin) cd "apps/$NCS_WEB_APP" && npm ci ;; \
      dashboard) cd apps/dashboard && pnpm install --frozen-lockfile ;; \
      *) echo "NCS_WEB_APP must be user, admin or dashboard" >&2; exit 2 ;; \
    esac

COPY apps/user ./apps/user
COPY apps/admin ./apps/admin
COPY apps/dashboard ./apps/dashboard
RUN case "$NCS_WEB_APP" in \
      user|admin) cd "apps/$NCS_WEB_APP" && npm run build ;; \
      dashboard) cd apps/dashboard && pnpm run build ;; \
    esac && mkdir -p /out && cp -R "apps/$NCS_WEB_APP/dist/." /out/

FROM nginx:1.27-alpine
COPY backend/deploy/web-nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=builder /out/ /usr/share/nginx/html/
HEALTHCHECK --interval=10s --timeout=3s --retries=6 \
  CMD wget -q -O /dev/null http://127.0.0.1/healthz || exit 1
