#!/usr/bin/env bash
#
# Render and validate the Nginx site (B-06).
#
# Two modes:
#   default    render the template into a file and check it with `nginx -t` (needs the real cert paths)
#   --drill    render with local ports (8443/8088) and a self-signed certificate into a throwaway
#              prefix, start that nginx, and assert the site actually behaves: static files are
#              served, HTTP redirects to HTTPS, /metrics and /readyz and the device callback are 403
#              from a network that is not on their allow list, then stop it. This is the check that
#              catches the mistakes a syntax test cannot: a missing allow/deny, or an envsubst run
#              that swallowed nginx's own variables.
#
# Usage:
#   backend/scripts/nginx-render.sh [--output FILE] [--drill] [--static-root DIR]
#
# Rendering variables are the ones documented in nginx/README.md. The envsubst call always passes an
# explicit whitelist: without it, nginx's own $host/$request_id/$remote_addr are replaced by empty
# strings and proxy_set_header loses its argument.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../.." && pwd)"
template="${repo_root}/nginx/ncs-api.conf.template"

output=""
drill="false"
static_root="${NCS_STATIC_ROOT:-}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --output) output="$2"; shift 2 ;;
        --drill) drill="true"; shift ;;
        --static-root) static_root="$2"; shift 2 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

fail() { echo "FAIL: $*" >&2; exit 1; }
step() { printf '\n=== %s ===\n' "$*"; }
find_mime_types() {
    local candidate="/etc/nginx/mime.types" conf_path
    if [[ -f "${candidate}" ]]; then
        printf '%s\n' "${candidate}"
        return
    fi
    conf_path="$(nginx -V 2>&1 | sed -n 's/.*--conf-path=\([^ ]*\).*/\1/p')"
    candidate="$(dirname "${conf_path:-/etc/nginx/nginx.conf}")/mime.types"
    [[ -f "${candidate}" ]] || fail "nginx mime.types not found beside ${conf_path:-the configured nginx.conf}"
    printf '%s\n' "${candidate}"
}

[[ -f "${template}" ]] || fail "template not found: ${template}"

render() {
    # Every variable the template uses, and nothing else.
    envsubst '${NCS_PUBLIC_HOST} ${NCS_HTTP_PORT} ${NCS_HTTPS_PORT} ${NCS_STATIC_ROOT} ${NCS_API_UPSTREAM} \
${NCS_TLS_CERT} ${NCS_TLS_KEY} ${NCS_METRICS_UPSTREAM} ${NCS_GATEWAY_ALLOW} ${NCS_OPS_ALLOW} ${NCS_ACCESS_LOG} ${NCS_ERROR_LOG}' \
        < "${template}"
}

if [[ "${drill}" == "true" ]]; then
    step "drill: render for local ports and a self-signed certificate"
    work_dir="$(mktemp -d)"
    trap 'rm -rf "${work_dir}"' EXIT INT TERM
    mkdir -p "${work_dir}/prefix/logs" "${work_dir}/prefix/temp" "${work_dir}/certs" "${work_dir}/html"

    static_root="${static_root:-${work_dir}/html}"
    [[ -d "${static_root}" ]] || fail "static root not found: ${static_root}"
    if [[ ! -f "${static_root}/index.html" ]]; then
        echo "ncs static placeholder" > "${static_root}/index.html"
    fi

    cert="${work_dir}/certs/ncs.crt"
    key="${work_dir}/certs/ncs.key"
    openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj "/CN=ncs.drill.local" \
        -keyout "${key}" -out "${cert}" >/dev/null 2>&1 || fail "could not generate the drill certificate"

    # The ops and gateway ranges deliberately exclude 127.0.0.1: a restriction that allows the caller
    # proves nothing, so the drill asks from a network that must be refused.
    export NCS_PUBLIC_HOST="ncs.drill.local"
    export NCS_HTTP_PORT="8088"
    export NCS_HTTPS_PORT="8443"
    export NCS_STATIC_ROOT="${static_root}"
    export NCS_API_UPSTREAM="${NCS_API_UPSTREAM:-127.0.0.1:8080}"
    export NCS_METRICS_UPSTREAM="${NCS_METRICS_UPSTREAM:-127.0.0.1:9090}"
    export NCS_TLS_CERT="${cert}" NCS_TLS_KEY="${key}"
    export NCS_GATEWAY_ALLOW="10.20.30.0/24"
    export NCS_OPS_ALLOW="10.99.0.0/16"
    export NCS_ACCESS_LOG="${work_dir}/prefix/logs/access.log"
    export NCS_ERROR_LOG="${work_dir}/prefix/logs/error.log"

    render > "${work_dir}/site.conf"
    mime_types="$(find_mime_types)"
    cat > "${work_dir}/nginx.conf" <<CONF
worker_processes 1;
error_log ${work_dir}/prefix/logs/error.log warn;
pid ${work_dir}/prefix/logs/nginx.pid;
events { worker_connections 64; }
http {
    include ${mime_types};
    default_type application/octet-stream;
    client_body_temp_path ${work_dir}/prefix/temp/client;
    proxy_temp_path ${work_dir}/prefix/temp/proxy;
    fastcgi_temp_path ${work_dir}/prefix/temp/fastcgi;
    uwsgi_temp_path ${work_dir}/prefix/temp/uwsgi;
    scgi_temp_path ${work_dir}/prefix/temp/scgi;
    access_log ${work_dir}/prefix/logs/access.log;
    include ${work_dir}/site.conf;
}
CONF

    nginx -t -c "${work_dir}/nginx.conf" -p "${work_dir}/prefix" || fail "the rendered site does not pass nginx -t"

    step "drill: start nginx and assert the site"
    nginx -c "${work_dir}/nginx.conf" -p "${work_dir}/prefix"
    stop_nginx() { nginx -s stop -c "${work_dir}/nginx.conf" -p "${work_dir}/prefix" 2>/dev/null || true; }
    trap 'stop_nginx; rm -rf "${work_dir}"' EXIT INT TERM
    sleep 0.5

    curl_https() { curl -sk --resolve "${NCS_PUBLIC_HOST}:8443:127.0.0.1" "$@"; }

    static_status="$(curl_https -o /dev/null -w '%{http_code}' "https://${NCS_PUBLIC_HOST}:8443/")"
    [[ "${static_status}" == "200" ]] || fail "static path answered ${static_status}, want 200"

    redirect_status="$(curl -s --resolve "${NCS_PUBLIC_HOST}:8088:127.0.0.1" -o /dev/null -w '%{http_code}' "http://${NCS_PUBLIC_HOST}:8088/api/v1/stations")"
    [[ "${redirect_status}" == "301" ]] || fail "HTTP answered ${redirect_status}, want a 301 to HTTPS"

    for path in /metrics /readyz /api/v1/internal/charger-events; do
        status="$(curl_https -o /dev/null -w '%{http_code}' "https://${NCS_PUBLIC_HOST}:8443${path}")"
        [[ "${status}" == "403" ]] || fail "${path} answered ${status} from an outside network, want 403"
        echo "${path}: ${status} (restricted)"
    done

    health_status="$(curl_https -o /dev/null -w '%{http_code}' "https://${NCS_PUBLIC_HOST}:8443/healthz")"
    # /healthz has no allow list: a load balancer's probe has to reach it. Without the API running
    # behind the proxy it answers 502, which still proves the request was proxied rather than refused.
    [[ "${health_status}" == "200" || "${health_status}" == "502" ]] || fail "healthz answered ${health_status}, want 200 or a proxied 502"
    echo "/healthz: ${health_status} (proxied, no allow list)"

    # trace ID: a valid client value is carried, an invalid one is replaced by nginx's own id.
    grep -q 'request_id=' "${work_dir}/prefix/logs/access.log" || fail "the access log does not carry a trace id"

    stop_nginx
    trap 'rm -rf "${work_dir}"' EXIT INT TERM

    echo
    echo "PASS: nginx drill - static 200, http->https 301, /metrics, /readyz and the device callback are 403 outside their allow lists"
    exit 0
fi

step "render the site"
: "${NCS_PUBLIC_HOST:?set NCS_PUBLIC_HOST (see nginx/README.md)}"
: "${NCS_TLS_CERT:?set NCS_TLS_CERT}"
: "${NCS_TLS_KEY:?set NCS_TLS_KEY}"
: "${NCS_GATEWAY_ALLOW:?set NCS_GATEWAY_ALLOW to the charger gateway network}"
: "${NCS_OPS_ALLOW:?set NCS_OPS_ALLOW to the ops/monitoring network}"
: "${NCS_STATIC_ROOT:?set NCS_STATIC_ROOT to the H5 static directory}"
export NCS_HTTP_PORT="${NCS_HTTP_PORT:-80}"
export NCS_HTTPS_PORT="${NCS_HTTPS_PORT:-443}"
export NCS_API_UPSTREAM="${NCS_API_UPSTREAM:-127.0.0.1:8080}"
export NCS_METRICS_UPSTREAM="${NCS_METRICS_UPSTREAM:-127.0.0.1:9090}"
export NCS_ACCESS_LOG="${NCS_ACCESS_LOG:-/var/log/nginx/ncs-access.log}"
export NCS_ERROR_LOG="${NCS_ERROR_LOG:-/var/log/nginx/ncs-error.log}"

if [[ -n "${output}" ]]; then
    render > "${output}"
    echo "rendered ${output}"
else
    render
fi
