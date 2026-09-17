package observability

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"
)

// The metrics endpoint each process serves.
//
// Prometheus scrapes a process, so every process that has something to say has to expose it: the
// worker's consumption counters and the publisher's lock state are not visible from the API, and a
// monitoring setup that can only see the API cannot tell "the worker is behind" from "the stream is
// fine". Each binary therefore serves its own registry on its own loopback address.
//
// The bind address is guarded on purpose. This endpoint reports the deployment's internals - stream
// backlogs, dependency states, migration versions - so the default is loopback and a bind that would
// expose it to the internet is refused unless the operator says so explicitly.

// MetricsContentType is re-exported here so a caller wiring the endpoint does not have to know that
// the exposition format lives in prometheus.go.
const metricsReadHeaderTimeout = 5 * time.Second

// ErrPublicMetricsBind reports a bind address that would expose the endpoint beyond the host or its
// private networks.
var ErrPublicMetricsBind = errors.New("observability: metrics address must be a loopback or private address")

// MetricsServerConfig configures one process's metrics endpoint.
type MetricsServerConfig struct {
	// Addr is the listen address, for example 127.0.0.1:9091.
	Addr string
	// Registry is the process's registry.
	Registry *Registry
	// Logger records startup and exposition failures.
	Logger *slog.Logger
	// AllowPublicBind permits an address that is neither loopback nor private. It exists so a
	// deployment with a separate monitoring network can opt in explicitly; the default has to be
	// safe because the consequence of the wrong default is an internal endpoint on the internet.
	AllowPublicBind bool
}

// MetricsServer is a running metrics endpoint.
type MetricsServer struct {
	listener net.Listener
	server   *http.Server
	logger   *slog.Logger
}

// StartMetricsServer validates the address, binds it and serves the registry.
//
// A failure to bind is returned rather than logged: a process whose metrics endpoint silently did
// not start looks healthy to every dashboard until somebody needs the data.
func StartMetricsServer(cfg MetricsServerConfig) (*MetricsServer, error) {
	if cfg.Registry == nil {
		return nil, errors.New("observability: metrics server requires a registry")
	}
	if err := ValidateMetricsAddress(cfg.Addr, cfg.AllowPublicBind); err != nil {
		return nil, err
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(discardWriter{}, nil))
	}

	listener, err := net.Listen("tcp", strings.TrimSpace(cfg.Addr))
	if err != nil {
		return nil, fmt.Errorf("listen for metrics on %s: %w", cfg.Addr, err)
	}

	server := &MetricsServer{
		listener: listener,
		logger:   logger,
		server: &http.Server{
			Handler:           MetricsHandler(cfg.Registry, logger),
			ReadHeaderTimeout: metricsReadHeaderTimeout,
		},
	}
	go func() {
		if err := server.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics endpoint stopped", "error", err)
		}
	}()
	logger.Info("metrics endpoint listening", "addr", listener.Addr().String())
	return server, nil
}

// Addr reports the bound address, which is useful when the caller asked for port 0.
func (s *MetricsServer) Addr() string {
	if s == nil || s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// Shutdown stops the endpoint.
func (s *MetricsServer) Shutdown(ctx context.Context) error {
	if s == nil || s.server == nil {
		return nil
	}
	return s.server.Shutdown(ctx)
}

// MetricsHandler serves the registry in the Prometheus text format.
//
// It is exported because the API also exposes it through Nginx for the ops range, and because tests
// drive it directly.
func MetricsHandler(registry *Registry, logger *slog.Logger) http.HandlerFunc {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(discardWriter{}, nil))
	}
	return func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet && request.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.WriteHeader(http.StatusMethodNotAllowed)
			_, _ = w.Write([]byte(`{"success":false,"code":1002,"message":"method not allowed"}`))
			return
		}
		w.Header().Set("Content-Type", PrometheusContentType)
		if _, err := registry.WritePrometheus(w); err != nil {
			// The body may be partially written; log and stop rather than inject an error document
			// into the middle of an exposition body.
			logger.Error("metrics exposition failed", "error", err)
		}
	}
}

// ValidateMetricsAddress enforces the rule the ruling set: metrics listen on the host or on a private
// network, never on a public address.
//
// The host part may be empty only when the port is a loopback-style explicit address, which is why an
// empty host (":9091", all interfaces) is refused outright: it is the most convenient way to expose
// the endpoint by accident.
func ValidateMetricsAddress(addr string, allowPublic bool) error {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return fmt.Errorf("%w: address is empty", ErrPublicMetricsBind)
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("observability: invalid metrics address %q: %w", addr, err)
	}
	if port == "" {
		return fmt.Errorf("observability: metrics address %q has no port", addr)
	}
	if allowPublic {
		return nil
	}
	if host == "" {
		return fmt.Errorf("%w: %q listens on every interface", ErrPublicMetricsBind, addr)
	}
	if strings.EqualFold(host, "localhost") {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil {
		// A hostname cannot be checked here, and a name is exactly how an external address gets in.
		return fmt.Errorf("%w: %q is not an IP address", ErrPublicMetricsBind, host)
	}
	if isLoopbackOrPrivate(ip) {
		return nil
	}
	return fmt.Errorf("%w: %s is a public address", ErrPublicMetricsBind, ip)
}

func isLoopbackOrPrivate(ip net.IP) bool {
	if ip.IsLoopback() {
		return true
	}
	// Private ranges from RFC 1918 and RFC 4193, plus link-local: a monitoring host is normally on
	// the same private network as the process.
	return ip.IsPrivate() || ip.IsLinkLocalUnicast()
}

// discardWriter keeps this file's fallback logger dependency-free.
type discardWriter struct{}

func (discardWriter) Write(body []byte) (int, error) { return len(body), nil }
