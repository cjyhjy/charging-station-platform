package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Settings is the complete, reloadable configuration of the Redis foundation.
//
// It exists because a Policy that only a developer can change in code is not a
// real policy: an operator must be able to change the degradation behaviour and
// the lifetimes of a deployment without a rebuild. SettingsFromEnv is the
// supported way in, and NewCapabilities is the single place the values are
// applied.
type Settings struct {
	Conn    ConnConfig
	Policy  Policy
	Cache   CacheConfig
	Session SessionConfig
	Limit   Limit
	Guard   GuardConfig
	// DeadLetter bounds the two keys behind the idempotent dead-letter write, which the
	// worker needs in order to tell "somebody is writing" apart from "it is written".
	DeadLetter DeadLetterClaimConfig
	// Required decides whether a process refuses to start when Redis is
	// unreachable. It defaults to true, because with session and lock set to
	// FailClosed a process that cannot reach Redis cannot honour its guarantees.
	// Setting it to false is an explicit acceptance of a degraded deployment and
	// must be logged loudly.
	Required bool
}

// DefaultSettings returns a coherent development baseline.
func DefaultSettings() Settings {
	return Settings{
		Conn:       DefaultConnConfig(),
		Policy:     DefaultPolicy(),
		Cache:      DefaultCacheConfig(),
		Session:    DefaultSessionConfig(),
		Limit:      Limit{Requests: 20, Window: time.Minute},
		Guard:      DefaultGuardConfig(),
		DeadLetter: DefaultDeadLetterClaimConfig(),
		Required:   true,
	}
}

// Validate rejects an incoherent configuration before any connection is opened.
func (s Settings) Validate() error {
	if err := s.Conn.Validate(); err != nil {
		return err
	}
	if err := s.Policy.Validate(); err != nil {
		return err
	}
	if err := s.Cache.validate(); err != nil {
		return err
	}
	if err := s.Session.validate(); err != nil {
		return err
	}
	if err := s.Limit.validate(); err != nil {
		return err
	}
	if err := s.Guard.validate(); err != nil {
		return err
	}
	if err := s.DeadLetter.validate(); err != nil {
		return err
	}
	return nil
}

// Environment variables read by SettingsFromEnv. The fail-mode variables accept
// "fail-open" or "fail-closed" so an operator never has to know the Go constant
// order.
const (
	envPolicyCache     = "NCS_REDIS_FAILMODE_CACHE"
	envPolicySession   = "NCS_REDIS_FAILMODE_SESSION"
	envPolicyRateLimit = "NCS_REDIS_FAILMODE_RATE_LIMIT"
	envPolicyLock      = "NCS_REDIS_FAILMODE_LOCK"
	envPolicyIdempot   = "NCS_REDIS_FAILMODE_IDEMPOTENCY"

	envCacheTTL             = "NCS_REDIS_CACHE_TTL"
	envSessionIdleTTL       = "NCS_REDIS_SESSION_IDLE_TTL"
	envSessionAbsoluteTTL   = "NCS_REDIS_SESSION_ABSOLUTE_TTL"
	envRateLimitRequests    = "NCS_REDIS_RATE_LIMIT_REQUESTS"
	envRateLimitWindow      = "NCS_REDIS_RATE_LIMIT_WINDOW"
	envIdempotencyTTL       = "NCS_REDIS_IDEMPOTENCY_TTL"
	envDeadLetterWriteTTL   = "NCS_REDIS_DEAD_LETTER_WRITE_TTL"
	envDeadLetterWrittenTTL = "NCS_REDIS_DEAD_LETTER_WRITTEN_TTL"
	envRedisRequired        = "NCS_REDIS_REQUIRED"
	envFailModeOpen         = "fail-open"
	envFailModeClosed       = "fail-closed"
	envFailModeOpenAlias    = "open"
	envFailModeClosedAlias  = "closed"
)

// SettingsFromEnv reads and validates the configuration through getenv. A nil
// getenv reads the process environment. Empty values are treated as unset, so a
// blank line in an .env file cannot blank out a default.
func SettingsFromEnv(getenv func(string) string) (Settings, error) {
	if getenv == nil {
		getenv = defaultGetenv
	}
	value := func(name string) string { return strings.TrimSpace(getenv(name)) }

	conn, err := ConnConfigFromEnv(getenv)
	if err != nil {
		return Settings{}, err
	}
	settings := DefaultSettings()
	settings.Conn = conn

	failModes := []struct {
		name   string
		target *FailMode
	}{
		{envPolicyCache, &settings.Policy.Cache},
		{envPolicySession, &settings.Policy.Session},
		{envPolicyRateLimit, &settings.Policy.RateLimit},
		{envPolicyLock, &settings.Policy.Lock},
		{envPolicyIdempot, &settings.Policy.Idempotency},
	}
	for _, item := range failModes {
		raw := value(item.name)
		if raw == "" {
			continue
		}
		mode, err := parseFailMode(raw)
		if err != nil {
			return Settings{}, fmt.Errorf("invalid %s=%q: %w", item.name, raw, err)
		}
		*item.target = mode
	}

	if err := assignDuration(value(envCacheTTL), envCacheTTL, &settings.Cache.DefaultTTL); err != nil {
		return Settings{}, err
	}
	if err := assignDuration(value(envSessionIdleTTL), envSessionIdleTTL, &settings.Session.IdleTTL); err != nil {
		return Settings{}, err
	}
	if err := assignDuration(value(envSessionAbsoluteTTL), envSessionAbsoluteTTL, &settings.Session.AbsoluteTTL); err != nil {
		return Settings{}, err
	}
	if err := assignDuration(value(envRateLimitWindow), envRateLimitWindow, &settings.Limit.Window); err != nil {
		return Settings{}, err
	}
	if err := assignInt(value(envRateLimitRequests), envRateLimitRequests, &settings.Limit.Requests); err != nil {
		return Settings{}, err
	}
	if err := assignDuration(value(envDeadLetterWriteTTL), envDeadLetterWriteTTL, &settings.DeadLetter.WriteTTL); err != nil {
		return Settings{}, err
	}
	if err := assignDuration(value(envDeadLetterWrittenTTL), envDeadLetterWrittenTTL, &settings.DeadLetter.WrittenTTL); err != nil {
		return Settings{}, err
	}
	if err := assignDuration(value(envIdempotencyTTL), envIdempotencyTTL, &settings.Guard.TTL); err != nil {
		return Settings{}, err
	}
	if raw := value(envRedisRequired); raw != "" {
		required, err := strconv.ParseBool(raw)
		if err != nil {
			return Settings{}, fmt.Errorf("invalid %s=%q: %w", envRedisRequired, raw, err)
		}
		settings.Required = required
	}

	if err := settings.Validate(); err != nil {
		return Settings{}, err
	}
	return settings, nil
}

func parseFailMode(raw string) (FailMode, error) {
	switch strings.ToLower(raw) {
	case envFailModeOpen, envFailModeOpenAlias:
		return FailOpen, nil
	case envFailModeClosed, envFailModeClosedAlias:
		return FailClosed, nil
	default:
		return FailClosed, fmt.Errorf("expected %q or %q", envFailModeOpen, envFailModeClosed)
	}
}

func assignDuration(raw, name string, target *time.Duration) error {
	if raw == "" {
		return nil
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return fmt.Errorf("invalid %s=%q: %w", name, raw, err)
	}
	*target = parsed
	return nil
}

func assignInt(raw, name string, target *int) error {
	if raw == "" {
		return nil
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil {
		return fmt.Errorf("invalid %s=%q: %w", name, raw, err)
	}
	*target = parsed
	return nil
}

// Capabilities is the assembled Redis foundation: every capability the rest of
// the backend consumes, built from one Settings value over one connection pool.
//
// This is the runtime wiring point. Building it is what turns a Policy from a
// code parameter into behaviour that a deployment actually applies.
type Capabilities struct {
	Cache    *Cache
	Sessions *Sessions
	Limiter  *Limiter
	Locker   *Locker
	Health   *Health
	Guard    *Guard
	// DeadLetterClaims is the stateful claim store behind the worker's idempotent
	// dead-letter write.
	DeadLetterClaims *DeadLetterClaims
	Stats            *Degradation
	// Streams is the real Redis Streams boundary. It is nil only when the
	// foundation was built over an injected command boundary, which is the test
	// path; NewCapabilities always sets it.
	Streams StreamClient
	// Inspector reads the runtime state of consumed streams for the observability
	// layer. It is nil only on the injected test path.
	Inspector StreamInspector

	commands Commands
	streams  StreamClient
}

// NewCapabilities builds the foundation over a real, pooled Redis client.
//
// It does not dial eagerly, so a successful return does not prove Redis is
// reachable: call Health.Check for that. A caller that needs startup failure
// detection should treat a failed check as fatal for a FailClosed deployment.
func NewCapabilities(settings Settings) (*Capabilities, error) {
	if err := settings.Validate(); err != nil {
		return nil, err
	}
	client, err := NewClient(settings.Conn)
	if err != nil {
		return nil, err
	}
	// The Streams boundary shares the same pool, so one process holds one bounded
	// set of connections for both the capability layer and the workers.
	return newCapabilitiesOver(client, NewStreamsClientOver(client), settings)
}

// newCapabilitiesOver wires the foundation over an already built command boundary.
// Tests use it to inject the in-memory adapters; production goes through
// NewCapabilities.
func newCapabilitiesOver(commands Commands, streams StreamClient, settings Settings) (*Capabilities, error) {
	stats := NewDegradation()

	cache, err := NewCache(commands, settings.Policy, stats, settings.Cache)
	if err != nil {
		return nil, err
	}
	sessions, err := NewSessions(commands, settings.Policy, stats, settings.Session, nil)
	if err != nil {
		return nil, err
	}
	limiter, err := NewLimiter(commands, settings.Policy, stats, nil)
	if err != nil {
		return nil, err
	}
	locker, err := NewLocker(commands, settings.Policy, stats, nil, nil)
	if err != nil {
		return nil, err
	}
	health, err := NewHealth(commands, stats, DefaultHealthTimeout, nil)
	if err != nil {
		return nil, err
	}
	guard, err := NewGuard(commands, settings.Policy, stats, settings.Guard, nil)
	if err != nil {
		return nil, err
	}
	deadLetterClaims, err := NewDeadLetterClaims(commands, settings.Policy, stats, settings.DeadLetter, nil)
	if err != nil {
		return nil, err
	}

	return &Capabilities{
		Cache:            cache,
		Sessions:         sessions,
		Limiter:          limiter,
		Locker:           locker,
		Health:           health,
		Guard:            guard,
		DeadLetterClaims: deadLetterClaims,
		Stats:            stats,
		Streams:          streams,
		Inspector:        inspectorOf(streams),
		commands:         commands,
		streams:          streams,
	}, nil
}

// inspectorOf exposes the stream inspector when the injected Streams boundary provides
// one. The in-memory adapter does not, so the observability collector is left unset on
// that path rather than being given a type that cannot sample.
func inspectorOf(streams StreamClient) StreamInspector {
	inspector, ok := streams.(StreamInspector)
	if !ok {
		return nil
	}
	return inspector
}

// Ready reports whether Redis answered a probe. A FailClosed deployment should
// refuse traffic while this returns false, because sessions and order locks
// cannot be honoured.
func (c *Capabilities) Ready(ctx context.Context) error {
	return c.Health.Check(ctx)
}

// Close releases every pooled connection. The capability layer and the Streams
// boundary share one pool, so closing once is enough and closing twice is safe.
func (c *Capabilities) Close() error {
	if c.commands == nil {
		return nil
	}
	return c.commands.Close()
}

// PolicySummary renders the effective degradation policy for a startup log, so
// an operator can confirm from the logs what the running process will do when
// Redis fails.
func (s Settings) PolicySummary() string {
	return fmt.Sprintf(
		"cache=%s session=%s rate-limit=%s lock=%s idempotency=%s cache-ttl=%s session-idle=%s session-absolute=%s rate-limit=%d/%s idempotency-ttl=%s dead-letter-write-ttl=%s dead-letter-written-ttl=%s",
		s.Policy.Cache, s.Policy.Session, s.Policy.RateLimit, s.Policy.Lock, s.Policy.Idempotency,
		s.Cache.DefaultTTL, s.Session.IdleTTL, s.Session.AbsoluteTTL,
		s.Limit.Requests, s.Limit.Window, s.Guard.TTL,
		s.DeadLetter.WriteTTL, s.DeadLetter.WrittenTTL,
	)
}
