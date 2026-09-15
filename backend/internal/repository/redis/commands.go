package redis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	// ErrNotFound is a normal result, not a dependency failure: the key is
	// absent or expired. Callers must treat it as "no value" and continue, and
	// a degradation policy must never swallow it.
	ErrNotFound = errors.New("redis key not found")

	// ErrUnavailable reports that Redis could not be reached, so the command
	// never had a chance to run. Every behaviour in degradation.go keys off
	// this sentinel, so a production adapter MUST wrap transport, timeout and
	// authentication failures with it instead of returning them raw.
	ErrUnavailable = errors.New("redis is unavailable")

	// ErrInvalidTTL reports a negative time-to-live. Zero is accepted and means
	// "no expiration", matching the Redis client convention this boundary
	// exposes.
	ErrInvalidTTL = errors.New("redis ttl must not be negative")
)

// Commands is the narrow command boundary for non-Stream Redis data. Redis
// Streams keep their own StreamClient in streams.go. Both are small on purpose:
// a production adapter can serve them from one connection pool, and tests only
// implement what they actually exercise.
//
// Contract for production adapters:
//
//   - honour ConnConfig timeouts and PoolConfig bounds;
//   - translate transport, timeout and auth failures into ErrUnavailable;
//   - return ErrNotFound for a missing key instead of a driver-specific error;
//   - keep every command atomic, because callers rely on INCR, SetNX and
//     CompareAndDelete for concurrency control.
type Commands interface {
	Ping(context.Context) error
	Get(context.Context, string) (string, error)
	Set(context.Context, string, string, time.Duration) error
	Del(context.Context, ...string) (int, error)
	// Incr increments a plain counter. A counter that must expire inside a fixed
	// window must use IncrWithWindow instead: INCR followed by a separate EXPIRE
	// leaves a permanent counter whenever the process dies, the reply is lost, or
	// the cleanup fails.
	Incr(context.Context, string) (int64, error)
	// IncrWithWindow increments a counter and guarantees its window atomically,
	// returning the new count and the remaining window. The count and the expiry
	// are set in one indivisible step, so no interleaving or crash can leave a
	// counter without an expiration.
	IncrWithWindow(context.Context, string, time.Duration) (count int64, remaining time.Duration, err error)
	Expire(context.Context, string, time.Duration) (bool, error)
	SetNX(context.Context, string, string, time.Duration) (bool, error)
	// CompareAndDelete must be atomic: it is what stops a lock release from
	// deleting a lock a different holder has since acquired.
	CompareAndDelete(context.Context, string, string) (bool, error)
	// TTL returns the remaining lifetime of a key. hasExpiry is false for a key
	// that exists without an expiration, so callers never confuse "no TTL" with
	// "not found".
	TTL(context.Context, string) (ttl time.Duration, hasExpiry bool, err error)
	// RunScript executes a Lua script server-side with KEYS/ARGV and returns
	// the raw reply. It is the escape hatch for atomic multi-key operations
	// that no single command can express — e.g. verifying and voiding an SMS
	// code while counting wrong submissions, which must not interleave with
	// a concurrent verify of the same code.
	RunScript(ctx context.Context, script string, keys []string, args []string) (any, error)
	Close() error
}

// Opener builds a Commands from configuration. Client in client.go is the
// production implementation; tests and local runs use MemoryCommands. The
// interface exists so an adapter can be substituted without touching any
// capability in this package.
type Opener interface {
	Open(context.Context, ConnConfig) (Commands, error)
}

// PoolConfig bounds connection reuse. Every value is per process.
type PoolConfig struct {
	MaxOpen     int
	MaxIdle     int
	IdleTimeout time.Duration
	MaxLifetime time.Duration
}

// ConnConfig is the Redis connection baseline, including the pool settings Client
// applies. It is deliberately free of any client-specific type, so the adapter in
// client.go can be replaced without changing this package.
type ConnConfig struct {
	Address      string
	Username     string
	Password     string
	Database     int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	Pool         PoolConfig
}

// Environment variables read by ConnConfigFromEnv. They follow the NCS_ prefix
// used by the other backend modules.
const (
	envRedisAddress      = "NCS_REDIS_ADDR"
	envRedisUsername     = "NCS_REDIS_USERNAME"
	envRedisPassword     = "NCS_REDIS_PASSWORD"
	envRedisDatabase     = "NCS_REDIS_DB"
	envRedisDialTimeout  = "NCS_REDIS_DIAL_TIMEOUT"
	envRedisReadTimeout  = "NCS_REDIS_READ_TIMEOUT"
	envRedisWriteTimeout = "NCS_REDIS_WRITE_TIMEOUT"
	envRedisPoolMaxOpen  = "NCS_REDIS_POOL_MAX_OPEN"
	envRedisPoolMaxIdle  = "NCS_REDIS_POOL_MAX_IDLE"
	envRedisPoolIdleTTL  = "NCS_REDIS_POOL_IDLE_TIMEOUT"
	envRedisPoolLifetime = "NCS_REDIS_POOL_MAX_LIFETIME"
)

const defaultRedisAddress = "127.0.0.1:6379"

// defaultGetenv reads the process environment. It is shared by
// ConnConfigFromEnv and SettingsFromEnv so both treat an unset variable the same
// way.
func defaultGetenv(name string) string { return os.Getenv(name) }

// DefaultConnConfig returns a development-safe baseline. Callers must still
// supply Address in production; the localhost default never leaves a
// deployment credential implied.
func DefaultConnConfig() ConnConfig {
	return ConnConfig{
		Address:      defaultRedisAddress,
		Database:     0,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
		Pool: PoolConfig{
			MaxOpen:     32,
			MaxIdle:     8,
			IdleTimeout: 5 * time.Minute,
			MaxLifetime: time.Hour,
		},
	}
}

// String redacts the password so a ConnConfig can be logged or embedded in an
// error without leaking a credential. Address and username are kept because
// operators need them to diagnose a connection failure.
func (c ConnConfig) String() string {
	password := "unset"
	if c.Password != "" {
		password = "redacted"
	}
	return fmt.Sprintf(
		"redis.ConnConfig{address=%s, username=%s, password=%s, database=%d, pool={maxOpen=%d, maxIdle=%d}}",
		c.Address, c.Username, password, c.Database, c.Pool.MaxOpen, c.Pool.MaxIdle,
	)
}

// GoString keeps %#v output safe as well: a config printed with %#v must not
// reveal the password.
func (c ConnConfig) GoString() string { return c.String() }

// LogValue makes log/slog redact the password without every call site having to
// remember. This is the supported way to log a connection config.
func (c ConnConfig) LogValue() slog.Value {
	password := "unset"
	if c.Password != "" {
		password = "redacted"
	}
	return slog.GroupValue(
		slog.String("address", c.Address),
		slog.String("username", c.Username),
		slog.String("password", password),
		slog.Int("database", c.Database),
		slog.Group("pool",
			slog.Int("maxOpen", c.Pool.MaxOpen),
			slog.Int("maxIdle", c.Pool.MaxIdle),
			slog.Duration("idleTimeout", c.Pool.IdleTimeout),
			slog.Duration("maxLifetime", c.Pool.MaxLifetime),
		),
	)
}

// Validate rejects a configuration that would produce a silently degraded
// connection: a missing address, an unusable pool, or a non-positive timeout.
func (c ConnConfig) Validate() error {
	if strings.TrimSpace(c.Address) == "" {
		return fmt.Errorf("redis address is required")
	}
	if c.Database < 0 {
		return fmt.Errorf("redis database must not be negative: %d", c.Database)
	}
	if c.DialTimeout <= 0 {
		return fmt.Errorf("redis dial timeout must be greater than zero")
	}
	if c.ReadTimeout <= 0 {
		return fmt.Errorf("redis read timeout must be greater than zero")
	}
	if c.WriteTimeout <= 0 {
		return fmt.Errorf("redis write timeout must be greater than zero")
	}
	if c.Pool.MaxOpen <= 0 {
		return fmt.Errorf("redis pool max open must be greater than zero: %d", c.Pool.MaxOpen)
	}
	if c.Pool.MaxIdle < 0 {
		return fmt.Errorf("redis pool max idle must not be negative: %d", c.Pool.MaxIdle)
	}
	if c.Pool.MaxIdle > c.Pool.MaxOpen {
		return fmt.Errorf("redis pool max idle must not exceed max open: idle=%d open=%d", c.Pool.MaxIdle, c.Pool.MaxOpen)
	}
	if c.Pool.IdleTimeout <= 0 {
		return fmt.Errorf("redis pool idle timeout must be greater than zero")
	}
	if c.Pool.MaxLifetime <= 0 {
		return fmt.Errorf("redis pool max lifetime must be greater than zero")
	}
	return nil
}

// ConnConfigFromEnv reads configuration through getenv and validates it. A nil
// getenv reads the process environment. Empty values are treated as unset so a
// blank line in an .env file cannot blank out a default.
func ConnConfigFromEnv(getenv func(string) string) (ConnConfig, error) {
	if getenv == nil {
		getenv = defaultGetenv
	}
	config := DefaultConnConfig()

	// Addresses, numbers and durations are trimmed, because surrounding
	// whitespace there is a typo. Credentials are NOT: a password or ACL username
	// may legitimately begin or end with a space, and trimming it would silently
	// change the secret and break authentication.
	value := func(name string) string { return strings.TrimSpace(getenv(name)) }
	credential := func(name string) string { return getenv(name) }

	if address := value(envRedisAddress); address != "" {
		config.Address = address
	}
	config.Username = credential(envRedisUsername)
	config.Password = credential(envRedisPassword)

	if raw := value(envRedisDatabase); raw != "" {
		database, err := strconv.Atoi(raw)
		if err != nil {
			return ConnConfig{}, fmt.Errorf("invalid %s=%q: %w", envRedisDatabase, raw, err)
		}
		config.Database = database
	}

	durations := []struct {
		name   string
		target *time.Duration
	}{
		{envRedisDialTimeout, &config.DialTimeout},
		{envRedisReadTimeout, &config.ReadTimeout},
		{envRedisWriteTimeout, &config.WriteTimeout},
		{envRedisPoolIdleTTL, &config.Pool.IdleTimeout},
		{envRedisPoolLifetime, &config.Pool.MaxLifetime},
	}
	for _, item := range durations {
		if raw := value(item.name); raw != "" {
			parsed, err := time.ParseDuration(raw)
			if err != nil {
				return ConnConfig{}, fmt.Errorf("invalid %s=%q: %w", item.name, raw, err)
			}
			*item.target = parsed
		}
	}

	integers := []struct {
		name   string
		target *int
	}{
		{envRedisPoolMaxOpen, &config.Pool.MaxOpen},
		{envRedisPoolMaxIdle, &config.Pool.MaxIdle},
	}
	for _, item := range integers {
		if raw := value(item.name); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil {
				return ConnConfig{}, fmt.Errorf("invalid %s=%q: %w", item.name, raw, err)
			}
			*item.target = parsed
		}
	}

	if err := config.Validate(); err != nil {
		return ConnConfig{}, err
	}
	return config, nil
}
