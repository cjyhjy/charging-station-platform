package redis

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestDefaultConnConfigIsValid(t *testing.T) {
	config := DefaultConnConfig()
	if err := config.Validate(); err != nil {
		t.Fatalf("default config must be valid: %v", err)
	}
	if config.Address != defaultRedisAddress {
		t.Fatalf("expected address %q, got %q", defaultRedisAddress, config.Address)
	}
}

func TestConnConfigValidateRejectsUnusableValues(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ConnConfig)
	}{
		{"empty address", func(c *ConnConfig) { c.Address = "   " }},
		{"negative database", func(c *ConnConfig) { c.Database = -1 }},
		{"zero dial timeout", func(c *ConnConfig) { c.DialTimeout = 0 }},
		{"negative read timeout", func(c *ConnConfig) { c.ReadTimeout = -time.Second }},
		{"zero write timeout", func(c *ConnConfig) { c.WriteTimeout = 0 }},
		{"zero pool max open", func(c *ConnConfig) { c.Pool.MaxOpen = 0 }},
		{"negative pool max idle", func(c *ConnConfig) { c.Pool.MaxIdle = -1 }},
		{"idle above open", func(c *ConnConfig) { c.Pool.MaxIdle = c.Pool.MaxOpen + 1 }},
		{"zero pool idle timeout", func(c *ConnConfig) { c.Pool.IdleTimeout = 0 }},
		{"zero pool max lifetime", func(c *ConnConfig) { c.Pool.MaxLifetime = 0 }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := DefaultConnConfig()
			test.mutate(&config)
			if err := config.Validate(); err == nil {
				t.Fatalf("expected %s to be rejected", test.name)
			}
		})
	}
}

func TestConnConfigRedactsPasswordInEveryRendering(t *testing.T) {
	const secret = "sup3r-s3cret"
	config := DefaultConnConfig()
	config.Username = "ncs"
	config.Password = secret

	renderings := map[string]string{
		"String":   config.String(),
		"GoString": config.GoString(),
	}
	var buffer bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buffer, nil))
	logger.Info("connect", "config", config)
	renderings["slog"] = buffer.String()

	for name, rendered := range renderings {
		if strings.Contains(rendered, secret) {
			t.Fatalf("%s leaked the password: %s", name, rendered)
		}
		if !strings.Contains(rendered, "redacted") {
			t.Fatalf("%s must mark the password as redacted: %s", name, rendered)
		}
	}
}

func TestConnConfigRedactionReportsUnsetPassword(t *testing.T) {
	config := DefaultConnConfig()
	if !strings.Contains(config.String(), "password=unset") {
		t.Fatalf("expected an unset password marker, got %s", config.String())
	}
}

func TestConnConfigFromEnvAppliesOverrides(t *testing.T) {
	env := map[string]string{
		envRedisAddress:      "redis.internal:6380",
		envRedisUsername:     "ncs",
		envRedisPassword:     "secret",
		envRedisDatabase:     "3",
		envRedisDialTimeout:  "5s",
		envRedisReadTimeout:  "1500ms",
		envRedisWriteTimeout: "750ms",
		envRedisPoolMaxOpen:  "64",
		envRedisPoolMaxIdle:  "16",
		envRedisPoolIdleTTL:  "2m",
		envRedisPoolLifetime: "30m",
	}
	config, err := ConnConfigFromEnv(func(name string) string { return env[name] })
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	checks := []struct {
		name string
		got  any
		want any
	}{
		{"address", config.Address, "redis.internal:6380"},
		{"username", config.Username, "ncs"},
		{"password", config.Password, "secret"},
		{"database", config.Database, 3},
		{"dial timeout", config.DialTimeout, 5 * time.Second},
		{"read timeout", config.ReadTimeout, 1500 * time.Millisecond},
		{"write timeout", config.WriteTimeout, 750 * time.Millisecond},
		{"pool max open", config.Pool.MaxOpen, 64},
		{"pool max idle", config.Pool.MaxIdle, 16},
		{"pool idle timeout", config.Pool.IdleTimeout, 2 * time.Minute},
		{"pool max lifetime", config.Pool.MaxLifetime, 30 * time.Minute},
	}
	for _, check := range checks {
		if check.got != check.want {
			t.Errorf("expected %s=%v, got %v", check.name, check.want, check.got)
		}
	}
}

func TestConnConfigFromEnvTreatsBlankAsUnset(t *testing.T) {
	// A blank line in an .env file must not blank out a working default.
	config, err := ConnConfigFromEnv(func(name string) string {
		if name == envRedisAddress {
			return "   "
		}
		return ""
	})
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if config.Address != defaultRedisAddress {
		t.Fatalf("expected the default address, got %q", config.Address)
	}
}

func TestConnConfigFromEnvRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{"database", envRedisDatabase, "not-a-number"},
		{"dial timeout", envRedisDialTimeout, "soon"},
		{"pool max open", envRedisPoolMaxOpen, "many"},
		{"negative database", envRedisDatabase, "-1"},
		{"idle above open", envRedisPoolMaxIdle, "1024"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ConnConfigFromEnv(func(name string) string {
				if name == test.key {
					return test.value
				}
				return ""
			})
			if err == nil {
				t.Fatalf("expected %s=%q to be rejected", test.key, test.value)
			}
		})
	}
}
