package redis

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestDefaultSettingsIsValid(t *testing.T) {
	settings := DefaultSettings()
	if err := settings.Validate(); err != nil {
		t.Fatalf("default settings must be valid: %v", err)
	}
	if !settings.Required {
		t.Fatal("redis must be required by default, because session and lock fail closed")
	}
	if settings.Policy.Lock != FailClosed {
		t.Fatal("the approved lock default is FailClosed")
	}
}

func TestSettingsValidateRejectsIncoherentCombinations(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Settings)
	}{
		{"invalid connection", func(s *Settings) { s.Conn.Address = "" }},
		{"invalid policy", func(s *Settings) { s.Policy.Cache = FailMode(9) }},
		{"zero cache ttl", func(s *Settings) { s.Cache.DefaultTTL = 0 }},
		{"absolute shorter than idle", func(s *Settings) { s.Session.IdleTTL = time.Hour; s.Session.AbsoluteTTL = time.Minute }},
		{"zero rate limit", func(s *Settings) { s.Limit.Requests = 0 }},
		{"zero window", func(s *Settings) { s.Limit.Window = 0 }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			settings := DefaultSettings()
			test.mutate(&settings)
			if err := settings.Validate(); err == nil {
				t.Fatal("expected the settings to be rejected")
			}
		})
	}
}

func envMap(values map[string]string) func(string) string {
	return func(name string) string { return values[name] }
}

func TestSettingsFromEnvAppliesEveryOverride(t *testing.T) {
	settings, err := SettingsFromEnv(envMap(map[string]string{
		envRedisAddress:       "redis.internal:6380",
		envRedisDatabase:      "2",
		envPolicyCache:        "fail-closed",
		envPolicySession:      "fail-open",
		envPolicyRateLimit:    "fail-closed",
		envPolicyLock:         "fail-closed",
		envCacheTTL:           "2m",
		envSessionIdleTTL:     "45m",
		envSessionAbsoluteTTL: "6h",
		envRateLimitRequests:  "7",
		envRateLimitWindow:    "30s",
		envRedisRequired:      "false",
	}))
	if err != nil {
		t.Fatalf("load settings: %v", err)
	}

	if settings.Conn.Address != "redis.internal:6380" || settings.Conn.Database != 2 {
		t.Fatalf("unexpected connection settings %+v", settings.Conn)
	}
	if settings.Policy.Cache != FailClosed {
		t.Error("expected the cache policy override to apply")
	}
	if settings.Policy.Session != FailOpen {
		t.Error("expected the session policy override to apply")
	}
	if settings.Policy.RateLimit != FailClosed {
		t.Error("expected the rate limit policy override to apply")
	}
	if settings.Policy.Lock != FailClosed {
		t.Error("expected the lock policy override to apply")
	}
	if settings.Cache.DefaultTTL != 2*time.Minute {
		t.Errorf("unexpected cache ttl %s", settings.Cache.DefaultTTL)
	}
	if settings.Session.IdleTTL != 45*time.Minute || settings.Session.AbsoluteTTL != 6*time.Hour {
		t.Errorf("unexpected session settings %+v", settings.Session)
	}
	if settings.Limit.Requests != 7 || settings.Limit.Window != 30*time.Second {
		t.Errorf("unexpected rate limit %+v", settings.Limit)
	}
	if settings.Required {
		t.Error("expected NCS_REDIS_REQUIRED=false to apply")
	}
}

func TestSettingsFromEnvAcceptsFailModeAliasesAndCase(t *testing.T) {
	for _, raw := range []string{"fail-open", "FAIL-OPEN", "open", "Open"} {
		settings, err := SettingsFromEnv(envMap(map[string]string{envPolicyCache: raw}))
		if err != nil {
			t.Fatalf("%q: %v", raw, err)
		}
		if settings.Policy.Cache != FailOpen {
			t.Errorf("expected %q to select FailOpen", raw)
		}
	}
	for _, raw := range []string{"fail-closed", "FAIL-CLOSED", "closed"} {
		settings, err := SettingsFromEnv(envMap(map[string]string{envPolicyCache: raw}))
		if err != nil {
			t.Fatalf("%q: %v", raw, err)
		}
		if settings.Policy.Cache != FailClosed {
			t.Errorf("expected %q to select FailClosed", raw)
		}
	}
}

func TestSettingsFromEnvRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{"unknown fail mode", envPolicyLock, "maybe"},
		{"empty-ish fail mode", envPolicySession, "-"},
		{"invalid cache ttl", envCacheTTL, "soon"},
		{"invalid session idle ttl", envSessionIdleTTL, "later"},
		{"invalid request count", envRateLimitRequests, "many"},
		{"invalid required flag", envRedisRequired, "perhaps"},
		{"absolute shorter than idle", envSessionAbsoluteTTL, "1m"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := SettingsFromEnv(envMap(map[string]string{test.key: test.value}))
			if err == nil {
				t.Fatalf("expected %s=%q to be rejected", test.key, test.value)
			}
		})
	}
}

func TestSettingsFromEnvTreatsBlankAsUnset(t *testing.T) {
	settings, err := SettingsFromEnv(envMap(map[string]string{
		envPolicyCache: "   ",
		envCacheTTL:    "",
	}))
	if err != nil {
		t.Fatalf("load settings: %v", err)
	}
	if settings.Policy.Cache != DefaultPolicy().Cache {
		t.Fatal("a blank value must not change the default policy")
	}
	if settings.Cache.DefaultTTL != DefaultCacheConfig().DefaultTTL {
		t.Fatal("a blank value must not change the default ttl")
	}
}

func TestPolicySummaryReportsEveryMode(t *testing.T) {
	settings := DefaultSettings()
	summary := settings.PolicySummary()
	for _, fragment := range []string{
		"cache=fail-open",
		"session=fail-closed",
		"rate-limit=fail-open",
		"lock=fail-closed",
	} {
		if !strings.Contains(summary, fragment) {
			t.Errorf("expected the summary to contain %q, got %q", fragment, summary)
		}
	}
}

// This is the test that makes "change the configuration, not the code" real: the
// same code path must behave differently once the policy comes from the
// environment.
func TestSettingsFromEnvChangesRuntimeDegradationBehaviour(t *testing.T) {
	ctx := context.Background()

	t.Run("default fail-closed session refuses while redis is down", func(t *testing.T) {
		settings := DefaultSettings()
		commands := NewMemoryCommands()
		capabilities, err := newCapabilitiesOver(commands, NewMemoryStream(), settings)
		if err != nil {
			t.Fatalf("new capabilities: %v", err)
		}
		defer capabilities.Close()

		commands.SetDown(errors.New("connection refused"))
		if _, _, err := capabilities.Sessions.Load(ctx, "sess_01"); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("expected ErrUnavailable under the default policy, got %v", err)
		}
	})

	t.Run("NCS_REDIS_FAILMODE_SESSION=fail-open degrades instead", func(t *testing.T) {
		settings, err := SettingsFromEnv(envMap(map[string]string{
			envPolicySession: "fail-open",
		}))
		if err != nil {
			t.Fatalf("load settings: %v", err)
		}
		commands := NewMemoryCommands()
		capabilities, err := newCapabilitiesOver(commands, NewMemoryStream(), settings)
		if err != nil {
			t.Fatalf("new capabilities: %v", err)
		}
		defer capabilities.Close()

		commands.SetDown(errors.New("connection refused"))
		payload, found, err := capabilities.Sessions.Load(ctx, "sess_01")
		if err != nil {
			t.Fatalf("expected the configured policy to hide the outage, got %v", err)
		}
		// Degrading must never look like a valid login.
		if found || payload != "" {
			t.Fatalf("expected no session, got found=%v payload=%q", found, payload)
		}
		if got := capabilities.Stats.Failures(CapabilitySession); got != 1 {
			t.Fatalf("expected the hidden failure to be counted, got %d", got)
		}
	})
}

func TestCapabilitiesExposeEveryFoundationCapability(t *testing.T) {
	settings := DefaultSettings()
	commands := NewMemoryCommands()
	capabilities, err := newCapabilitiesOver(commands, NewMemoryStream(), settings)
	if err != nil {
		t.Fatalf("new capabilities: %v", err)
	}
	defer capabilities.Close()

	if capabilities.Cache == nil || capabilities.Sessions == nil || capabilities.Limiter == nil ||
		capabilities.Locker == nil || capabilities.Health == nil || capabilities.Stats == nil {
		t.Fatal("expected every capability to be constructed")
	}
	if err := capabilities.Ready(context.Background()); err != nil {
		t.Fatalf("ready: %v", err)
	}
}

func TestCapabilitiesReadyReportsAnOutage(t *testing.T) {
	settings := DefaultSettings()
	commands := NewMemoryCommands()
	capabilities, err := newCapabilitiesOver(commands, NewMemoryStream(), settings)
	if err != nil {
		t.Fatalf("new capabilities: %v", err)
	}
	defer capabilities.Close()

	commands.SetDown(errors.New("connection refused"))
	if err := capabilities.Ready(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected ErrUnavailable, got %v", err)
	}
}

func TestNewCapabilitiesRejectsInvalidSettings(t *testing.T) {
	settings := DefaultSettings()
	settings.Cache.DefaultTTL = 0
	if _, err := NewCapabilities(settings); err == nil {
		t.Fatal("expected invalid settings to be rejected")
	}
}
