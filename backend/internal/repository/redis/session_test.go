package redis

import (
	"context"
	"errors"
	"testing"
	"time"
)

func newTestSessions(t *testing.T, commands Commands, policy Policy, stats *Degradation, config SessionConfig) *Sessions {
	t.Helper()
	sessions, err := NewSessions(commands, policy, stats, config, nil)
	if err != nil {
		t.Fatalf("new sessions: %v", err)
	}
	return sessions
}

// newSessionsFixture shares one clock between the store and the sessions, so a
// test that advances time expires the session exactly as production would.
func newSessionsFixture(t *testing.T, policy Policy, stats *Degradation, config SessionConfig) (*MemoryCommands, *Sessions, *testClock) {
	t.Helper()
	clock := newTestClock()
	commands := NewMemoryCommands()
	commands.SetClock(clock.Now)
	sessions, err := NewSessions(commands, policy, stats, config, clock.Now)
	if err != nil {
		t.Fatalf("new sessions: %v", err)
	}
	return commands, sessions, clock
}

func TestNewSessionsValidatesInputs(t *testing.T) {
	valid := DefaultSessionConfig()
	tests := []struct {
		name     string
		commands Commands
		policy   Policy
		config   SessionConfig
	}{
		{"nil commands", nil, DefaultPolicy(), valid},
		{"invalid policy", NewMemoryCommands(), Policy{Session: FailMode(9)}, valid},
		{"zero idle ttl", NewMemoryCommands(), DefaultPolicy(), SessionConfig{IdleTTL: 0, AbsoluteTTL: time.Hour}},
		{"zero absolute ttl", NewMemoryCommands(), DefaultPolicy(), SessionConfig{IdleTTL: time.Minute, AbsoluteTTL: 0}},
		{"absolute shorter than idle", NewMemoryCommands(), DefaultPolicy(), SessionConfig{IdleTTL: time.Hour, AbsoluteTTL: time.Minute}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewSessions(test.commands, test.policy, nil, test.config, nil); err == nil {
				t.Fatal("expected the configuration to be rejected")
			}
		})
	}
}

func TestSessionsSaveLoadDelete(t *testing.T) {
	ctx := context.Background()
	commands, sessions, _ := newSessionsFixture(t, DefaultPolicy(), nil, DefaultSessionConfig())

	if err := sessions.Save(ctx, "sess_01", "user_01"); err != nil {
		t.Fatalf("save: %v", err)
	}
	payload, found, err := sessions.Load(ctx, "sess_01")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !found || payload != "user_01" {
		t.Fatalf("expected user_01, got found=%v payload=%q", found, payload)
	}

	// The session must be stored under the frozen key, so a running process and
	// an operator inspecting Redis agree on where it lives.
	key, _ := SessionKey("sess_01")
	if _, err := commands.Get(ctx, key); err != nil {
		t.Fatalf("expected the session at %s: %v", key, err)
	}

	if err := sessions.Delete(ctx, "sess_01"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, found, err := sessions.Load(ctx, "sess_01"); err != nil || found {
		t.Fatalf("expected the session to be gone, got found=%v err=%v", found, err)
	}
}

// Logout must be idempotent: a retried logout is not an error.
func TestSessionsDeleteIsIdempotent(t *testing.T) {
	ctx := context.Background()
	sessions := newTestSessions(t, NewMemoryCommands(), DefaultPolicy(), nil, DefaultSessionConfig())

	if err := sessions.Delete(ctx, "sess_absent"); err != nil {
		t.Fatalf("expected deleting a missing session to succeed, got %v", err)
	}
}

// An empty payload is a legitimate stored value, so it must not be reported as a
// miss. Conflating the two would log a user out for an empty session payload.
func TestSessionsLoadDistinguishesEmptyPayloadFromMissingSession(t *testing.T) {
	ctx := context.Background()
	sessions := newTestSessions(t, NewMemoryCommands(), DefaultPolicy(), nil, DefaultSessionConfig())

	if err := sessions.Save(ctx, "sess_empty", ""); err != nil {
		t.Fatalf("save: %v", err)
	}
	payload, found, err := sessions.Load(ctx, "sess_empty")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !found {
		t.Fatal("an empty payload must still report a found session")
	}
	if payload != "" {
		t.Fatalf("expected an empty payload, got %q", payload)
	}

	if _, found, err := sessions.Load(ctx, "sess_absent"); err != nil || found {
		t.Fatalf("expected a missing session to report found=false, got found=%v err=%v", found, err)
	}
}

func TestSessionsIdleWindowExpiresWithoutUse(t *testing.T) {
	ctx := context.Background()
	config := SessionConfig{IdleTTL: 10 * time.Minute, AbsoluteTTL: time.Hour}
	_, sessions, clock := newSessionsFixture(t, DefaultPolicy(), nil, config)

	if err := sessions.Save(ctx, "sess_01", "user_01"); err != nil {
		t.Fatalf("save: %v", err)
	}
	clock.Advance(10 * time.Minute)

	if _, found, err := sessions.Load(ctx, "sess_01"); err != nil || found {
		t.Fatalf("expected the idle window to expire the session, got found=%v err=%v", found, err)
	}
}

func TestSessionsRefreshKeepsAnActiveSessionAlive(t *testing.T) {
	ctx := context.Background()
	config := SessionConfig{IdleTTL: 10 * time.Minute, AbsoluteTTL: time.Hour}
	_, sessions, clock := newSessionsFixture(t, DefaultPolicy(), nil, config)

	if err := sessions.Save(ctx, "sess_01", "user_01"); err != nil {
		t.Fatalf("save: %v", err)
	}

	// Use the session every 9 minutes, inside the 10 minute idle window. The
	// session must survive well past the idle window, which is the point of a
	// sliding refresh.
	for i := 0; i < 4; i++ {
		clock.Advance(9 * time.Minute)
		refreshed, err := sessions.Refresh(ctx, "sess_01")
		if err != nil {
			t.Fatalf("refresh %d: %v", i, err)
		}
		if !refreshed {
			t.Fatalf("refresh %d should have kept the session alive", i)
		}
		if _, found, err := sessions.Load(ctx, "sess_01"); err != nil || !found {
			t.Fatalf("load %d: expected the session to survive, got found=%v err=%v", i, found, err)
		}
	}
}

// This is the invariant the previous revision of this module got wrong: a
// refresh loop must not be able to extend a session forever. The absolute
// deadline is fixed when the session is created and no refresh may move it.
func TestSessionsRefreshLoopCannotExceedTheAbsoluteDeadline(t *testing.T) {
	ctx := context.Background()
	config := SessionConfig{IdleTTL: 10 * time.Minute, AbsoluteTTL: 30 * time.Minute}
	_, sessions, clock := newSessionsFixture(t, DefaultPolicy(), nil, config)

	if err := sessions.Save(ctx, "sess_01", "user_01"); err != nil {
		t.Fatalf("save: %v", err)
	}
	deadline, found, err := sessions.AbsoluteDeadline(ctx, "sess_01")
	if err != nil || !found {
		t.Fatalf("absolute deadline: found=%v err=%v", found, err)
	}
	wantDeadline := clock.Now().Add(config.AbsoluteTTL)
	if !deadline.Equal(wantDeadline) {
		t.Fatalf("expected the deadline to be fixed at %s, got %s", wantDeadline, deadline)
	}

	// Refresh as often as possible, always inside the idle window. The session
	// must still die at the absolute deadline.
	refreshes := 0
	for elapsed := time.Duration(0); elapsed < 2*config.AbsoluteTTL; elapsed += 9 * time.Minute {
		clock.Advance(9 * time.Minute)
		refreshed, err := sessions.Refresh(ctx, "sess_01")
		if err != nil {
			t.Fatalf("refresh at %s: %v", elapsed, err)
		}
		if refreshed {
			refreshes++
			continue
		}
		// The refresh must fail only because the ceiling passed.
		if clock.Now().Before(wantDeadline) {
			t.Fatalf("refresh was refused at %s before the absolute deadline %s", clock.Now(), wantDeadline)
		}
	}

	if refreshes == 0 {
		t.Fatal("expected at least one refresh to succeed")
	}
	if _, found, err := sessions.Load(ctx, "sess_01"); err != nil || found {
		t.Fatalf("expected the session to be gone after the absolute deadline, got found=%v err=%v", found, err)
	}
}

// A read past the absolute deadline must end the session even if its stored TTL
// would still consider it alive.
func TestSessionsLoadRejectsASessionPastItsAbsoluteDeadline(t *testing.T) {
	ctx := context.Background()
	config := SessionConfig{IdleTTL: 5 * time.Minute, AbsoluteTTL: 20 * time.Minute}
	commands, sessions, clock := newSessionsFixture(t, DefaultPolicy(), nil, config)

	if err := sessions.Save(ctx, "sess_01", "user_01"); err != nil {
		t.Fatalf("save: %v", err)
	}
	key, _ := SessionKey("sess_01")

	// Simulate a TTL that was set too generously: the key stays present well past
	// the absolute deadline, so only the envelope can enforce the ceiling.
	if _, err := commands.Expire(ctx, key, time.Hour); err != nil {
		t.Fatalf("expire: %v", err)
	}
	clock.Advance(config.AbsoluteTTL + time.Second)

	if _, found, err := sessions.Load(ctx, "sess_01"); err != nil || found {
		t.Fatalf("expected the absolute deadline to end the session, got found=%v err=%v", found, err)
	}
	// The session must be removed, not merely hidden.
	if _, err := commands.Get(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected the expired session to be deleted, got %v", err)
	}
}

func TestSessionsSaveWithIdleTTLCannotOutliveTheAbsoluteDeadline(t *testing.T) {
	ctx := context.Background()
	config := SessionConfig{IdleTTL: 10 * time.Minute, AbsoluteTTL: 30 * time.Minute}
	commands, sessions, clock := newSessionsFixture(t, DefaultPolicy(), nil, config)

	// Asking for an idle window longer than the ceiling is clamped, not honoured.
	if err := sessions.SaveWithIdleTTL(ctx, "sess_01", "user_01", 4*time.Hour); err != nil {
		t.Fatalf("save: %v", err)
	}
	key, _ := SessionKey("sess_01")
	ttl, hasExpiry, err := commands.TTL(ctx, key)
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if !hasExpiry || ttl > config.AbsoluteTTL {
		t.Fatalf("expected the ttl clamped to the ceiling %s, got ttl=%s hasExpiry=%v", config.AbsoluteTTL, ttl, hasExpiry)
	}

	if err := sessions.SaveWithIdleTTL(ctx, "sess_02", "user_01", 0); err == nil {
		t.Fatal("expected a zero idle ttl to be rejected")
	}

	clock.Advance(config.AbsoluteTTL + time.Second)
	if _, found, err := sessions.Load(ctx, "sess_01"); err != nil || found {
		t.Fatalf("expected the clamped session to end at the ceiling, got found=%v err=%v", found, err)
	}
}

func TestSessionsRefreshReportsMissingSession(t *testing.T) {
	ctx := context.Background()
	sessions := newTestSessions(t, NewMemoryCommands(), DefaultPolicy(), nil, DefaultSessionConfig())

	refreshed, err := sessions.Refresh(ctx, "sess_absent")
	if err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if refreshed {
		t.Fatal("expected refresh to report false for a missing session")
	}
}

// A value this package cannot decode must never be served as a valid session, and
// must be removed so the client is forced to log in again.
func TestSessionsRejectCorruptStoredValue(t *testing.T) {
	ctx := context.Background()
	config := SessionConfig{IdleTTL: 10 * time.Minute, AbsoluteTTL: time.Hour}
	commands, sessions, _ := newSessionsFixture(t, DefaultPolicy(), nil, config)
	key, _ := SessionKey("sess_01")

	if err := commands.Set(ctx, key, "not-json", time.Hour); err != nil {
		t.Fatalf("set: %v", err)
	}
	if _, found, err := sessions.Load(ctx, "sess_01"); err == nil || found {
		t.Fatalf("expected a corrupt session to be rejected, got found=%v err=%v", found, err)
	}
	if _, err := commands.Get(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected the corrupt session to be deleted, got %v", err)
	}
}

// An envelope without a deadline cannot enforce a ceiling, so it is treated as
// corrupt rather than trusted.
func TestSessionsRejectEnvelopeWithoutDeadline(t *testing.T) {
	ctx := context.Background()
	config := SessionConfig{IdleTTL: 10 * time.Minute, AbsoluteTTL: time.Hour}
	commands, sessions, _ := newSessionsFixture(t, DefaultPolicy(), nil, config)
	key, _ := SessionKey("sess_01")

	if err := commands.Set(ctx, key, `{"payload":"user_01"}`, time.Hour); err != nil {
		t.Fatalf("set: %v", err)
	}
	if _, found, err := sessions.Load(ctx, "sess_01"); err == nil || found {
		t.Fatalf("expected an envelope without a deadline to be rejected, got found=%v err=%v", found, err)
	}
}

func TestSessionsRejectEmptyIdentifier(t *testing.T) {
	ctx := context.Background()
	sessions := newTestSessions(t, NewMemoryCommands(), DefaultPolicy(), nil, DefaultSessionConfig())

	if err := sessions.Save(ctx, "", "user_01"); err == nil {
		t.Fatal("expected an error for an empty session id")
	}
	if _, _, err := sessions.Load(ctx, ""); err == nil {
		t.Fatal("expected an error for an empty session id")
	}
	if err := sessions.Delete(ctx, ""); err == nil {
		t.Fatal("expected an error for an empty session id")
	}
}

// This is the security-critical default. An unverifiable login must never be
// accepted, so a Redis outage must surface rather than look like "no session".
func TestSessionsFailClosedOnOutage(t *testing.T) {
	ctx := context.Background()
	stats := NewDegradation()
	commands, sessions, _ := newSessionsFixture(t, DefaultPolicy(), stats, DefaultSessionConfig())

	if err := sessions.Save(ctx, "sess_01", "user_01"); err != nil {
		t.Fatalf("save: %v", err)
	}
	commands.SetDown(errors.New("connection refused"))

	_, _, err := sessions.Load(ctx, "sess_01")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected ErrUnavailable so the caller can answer 401 or 503, got %v", err)
	}
	if got := stats.Failures(CapabilitySession); got != 1 {
		t.Fatalf("expected the outage to be counted, got %d", got)
	}

	if err := sessions.Save(ctx, "sess_01", "user_01"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected save to surface the outage, got %v", err)
	}
}

// A caller that explicitly widens the session policy gets the safe zero value:
// "no session", never "authenticated".
func TestSessionsFailOpenReportsNoSession(t *testing.T) {
	ctx := context.Background()
	policy := DefaultPolicy()
	policy.Session = FailOpen
	commands, sessions, _ := newSessionsFixture(t, policy, nil, DefaultSessionConfig())

	commands.SetDown(errors.New("connection refused"))
	payload, found, err := sessions.Load(ctx, "sess_01")
	if err != nil {
		t.Fatalf("expected the outage to be hidden, got %v", err)
	}
	if found || payload != "" {
		t.Fatalf("a degraded session read must never look authenticated, got found=%v payload=%q", found, payload)
	}
}
