package auth

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	bredis "github.com/heguangV/charging-station-platform/backend/internal/repository/redis"
)

// redisCommands returns a live command surface when NCS_TEST_REDIS_ADDR is
// set; tests skip otherwise. Keys are namespaced per run and cleaned up.
func redisCommands(t *testing.T) (bredis.Commands, func()) {
	t.Helper()
	addr := os.Getenv("NCS_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("NCS_TEST_REDIS_ADDR not set; Redis integration tests skipped")
	}
	config := bredis.DefaultConnConfig()
	config.Address = addr
	client, err := bredis.NewClient(config)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	commands, err := client.Open(context.Background(), config)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	return commands, func() { _ = client.Close() }
}

func TestSMSCodeAtomicVerifyOnRealRedis(t *testing.T) {
	commands, cleanup := redisCommands(t)
	defer cleanup()
	store, err := NewRedisSMSCodeStore(commands)
	if err != nil {
		t.Fatalf("NewRedisSMSCodeStore() error = %v", err)
	}
	ctx := context.Background()
	phone := "13600001111"

	if err := store.Issue(ctx, phone, "123456", time.Minute); err != nil {
		t.Fatalf("Issue() error = %v", err)
	}

	// Wrong codes advance the failure budget without consuming the code.
	for attempt := 1; attempt <= 4; attempt++ {
		verified, lockedOut, found, err := store.Verify(ctx, phone, "000000", 5)
		if err != nil || verified || !found {
			t.Fatalf("wrong verify %d = %v/%v/%v, %v", attempt, verified, lockedOut, found, err)
		}
	}
	if _, found := store.Peek(ctx, phone); !found {
		t.Fatal("code vanished before the failure budget was exhausted")
	}

	// The fifth wrong submission voids the code.
	verified, lockedOut, _, err := store.Verify(ctx, phone, "000000", 5)
	if err != nil || verified || !lockedOut {
		t.Fatalf("fifth wrong verify = %v/%v, %v; want locked out", verified, lockedOut, err)
	}
	if _, found := store.Peek(ctx, phone); found {
		t.Fatal("code survived the lockout")
	}

	// A fresh code verifies exactly once.
	if err := store.Issue(ctx, phone, "654321", time.Minute); err != nil {
		t.Fatalf("re-issue: %v", err)
	}
	verified, _, _, err = store.Verify(ctx, phone, "654321", 5)
	if err != nil || !verified {
		t.Fatalf("correct verify = %v, %v", verified, err)
	}
	verified, _, found, err := store.Verify(ctx, phone, "654321", 5)
	if err != nil || verified || found {
		t.Fatalf("replayed verify = %v/%v, %v; want consumed", verified, found, err)
	}
}

func TestSMSCodeVerifyIsAtomicUnderConcurrency(t *testing.T) {
	commands, cleanup := redisCommands(t)
	defer cleanup()
	store, err := NewRedisSMSCodeStore(commands)
	if err != nil {
		t.Fatalf("NewRedisSMSCodeStore() error = %v", err)
	}
	ctx := context.Background()
	phone := "13600002222"

	if err := store.Issue(ctx, phone, "111222", time.Minute); err != nil {
		t.Fatalf("Issue() error = %v", err)
	}

	// Ten concurrent submissions of the same correct code: the atomic script
	// must let exactly one through.
	const racers = 10
	start := make(chan struct{})
	var group sync.WaitGroup
	var winners int64
	var mutex sync.Mutex
	for index := 0; index < racers; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			verified, _, _, err := store.Verify(ctx, phone, "111222", 5)
			if err != nil {
				t.Errorf("verify error: %v", err)
				return
			}
			if verified {
				mutex.Lock()
				winners++
				mutex.Unlock()
			}
		}()
	}
	close(start)
	group.Wait()

	if winners != 1 {
		t.Fatalf("atomic verify winners = %d, want exactly 1", winners)
	}
}
