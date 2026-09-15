package auth

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestInMemoryStoreSaveLoadDelete(t *testing.T) {
	store := NewInMemorySessionStore(time.Minute, nil)
	ctx := context.Background()

	session := Session{IdentityID: 7, Role: RoleUser, DisplayName: "开发用户", ExpiresAt: time.Now().Add(time.Hour)}
	if err := store.Save(ctx, "token-1", session); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	loaded, err := store.Load(ctx, "token-1")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if loaded.IdentityID != 7 || loaded.Role != RoleUser || loaded.DisplayName != "开发用户" {
		t.Fatalf("loaded session = %#v", loaded)
	}

	if err := store.Delete(ctx, "token-1"); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if _, err := store.Load(ctx, "token-1"); err != nil && err.Error() != ErrSessionNotFound.Error() {
		t.Fatalf("Load() after delete error = %v", err)
	} else if err == nil {
		t.Fatal("Load() after delete returned a session")
	}

	// Deleting an unknown token stays a no-op.
	if err := store.Delete(ctx, "token-unknown"); err != nil {
		t.Fatalf("Delete(unknown) error = %v", err)
	}
}

func TestInMemoryStoreAbsoluteExpiry(t *testing.T) {
	now := time.Now()
	clock := func() time.Time { return now }
	store := NewInMemorySessionStore(time.Minute, clock)
	ctx := context.Background()

	if err := store.Save(ctx, "token-1", Session{IdentityID: 1, ExpiresAt: now.Add(time.Hour)}); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	now = now.Add(2 * time.Hour) // past the absolute deadline
	if _, err := store.Load(ctx, "token-1"); err != nil && err.Error() != ErrSessionNotFound.Error() {
		t.Fatalf("expired Load error = %v", err)
	} else if err == nil {
		t.Fatal("expired session still loads")
	}
}

func TestInMemoryStoreIdleExpiryAndTouch(t *testing.T) {
	now := time.Now()
	clock := func() time.Time { return now }
	store := NewInMemorySessionStore(10*time.Minute, clock)
	ctx := context.Background()

	if err := store.Save(ctx, "token-1", Session{IdentityID: 1, ExpiresAt: now.Add(time.Hour)}); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	now = now.Add(5 * time.Minute)
	if _, err := store.Load(ctx, "token-1"); err != nil {
		t.Fatalf("Load() within idle window error = %v", err)
	}

	// The load touched the cursor: another 5 idle minutes are fine.
	now = now.Add(5 * time.Minute)
	if _, err := store.Load(ctx, "token-1"); err != nil {
		t.Fatalf("Load() after touch error = %v", err)
	}

	// Past the touched idle window the session expires even though the
	// absolute deadline is far away.
	now = now.Add(11 * time.Minute)
	if _, err := store.Load(ctx, "token-1"); err == nil {
		t.Fatal("idle-expired session still loads")
	}
}

func TestInMemoryStoreConcurrentAccess(t *testing.T) {
	store := NewInMemorySessionStore(time.Minute, nil)
	ctx := context.Background()

	var group sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		group.Add(1)
		go func(worker int) {
			defer group.Done()
			token := "token-" + time.Duration(worker).String()
			for round := 0; round < 50; round++ {
				_ = store.Save(ctx, token, Session{IdentityID: int64(worker), ExpiresAt: time.Now().Add(time.Minute)})
				if _, err := store.Load(ctx, token); err != nil {
					t.Errorf("Load() error = %v", err)
					return
				}
				_ = store.Delete(ctx, token)
			}
		}(worker)
	}
	group.Wait()
}

func TestInMemoryStoreRejectsEmptyToken(t *testing.T) {
	store := NewInMemorySessionStore(time.Minute, nil)
	if err := store.Save(context.Background(), "", Session{IdentityID: 1}); err == nil {
		t.Fatal("empty token accepted")
	}
}
