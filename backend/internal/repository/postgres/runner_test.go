package postgres

import (
	"context"
	"errors"
	"testing"
	testingfs "testing/fstest"
)

func singleFileFS() testingfs.MapFS {
	return testingfs.MapFS{
		"0001_first.sql":  &testingfs.MapFile{Data: []byte("CREATE TABLE first (id integer);\n")},
		"0002_second.sql": &testingfs.MapFile{Data: []byte("CREATE TABLE second (id integer);\n")},
		"notes.txt":       &testingfs.MapFile{Data: []byte("ignored")},
	}
}

func TestRunAppliesInOrderAndIsIdempotent(t *testing.T) {
	db := newFakeDB()
	first, err := Run(context.Background(), db, singleFileFS())
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	if len(first.Applied) != 2 || first.Applied[0].Version != 1 || first.Applied[1].Version != 2 {
		t.Fatalf("unexpected first report: %+v", first.Applied)
	}
	session := db.session
	if session.transactions() != 2 {
		t.Fatalf("per-file transactions = %d, want 2", session.transactions())
	}
	if session.closed != 1 {
		t.Fatalf("session closes = %d, want 1", session.closed)
	}

	second, err := Run(context.Background(), db, singleFileFS())
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if len(second.Applied) != 0 {
		t.Fatalf("expected no migrations on second run, got %+v", second.Applied)
	}
	if db.sessions != 2 {
		t.Fatalf("sessions opened = %d, want one per run", db.sessions)
	}
}

func TestRunAcquiresLockOnTheSessionBeforeWorking(t *testing.T) {
	db := newFakeDB()
	if _, err := Run(context.Background(), db, singleFileFS()); err != nil {
		t.Fatalf("run: %v", err)
	}
	session := db.session
	if session.locks != 1 {
		t.Fatalf("lock acquisitions = %d, want 1", session.locks)
	}
	if len(session.order) == 0 || session.order[0] != "lock" {
		t.Fatalf("call order = %v, want lock first", session.order)
	}
	if session.unlocked != 1 {
		t.Fatalf("unlock calls = %d, want exactly 1 on close", session.unlocked)
	}
}

func TestRunFailsWhenLockCannotBeAcquired(t *testing.T) {
	db := newFakeDB()
	db.lockErr = errors.New("another runner holds the lock")

	if _, err := Run(context.Background(), db, singleFileFS()); err == nil {
		t.Fatal("expected lock failure to abort the run")
	}
	if len(db.session.applied) != 0 {
		t.Fatalf("migrations applied despite lock failure: %v", db.session.applied)
	}
	if db.session.closed != 1 {
		t.Fatal("session was not closed after lock failure")
	}
}

func TestRunRejectsChangedAppliedMigration(t *testing.T) {
	db := newFakeDB()
	db.session.applied = map[int]AppliedMigration{1: {Version: 1, Name: "first.sql", Checksum: "old"}}

	_, err := Run(context.Background(), db, singleFileFS())
	if err == nil {
		t.Fatal("expected changed migration error")
	}
}

func TestRunRejectsDuplicateVersions(t *testing.T) {
	db := newFakeDB()
	migrations := testingfs.MapFS{
		"0001_first.sql":  &testingfs.MapFile{Data: []byte("SELECT 1;")},
		"0001_second.sql": &testingfs.MapFile{Data: []byte("SELECT 2;")},
	}

	if _, err := Run(context.Background(), db, migrations); err == nil {
		t.Fatal("expected duplicate version error")
	}
}

func TestRunRollsBackFailedMigrationFile(t *testing.T) {
	db := newFakeDB()
	db.session.execErrOn = "CREATE TABLE second"

	_, err := Run(context.Background(), db, singleFileFS())
	if err == nil {
		t.Fatal("expected migration SQL failure to abort the run")
	}
	session := db.session
	if session.rollbacks != 1 {
		t.Fatalf("rollbacks = %d, want exactly 1 for the failed file", session.rollbacks)
	}
	// The first file must have committed before the failure.
	if len(session.committed) != 1 || session.committed[0] != 1 {
		t.Fatalf("committed files = %v, want [1]", session.committed)
	}
}

func TestRunRejectsNilInputs(t *testing.T) {
	if _, err := Run(context.Background(), nil, singleFileFS()); err == nil {
		t.Fatal("nil database accepted")
	}
	if _, err := Run(context.Background(), newFakeDB(), nil); err == nil {
		t.Fatal("nil filesystem accepted")
	}
}

// fakeDB hands out one recording session per Run call.
type fakeDB struct {
	session    *fakeSession
	sessions   int
	sessionErr error
	lockErr    error
}

func newFakeDB() *fakeDB {
	db := &fakeDB{}
	db.session = &fakeSession{db: db, applied: make(map[int]AppliedMigration)}
	return db
}

func (db *fakeDB) Session(context.Context) (MigrationSession, error) {
	db.sessions++
	if db.sessionErr != nil {
		return nil, db.sessionErr
	}
	return db.session, nil
}

type fakeSession struct {
	db        *fakeDB
	applied   map[int]AppliedMigration
	order     []string
	locks     int
	unlocked  int
	closed    int
	begins    int
	commits   int
	rollbacks int
	committed []int
	inTx      bool
	execErrOn string
	lockErr   error
}

func (s *fakeSession) transactions() int { return s.begins }

func (s *fakeSession) AcquireLock(context.Context) error {
	s.order = append(s.order, "lock")
	s.locks++
	if s.lockErr != nil {
		return s.lockErr
	}
	if s.db.lockErr != nil {
		return s.db.lockErr
	}
	return nil
}

func (s *fakeSession) AppliedMigrations(context.Context) (map[int]AppliedMigration, error) {
	s.order = append(s.order, "applied")
	result := make(map[int]AppliedMigration, len(s.applied))
	for version, migration := range s.applied {
		result[version] = migration
	}
	return result, nil
}

func (s *fakeSession) Exec(_ context.Context, query string, args ...any) error {
	if s.execErrOn != "" && len(query) >= len(s.execErrOn) && query[:len(s.execErrOn)] == s.execErrOn {
		return errors.New("boom: " + s.execErrOn)
	}
	if len(args) == 3 && query[:minInt(len(query), len("INSERT INTO schema_migrations"))] == "INSERT INTO schema_migrations" {
		version, ok := args[0].(int)
		if !ok {
			return errors.New("invalid fake migration version")
		}
		name, ok := args[1].(string)
		if !ok {
			return errors.New("invalid fake migration name")
		}
		checksum, ok := args[2].(string)
		if !ok {
			return errors.New("invalid fake migration checksum")
		}
		s.applied[version] = AppliedMigration{Version: version, Name: name, Checksum: checksum}
	}
	return nil
}

func (s *fakeSession) BeginTransaction(context.Context) error {
	s.begins++
	s.inTx = true
	s.order = append(s.order, "begin")
	return nil
}

func (s *fakeSession) CommitTransaction(context.Context) error {
	s.commits++
	s.inTx = false
	s.order = append(s.order, "commit")
	// Inside the single-session fake the highest recorded version is the
	// file that just committed.
	s.committed = append(s.committed, s.lastAppliedVersion())
	return nil
}

func (s *fakeSession) RollbackTransaction(context.Context) error {
	s.rollbacks++
	s.inTx = false
	s.order = append(s.order, "rollback")
	return nil
}

func (s *fakeSession) Close(context.Context) error {
	s.closed++
	s.unlocked++
	return nil
}

// lastAppliedVersion returns the highest recorded version, which inside the
// single-session fake equals the version of the file that just committed.
func (s *fakeSession) lastAppliedVersion() int {
	highest := 0
	for version := range s.applied {
		if version > highest {
			highest = version
		}
	}
	return highest
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
