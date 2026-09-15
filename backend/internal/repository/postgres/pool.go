package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	// The pgx stdlib driver registers "pgx" with database/sql.
	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	pingTimeout = 5 * time.Second
	connMaxLife = 30 * time.Minute
	connMaxIdle = 5 * time.Minute
)

// Open connects to PostgreSQL through the pgx driver and applies conservative
// pool settings. Callers own Close.
func Open(ctx context.Context, dsn string, maxConns int) (*sql.DB, error) {
	if dsn == "" {
		return nil, fmt.Errorf("postgres: DSN is empty")
	}
	if maxConns < 1 {
		maxConns = 1
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: open connection: %w", err)
	}
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	db.SetConnMaxLifetime(connMaxLife)
	db.SetConnMaxIdleTime(connMaxIdle)

	pingContext, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	if err := db.PingContext(pingContext); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}
	return db, nil
}
