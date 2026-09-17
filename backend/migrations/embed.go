// Package migrations embeds the SQL migration files so a single binary can
// initialize an empty database without host filesystem access.
package migrations

import "embed"

// FS carries every numbered .sql file in this directory.
//
//go:embed *.sql
var FS embed.FS
