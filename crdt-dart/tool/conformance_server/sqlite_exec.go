package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"regexp"

	_ "modernc.org/sqlite"

	"github.com/xraph/grove/crdt"
)

// MetadataStore writes PostgreSQL-style $1 placeholders; SQLite reads ?1.
var dollarParam = regexp.MustCompile(`\$(\d+)`)

// sqliteExec adapts database/sql over modernc.org/sqlite to crdt.Executor.
type sqliteExec struct{ db *sql.DB }

func openSQLite() (*sqliteExec, error) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, err
	}
	// One connection keeps the in-memory database alive and serialises writes.
	db.SetMaxOpenConns(1)
	return &sqliteExec{db: db}, nil
}

func rewrite(q string) string { return dollarParam.ReplaceAllString(q, "?$1") }

func (e *sqliteExec) ExecContext(ctx context.Context, q string, args ...any) (crdt.ExecResult, error) {
	return e.db.ExecContext(ctx, rewrite(q), args...)
}

func (e *sqliteExec) QueryContext(ctx context.Context, q string, args ...any) (crdt.Rows, error) {
	rows, err := e.db.QueryContext(ctx, rewrite(q), args...)
	if err != nil {
		return nil, err
	}
	return &sqliteRows{Rows: rows}, nil
}

type sqliteRows struct{ *sql.Rows }

// Scan routes *json.RawMessage destinations through sql.NullString, because
// database/sql cannot scan TEXT or NULL into a named []byte type.
func (r *sqliteRows) Scan(dest ...any) error {
	proxies := make([]any, len(dest))
	var fixups []func()
	for i, d := range dest {
		rm, ok := d.(*json.RawMessage)
		if !ok {
			proxies[i] = d
			continue
		}
		var ns sql.NullString
		proxies[i] = &ns
		fixups = append(fixups, func() {
			if ns.Valid {
				*rm = json.RawMessage(ns.String)
			} else {
				*rm = nil
			}
		})
	}
	if err := r.Rows.Scan(proxies...); err != nil {
		return err
	}
	for _, f := range fixups {
		f()
	}
	return nil
}
