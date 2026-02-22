// Package grove is a polyglot Go ORM with native query syntax per database.
//
// Grove provides near-raw performance with driver-specific query builders that
// expose each database's native idioms. PostgreSQL queries use $1 placeholders
// and PG-specific features. MySQL queries use ? placeholders and backtick quoting.
// MongoDB queries use native BSON syntax.
//
// # Key Features
//
//   - Native query syntax per driver (no unified DSL)
//   - Dual tag system: grove:"..." primary, bun:"..." fallback
//   - Zero-reflection hot path (reflect once at registration)
//   - Modular migrations with multi-module dependency ordering
//   - Privacy hooks for tenant isolation, PII redaction, audit logging
//   - Part of the Forge ecosystem (github.com/xraph/forge)
//
// # Quick Start
//
//	// Define a model
//	type User struct {
//	    grove.BaseModel `grove:"table:users,alias:u"`
//
//	    ID    int64  `grove:"id,pk,autoincrement"`
//	    Name  string `grove:"name,notnull"`
//	    Email string `grove:"email,notnull,unique"`
//	}
//
//	// Connect and register
//	db, err := grove.Open(pgdriver.New(), "postgres://localhost:5432/mydb")
//	db.RegisterModel((*User)(nil))
//
//	// Query with native PostgreSQL syntax
//	var users []User
//	err = db.NewSelect(&users).
//	    Where("email ILIKE $1", "%@example.com").
//	    Scan(ctx)
package grove
