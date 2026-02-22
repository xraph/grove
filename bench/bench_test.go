// Package bench provides comparative benchmarks for Grove vs pure database/sql,
// Bun, and GORM using SQLite in-memory databases. These benchmarks are fully
// self-contained — no external database required.
//
// Run all benchmarks:
//
//	go test -bench=. -benchmem -count=5 ./bench/
//
// Run a specific category:
//
//	go test -bench=BenchmarkInsert -benchmem ./bench/
//	go test -bench=BenchmarkSelect -benchmem ./bench/
//	go test -bench=BenchmarkUpdate -benchmem ./bench/
//	go test -bench=BenchmarkDelete -benchmem ./bench/
//	go test -bench=BenchmarkBuild -benchmem ./bench/
package bench

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	_ "github.com/uptrace/bun/driver/sqliteshim"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/xraph/grove/schema"
	"github.com/xraph/grove/sqlitedriver"
)

// ---------------------------------------------------------------------------
// Models
// ---------------------------------------------------------------------------

// User is the benchmark model. All three ORMs use the same struct with their
// respective tags.
type User struct {
	ID        int64     `grove:"id,pk,autoincrement" bun:",pk,autoincrement" gorm:"primaryKey;autoIncrement"`
	Name      string    `grove:"name" bun:"name" gorm:"column:name"`
	Email     string    `grove:"email" bun:"email" gorm:"column:email"`
	Age       int       `grove:"age" bun:"age" gorm:"column:age"`
	Active    bool      `grove:"active" bun:"active" gorm:"column:active"`
	CreatedAt time.Time `grove:"created_at" bun:"created_at" gorm:"column:created_at"`
}

func (User) TableName() string { return "users" }

// BunTagUser has only bun tags to exercise the fallback tag resolution path.
type BunTagUser struct {
	ID        int64     `bun:",pk,autoincrement"`
	Name      string    `bun:"name"`
	Email     string    `bun:"email"`
	Age       int       `bun:"age"`
	Active    bool      `bun:"active"`
	CreatedAt time.Time `bun:"created_at"`
}

// ---------------------------------------------------------------------------
// Setup helpers
// ---------------------------------------------------------------------------

const createTableSQL = `CREATE TABLE IF NOT EXISTS "users" (
	"id" INTEGER PRIMARY KEY AUTOINCREMENT,
	"name" TEXT NOT NULL,
	"email" TEXT NOT NULL,
	"age" INTEGER NOT NULL DEFAULT 0,
	"active" INTEGER NOT NULL DEFAULT 1,
	"created_at" TEXT NOT NULL DEFAULT (datetime('now'))
)`

func newRawDB(b *testing.B) *sql.DB {
	b.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		b.Fatal(err)
	}
	if _, err := db.Exec(createTableSQL); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	return db
}

func newGroveDB(b *testing.B) *sqlitedriver.SqliteDB {
	b.Helper()
	sdb := sqlitedriver.New()
	if err := sdb.Open(context.Background(), ":memory:"); err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	if _, err := sdb.NewCreateTable((*User)(nil)).IfNotExists().Exec(ctx); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { sdb.Close() })
	return sdb
}

func newBunDB(b *testing.B) *bun.DB {
	b.Helper()
	sqldb, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	if _, err := sqldb.Exec("PRAGMA journal_mode=WAL"); err != nil {
		b.Fatal(err)
	}
	db := bun.NewDB(sqldb, sqlitedialect.New())
	if _, err := db.Exec(createTableSQL); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	return db
}

func newGormDB(b *testing.B) *gorm.DB {
	b.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger:                 logger.Discard,
		SkipDefaultTransaction: true,
	})
	if err != nil {
		b.Fatal(err)
	}
	sqlDB, _ := db.DB()
	if _, err := sqlDB.Exec("PRAGMA journal_mode=WAL"); err != nil {
		b.Fatal(err)
	}
	if err := db.AutoMigrate(&User{}); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { sqlDB.Close() })
	return db
}

func sampleUser(i int) User {
	return User{
		Name:      fmt.Sprintf("User_%d", i),
		Email:     fmt.Sprintf("user_%d@example.com", i),
		Age:       20 + (i % 50),
		Active:    i%3 != 0,
		CreatedAt: time.Now(),
	}
}

func seedUsers(b *testing.B, rawDB *sql.DB, n int) {
	b.Helper()
	for i := 0; i < n; i++ {
		u := sampleUser(i)
		_, err := rawDB.Exec(
			`INSERT INTO "users" ("name","email","age","active","created_at") VALUES (?,?,?,?,?)`,
			u.Name, u.Email, u.Age, u.Active, u.CreatedAt.Format(time.RFC3339),
		)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// ---------------------------------------------------------------------------
// INSERT benchmarks
// ---------------------------------------------------------------------------

func BenchmarkInsert(b *testing.B) {
	b.Run("RawSQL", func(b *testing.B) {
		db := newRawDB(b)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			u := sampleUser(i)
			_, err := db.Exec(
				`INSERT INTO "users" ("name","email","age","active","created_at") VALUES (?,?,?,?,?)`,
				u.Name, u.Email, u.Age, u.Active, u.CreatedAt.Format(time.RFC3339),
			)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Grove", func(b *testing.B) {
		sdb := newGroveDB(b)
		ctx := context.Background()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			u := sampleUser(i)
			_, err := sdb.NewInsert(&u).Exec(ctx)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Bun", func(b *testing.B) {
		db := newBunDB(b)
		ctx := context.Background()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			u := sampleUser(i)
			_, err := db.NewInsert().Model(&u).Exec(ctx)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("GORM", func(b *testing.B) {
		db := newGormDB(b)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			u := sampleUser(i)
			if err := db.Create(&u).Error; err != nil {
				b.Fatal(err)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// SELECT (single row by PK) benchmarks
// ---------------------------------------------------------------------------

func BenchmarkSelectOne(b *testing.B) {
	b.Run("RawSQL", func(b *testing.B) {
		db := newRawDB(b)
		seedUsers(b, db, 100)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var u User
			err := db.QueryRow(
				`SELECT "id","name","email","age","active","created_at" FROM "users" WHERE "id" = ?`,
				(i%100)+1,
			).Scan(&u.ID, &u.Name, &u.Email, &u.Age, &u.Active, &u.CreatedAt)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Grove", func(b *testing.B) {
		sdb := newGroveDB(b)
		ctx := context.Background()
		// Seed via Grove
		for i := 0; i < 100; i++ {
			u := sampleUser(i)
			sdb.NewInsert(&u).Exec(ctx)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var u User
			err := sdb.NewSelect(&u).
				Where("id = ?", (i%100)+1).
				Scan(ctx)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Bun", func(b *testing.B) {
		db := newBunDB(b)
		ctx := context.Background()
		// Seed
		for i := 0; i < 100; i++ {
			u := sampleUser(i)
			db.NewInsert().Model(&u).Exec(ctx)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var u User
			err := db.NewSelect().Model(&u).
				Where("id = ?", (i%100)+1).
				Scan(ctx)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("GORM", func(b *testing.B) {
		db := newGormDB(b)
		// Seed
		for i := 0; i < 100; i++ {
			u := sampleUser(i)
			db.Create(&u)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var u User
			if err := db.First(&u, (i%100)+1).Error; err != nil {
				b.Fatal(err)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// SELECT (multiple rows) benchmarks
// ---------------------------------------------------------------------------

func BenchmarkSelectMulti(b *testing.B) {
	b.Run("RawSQL", func(b *testing.B) {
		db := newRawDB(b)
		seedUsers(b, db, 200)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query(
				`SELECT "id","name","email","age","active","created_at" FROM "users" WHERE "active" = ? LIMIT 50`,
				true,
			)
			if err != nil {
				b.Fatal(err)
			}
			var users []User
			for rows.Next() {
				var u User
				if err := rows.Scan(&u.ID, &u.Name, &u.Email, &u.Age, &u.Active, &u.CreatedAt); err != nil {
					b.Fatal(err)
				}
				users = append(users, u)
			}
			rows.Close()
			if len(users) == 0 {
				b.Fatal("no rows returned")
			}
		}
	})

	b.Run("Grove", func(b *testing.B) {
		sdb := newGroveDB(b)
		ctx := context.Background()
		for i := 0; i < 200; i++ {
			u := sampleUser(i)
			sdb.NewInsert(&u).Exec(ctx)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var users []User
			err := sdb.NewSelect(&users).
				Where("active = ?", true).
				Limit(50).
				Scan(ctx)
			if err != nil {
				b.Fatal(err)
			}
			if len(users) == 0 {
				b.Fatal("no rows returned")
			}
		}
	})

	b.Run("Bun", func(b *testing.B) {
		db := newBunDB(b)
		ctx := context.Background()
		for i := 0; i < 200; i++ {
			u := sampleUser(i)
			db.NewInsert().Model(&u).Exec(ctx)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var users []User
			err := db.NewSelect().Model(&users).
				Where("active = ?", true).
				Limit(50).
				Scan(ctx)
			if err != nil {
				b.Fatal(err)
			}
			if len(users) == 0 {
				b.Fatal("no rows returned")
			}
		}
	})

	b.Run("GORM", func(b *testing.B) {
		db := newGormDB(b)
		for i := 0; i < 200; i++ {
			u := sampleUser(i)
			db.Create(&u)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var users []User
			if err := db.Where("active = ?", true).Limit(50).Find(&users).Error; err != nil {
				b.Fatal(err)
			}
			if len(users) == 0 {
				b.Fatal("no rows returned")
			}
		}
	})
}

// ---------------------------------------------------------------------------
// UPDATE benchmarks
// ---------------------------------------------------------------------------

func BenchmarkUpdate(b *testing.B) {
	b.Run("RawSQL", func(b *testing.B) {
		db := newRawDB(b)
		seedUsers(b, db, 100)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := db.Exec(
				`UPDATE "users" SET "name" = ?, "age" = ? WHERE "id" = ?`,
				fmt.Sprintf("Updated_%d", i), 30+i%20, (i%100)+1,
			)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Grove", func(b *testing.B) {
		sdb := newGroveDB(b)
		ctx := context.Background()
		for i := 0; i < 100; i++ {
			u := sampleUser(i)
			sdb.NewInsert(&u).Exec(ctx)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := sdb.NewUpdate(&User{}).
				Set("name = ?", fmt.Sprintf("Updated_%d", i)).
				Set("age = ?", 30+i%20).
				Where("id = ?", (i%100)+1).
				Exec(ctx)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Bun", func(b *testing.B) {
		db := newBunDB(b)
		ctx := context.Background()
		for i := 0; i < 100; i++ {
			u := sampleUser(i)
			db.NewInsert().Model(&u).Exec(ctx)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := db.NewUpdate().Model((*User)(nil)).
				Set("name = ?", fmt.Sprintf("Updated_%d", i)).
				Set("age = ?", 30+i%20).
				Where("id = ?", (i%100)+1).
				Exec(ctx)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("GORM", func(b *testing.B) {
		db := newGormDB(b)
		for i := 0; i < 100; i++ {
			u := sampleUser(i)
			db.Create(&u)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := db.Model(&User{}).
				Where("id = ?", (i%100)+1).
				Updates(map[string]any{
					"name": fmt.Sprintf("Updated_%d", i),
					"age":  30 + i%20,
				}).Error; err != nil {
				b.Fatal(err)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// DELETE benchmarks
// ---------------------------------------------------------------------------

func BenchmarkDelete(b *testing.B) {
	b.Run("RawSQL", func(b *testing.B) {
		db := newRawDB(b)
		// Re-seed on each iteration via StopTimer to isolate delete cost
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			db.Exec(`DELETE FROM "users"`)
			seedUsers(b, db, 10)
			b.StartTimer()
			_, err := db.Exec(`DELETE FROM "users" WHERE "id" = ?`, 1)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Grove", func(b *testing.B) {
		sdb := newGroveDB(b)
		ctx := context.Background()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			sdb.Exec(ctx, `DELETE FROM "users"`)
			for j := 0; j < 10; j++ {
				u := sampleUser(j)
				sdb.NewInsert(&u).Exec(ctx)
			}
			b.StartTimer()
			_, err := sdb.NewDelete(&User{}).
				Where("id = ?", 1).
				ForceDelete().
				Exec(ctx)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Bun", func(b *testing.B) {
		db := newBunDB(b)
		ctx := context.Background()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			db.Exec("DELETE FROM \"users\"")
			for j := 0; j < 10; j++ {
				u := sampleUser(j)
				db.NewInsert().Model(&u).Exec(ctx)
			}
			b.StartTimer()
			_, err := db.NewDelete().Model((*User)(nil)).
				Where("id = ?", 1).
				Exec(ctx)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("GORM", func(b *testing.B) {
		db := newGormDB(b)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			db.Exec(`DELETE FROM "users"`)
			for j := 0; j < 10; j++ {
				u := sampleUser(j)
				db.Create(&u)
			}
			b.StartTimer()
			if err := db.Where("id = ?", 1).Delete(&User{}).Error; err != nil {
				b.Fatal(err)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// Bulk INSERT benchmarks (100 rows)
// ---------------------------------------------------------------------------

func BenchmarkBulkInsert100(b *testing.B) {
	b.Run("RawSQL", func(b *testing.B) {
		db := newRawDB(b)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			db.Exec(`DELETE FROM "users"`)
			b.StartTimer()
			tx, err := db.Begin()
			if err != nil {
				b.Fatal(err)
			}
			stmt, err := tx.Prepare(`INSERT INTO "users" ("name","email","age","active","created_at") VALUES (?,?,?,?,?)`)
			if err != nil {
				b.Fatal(err)
			}
			for j := 0; j < 100; j++ {
				u := sampleUser(j)
				_, err := stmt.Exec(u.Name, u.Email, u.Age, u.Active, u.CreatedAt.Format(time.RFC3339))
				if err != nil {
					b.Fatal(err)
				}
			}
			stmt.Close()
			if err := tx.Commit(); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Grove", func(b *testing.B) {
		sdb := newGroveDB(b)
		ctx := context.Background()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			sdb.Exec(ctx, `DELETE FROM "users"`)
			users := make([]User, 100)
			for j := range users {
				users[j] = sampleUser(j)
			}
			b.StartTimer()
			_, err := sdb.NewInsert(&users).Exec(ctx)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Bun", func(b *testing.B) {
		db := newBunDB(b)
		ctx := context.Background()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			db.Exec("DELETE FROM \"users\"")
			users := make([]User, 100)
			for j := range users {
				users[j] = sampleUser(j)
			}
			b.StartTimer()
			_, err := db.NewInsert().Model(&users).Exec(ctx)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("GORM", func(b *testing.B) {
		db := newGormDB(b)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			db.Exec(`DELETE FROM "users"`)
			users := make([]User, 100)
			for j := range users {
				users[j] = sampleUser(j)
			}
			b.StartTimer()
			if err := db.Create(&users).Error; err != nil {
				b.Fatal(err)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// Bulk INSERT benchmarks (1000 rows)
// ---------------------------------------------------------------------------

func BenchmarkBulkInsert1000(b *testing.B) {
	b.Run("RawSQL", func(b *testing.B) {
		db := newRawDB(b)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			db.Exec(`DELETE FROM "users"`)
			b.StartTimer()
			tx, err := db.Begin()
			if err != nil {
				b.Fatal(err)
			}
			stmt, err := tx.Prepare(`INSERT INTO "users" ("name","email","age","active","created_at") VALUES (?,?,?,?,?)`)
			if err != nil {
				b.Fatal(err)
			}
			for j := 0; j < 1000; j++ {
				u := sampleUser(j)
				_, err := stmt.Exec(u.Name, u.Email, u.Age, u.Active, u.CreatedAt.Format(time.RFC3339))
				if err != nil {
					b.Fatal(err)
				}
			}
			stmt.Close()
			if err := tx.Commit(); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Grove", func(b *testing.B) {
		sdb := newGroveDB(b)
		ctx := context.Background()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			sdb.Exec(ctx, `DELETE FROM "users"`)
			users := make([]User, 1000)
			for j := range users {
				users[j] = sampleUser(j)
			}
			b.StartTimer()
			_, err := sdb.NewInsert(&users).Exec(ctx)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Bun", func(b *testing.B) {
		db := newBunDB(b)
		ctx := context.Background()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			db.Exec("DELETE FROM \"users\"")
			users := make([]User, 1000)
			for j := range users {
				users[j] = sampleUser(j)
			}
			b.StartTimer()
			_, err := db.NewInsert().Model(&users).Exec(ctx)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("GORM", func(b *testing.B) {
		db := newGormDB(b)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			db.Exec(`DELETE FROM "users"`)
			users := make([]User, 1000)
			for j := range users {
				users[j] = sampleUser(j)
			}
			b.StartTimer()
			if err := db.Create(&users).Error; err != nil {
				b.Fatal(err)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// SQL Generation (Build) benchmarks — no database I/O
// ---------------------------------------------------------------------------

func BenchmarkBuildSelect(b *testing.B) {
	b.Run("Grove", func(b *testing.B) {
		sdb := newGroveDB(b)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			q := sdb.NewSelect((*User)(nil)).
				Column("id", "name", "email").
				Where("active = ?", true).
				Where("age > ?", 18).
				OrderExpr("created_at DESC").
				Limit(20).
				Offset(0)
			_, _, err := q.Build()
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Bun", func(b *testing.B) {
		db := newBunDB(b)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			q := db.NewSelect().Model((*User)(nil)).
				Column("id", "name", "email").
				Where("active = ?", true).
				Where("age > ?", 18).
				OrderExpr("created_at DESC").
				Limit(20).
				Offset(0)
			_, err := q.AppendQuery(db.QueryGen(), nil)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkBuildInsert(b *testing.B) {
	b.Run("Grove", func(b *testing.B) {
		sdb := newGroveDB(b)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			u := sampleUser(i)
			q := sdb.NewInsert(&u)
			_, _, err := q.Build()
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Bun", func(b *testing.B) {
		db := newBunDB(b)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			u := sampleUser(i)
			q := db.NewInsert().Model(&u)
			_, err := q.AppendQuery(db.QueryGen(), nil)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkBuildUpdate(b *testing.B) {
	b.Run("Grove", func(b *testing.B) {
		sdb := newGroveDB(b)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			q := sdb.NewUpdate(&User{}).
				Set("name = ?", "Updated").
				Set("age = ?", 30).
				Where("id = ?", 1)
			_, _, err := q.Build()
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Bun", func(b *testing.B) {
		db := newBunDB(b)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			q := db.NewUpdate().Model((*User)(nil)).
				Set("name = ?", "Updated").
				Set("age = ?", 30).
				Where("id = ?", 1)
			_, err := q.AppendQuery(db.QueryGen(), nil)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// Schema cache benchmarks
// ---------------------------------------------------------------------------

func BenchmarkSchemaCache(b *testing.B) {
	b.Run("ColdStart", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r := schema.NewRegistry()
			r.Register((*User)(nil))
		}
	})

	b.Run("CacheHit", func(b *testing.B) {
		r := schema.NewRegistry()
		r.Register((*User)(nil))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			r.Get((*User)(nil))
		}
	})
}

// ---------------------------------------------------------------------------
// Tag resolution benchmarks
// ---------------------------------------------------------------------------

func BenchmarkTagResolution(b *testing.B) {
	b.Run("GroveTags", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r := schema.NewRegistry()
			r.Register((*User)(nil))
		}
	})

	b.Run("BunFallback", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r := schema.NewRegistry()
			r.Register((*BunTagUser)(nil))
		}
	})
}
