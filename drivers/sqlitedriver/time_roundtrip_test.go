package sqlitedriver

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"
	_ "time/tzdata" // named-zone case must not depend on the host's zoneinfo

	"github.com/stretchr/testify/require"

	"github.com/xraph/grove"
)

// timedRow stores the same instant in a value and a nullable column so every
// case covers both scan destinations.
type timedRow struct {
	grove.BaseModel `grove:"table:timed_rows"`
	ID              int64      `grove:"id,pk"`
	At              time.Time  `grove:"at,notnull"`
	Maybe           *time.Time `grove:"maybe"`
}

func openTimeDB(t *testing.T) *SqliteDB {
	t.Helper()
	ctx := context.Background()
	db := New()
	require.NoError(t, db.Open(ctx, filepath.Join(t.TempDir(), "time.db")))
	t.Cleanup(func() { _ = db.Close() })
	_, err := db.NewCreateTable((*timedRow)(nil)).Exec(ctx)
	require.NoError(t, err)
	return db
}

// timeCases are the shapes modernc renders with t.String() that grove could
// not read back: a monotonic reading appends " m=+...", and a nameless fixed
// zone renders its abbreviation as a bare offset ("+0200 +0200").
func timeCases(t *testing.T) map[string]time.Time {
	t.Helper()
	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	base := time.Date(2026, 9, 29, 14, 30, 15, 123456789, time.UTC)
	return map[string]time.Time{
		"monotonic":       time.Now().Add(24 * time.Hour),
		"nameless offset": base.In(time.FixedZone("", 2*60*60)),
		"named zone":      base.In(ny),
	}
}

func requireSameInstant(t *testing.T, want time.Time, got timedRow) {
	t.Helper()
	require.True(t, want.Equal(got.At), "At = %v, want %v", got.At, want)
	require.NotNil(t, got.Maybe)
	require.True(t, want.Equal(*got.Maybe), "Maybe = %v, want %v", *got.Maybe, want)
}

func TestTimeRoundTrip_Insert(t *testing.T) {
	for name, want := range timeCases(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			db := openTimeDB(t)

			w := want
			_, err := db.NewInsert(&timedRow{ID: 1, At: want, Maybe: &w}).Exec(ctx)
			require.NoError(t, err)

			var got timedRow
			require.NoError(t, db.NewSelect(&got).Where("id = ?", 1).Scan(ctx))
			requireSameInstant(t, want, got)
		})
	}
}

// TestTimeRoundTrip_BulkInsert covers the prepared-statement loop, which
// binds args through a separate path from the single-row insert.
func TestTimeRoundTrip_BulkInsert(t *testing.T) {
	for name, want := range timeCases(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			db := openTimeDB(t)

			w := want
			rows := []timedRow{{ID: 1, At: want, Maybe: &w}, {ID: 2, At: want, Maybe: &w}}
			_, err := db.NewInsert(&rows).Exec(ctx)
			require.NoError(t, err)

			var got []timedRow
			require.NoError(t, db.NewSelect(&got).OrderExpr("id").Scan(ctx))
			require.Len(t, got, 2)
			for _, g := range got {
				requireSameInstant(t, want, g)
			}
		})
	}
}

func TestTimeRoundTrip_Update(t *testing.T) {
	for name, want := range timeCases(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			db := openTimeDB(t)

			seed := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
			_, err := db.NewInsert(&timedRow{ID: 1, At: seed, Maybe: &seed}).Exec(ctx)
			require.NoError(t, err)

			w := want
			_, err = db.NewUpdate(&timedRow{ID: 1, At: want, Maybe: &w}).WherePK().Exec(ctx)
			require.NoError(t, err)

			var got timedRow
			require.NoError(t, db.NewSelect(&got).Where("id = ?", 1).Scan(ctx))
			requireSameInstant(t, want, got)
		})
	}
}

// TestTimeRoundTrip_LegacyRows reads text that older grove versions already
// wrote to disk, so databases created before the write-side fix stay readable.
func TestTimeRoundTrip_LegacyRows(t *testing.T) {
	tests := map[string]struct {
		stored string
		want   time.Time
	}{
		"monotonic suffix": {
			stored: "2026-09-30 14:30:15.123456789 +0000 UTC m=+86400.020833334",
			want:   time.Date(2026, 9, 30, 14, 30, 15, 123456789, time.UTC),
		},
		"negative monotonic suffix": {
			stored: "2026-09-30 14:30:15 +0000 UTC m=-0.000001",
			want:   time.Date(2026, 9, 30, 14, 30, 15, 0, time.UTC),
		},
		"nameless offset": {
			stored: "2026-09-29 16:30:15.123456789 +0200 +0200",
			want:   time.Date(2026, 9, 29, 14, 30, 15, 123456789, time.UTC),
		},
		"nameless offset with monotonic suffix": {
			stored: "2026-09-29 11:00:15 -0330 -0330 m=+3.5",
			want:   time.Date(2026, 9, 29, 14, 30, 15, 0, time.UTC),
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			db := openTimeDB(t)

			_, err := db.NewRaw(
				`INSERT INTO "timed_rows" ("id", "at", "maybe") VALUES (1, ?, ?)`,
				tt.stored, tt.stored,
			).Exec(ctx)
			require.NoError(t, err)

			var got timedRow
			require.NoError(t, db.NewSelect(&got).Where("id = ?", 1).Scan(ctx))
			requireSameInstant(t, tt.want, got)
		})
	}
}

// TestTimeRoundTrip_StoredAsUTC pins the write side: whatever zone or
// monotonic reading the caller's time carries, the TEXT column holds a plain
// UTC rendering. That keeps rows in one format, so lexical ORDER BY and range
// comparisons on the column agree with chronological order.
func TestTimeRoundTrip_StoredAsUTC(t *testing.T) {
	for name, want := range timeCases(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			db := openTimeDB(t)

			w := want
			_, err := db.NewInsert(&timedRow{ID: 1, At: want, Maybe: &w}).Exec(ctx)
			require.NoError(t, err)

			var at, maybe string
			require.NoError(t, db.NewRaw(`SELECT "at", "maybe" FROM "timed_rows" WHERE "id" = 1`).Scan(ctx, &at, &maybe))
			wantText := want.UTC().String()
			require.Equal(t, wantText, at)
			require.Equal(t, wantText, maybe)
		})
	}
}

// TestTimeRoundTrip_WhereArgs checks that a time used as a query argument is
// normalized the same way as stored values, so a range filter written with a
// local, monotonic time.Now() still matches correctly.
func TestTimeRoundTrip_WhereArgs(t *testing.T) {
	ctx := context.Background()
	db := openTimeDB(t)

	now := time.Now()
	rows := []timedRow{
		{ID: 1, At: now.Add(-time.Hour).UTC()},
		{ID: 2, At: now.Add(time.Hour).UTC()},
	}
	_, err := db.NewInsert(&rows).Exec(ctx)
	require.NoError(t, err)

	cutoff := now.In(time.FixedZone("", 14*60*60)) // far-east offset sorts wrong as text
	var got []timedRow
	require.NoError(t, db.NewSelect(&got).Where(`"at" < ?`, cutoff).Scan(ctx))
	require.Len(t, got, 1)
	require.Equal(t, int64(1), got[0].ID)
}

// TestTimeRoundTrip_InTx covers the transaction wrapper, which hands args to
// database/sql on its own path.
func TestTimeRoundTrip_InTx(t *testing.T) {
	ctx := context.Background()
	db := openTimeDB(t)

	want := time.Now().Add(24 * time.Hour)
	tx, err := db.BeginTxQuery(ctx, nil)
	require.NoError(t, err)
	_, err = tx.NewInsert(&timedRow{ID: 1, At: want, Maybe: &want}).Exec(ctx)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	var at string
	require.NoError(t, db.NewRaw(`SELECT "at" FROM "timed_rows" WHERE "id" = 1`).Scan(ctx, &at))
	require.Equal(t, want.UTC().String(), at)
}

func TestUTCArgs(t *testing.T) {
	local := time.Date(2026, 9, 29, 16, 30, 0, 0, time.FixedZone("", 2*60*60))
	var nilTime *time.Time
	args := []any{1, local, &local, nilTime, "x"}

	got := utcArgs(args)
	require.Equal(t, []any{1, local.UTC(), local.UTC(), nilTime, "x"}, got)
	require.Same(t, &local, args[2], "caller's slice must not be modified")
	require.Equal(t, local, args[1], "caller's slice must not be modified")

	valid := sql.NullTime{Time: local, Valid: true}
	invalid := sql.NullTime{}
	got = utcArgs([]any{valid, &valid, invalid, (*sql.NullTime)(nil)})
	require.Equal(t, sql.NullTime{Time: local.UTC(), Valid: true}, got[0])
	require.Equal(t, sql.NullTime{Time: local.UTC(), Valid: true}, got[1])
	require.Equal(t, invalid, got[2])
	require.Nil(t, got[3])
	require.Equal(t, local, valid.Time, "caller's NullTime must not be modified")

	plain := []any{1, "x"}
	require.Same(t, &plain[0], &utcArgs(plain)[0], "no times means no copy")
}

// nullTimeRow uses sql.NullTime, which binds through driver.Valuer rather
// than as a bare time.Time, so it needs its own coverage on both sides.
type nullTimeRow struct {
	grove.BaseModel `grove:"table:null_time_rows"`
	ID              int64        `grove:"id,pk"`
	At              sql.NullTime `grove:"at"`
}

func TestTimeRoundTrip_NullTime(t *testing.T) {
	cases := timeCases(t)
	cases["null"] = time.Time{}
	for name, want := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			db := New()
			require.NoError(t, db.Open(ctx, filepath.Join(t.TempDir(), "time.db")))
			t.Cleanup(func() { _ = db.Close() })
			_, err := db.NewCreateTable((*nullTimeRow)(nil)).Exec(ctx)
			require.NoError(t, err)

			in := sql.NullTime{Time: want, Valid: !want.IsZero()}
			_, err = db.NewInsert(&nullTimeRow{ID: 1, At: in}).Exec(ctx)
			require.NoError(t, err)

			// Two scalar dests keep RawQuery off its model path, which a lone
			// *sql.NullString (a struct pointer) would take.
			var stored string
			var isNull bool
			require.NoError(t, db.NewRaw(`SELECT coalesce("at", ''), "at" IS NULL FROM "null_time_rows" WHERE "id" = 1`).Scan(ctx, &stored, &isNull))
			var got nullTimeRow
			require.NoError(t, db.NewSelect(&got).Where("id = ?", 1).Scan(ctx))

			if !in.Valid {
				require.True(t, isNull)
				require.False(t, got.At.Valid)
				return
			}
			require.Equal(t, want.UTC().String(), stored)
			require.True(t, got.At.Valid)
			require.True(t, want.Equal(got.At.Time), "At = %v, want %v", got.At.Time, want)
		})
	}
}
