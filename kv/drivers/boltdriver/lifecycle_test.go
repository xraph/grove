package boltdriver

import (
	"path/filepath"
	"testing"
)

func TestOpenPreservesExistingDatabase(t *testing.T) {
	db := New()
	if err := db.Open(t.Context(), filepath.Join(t.TempDir(), "store.db")); err != nil {
		t.Fatal(err)
	}
	existing := db.db
	defer existing.Close()
	err := db.Open(t.Context(), filepath.Join(t.TempDir(), "store.db"))
	if db.db != nil && db.db != existing {
		defer db.db.Close()
	}
	if err == nil {
		t.Fatal("second Open must reject an already initialized driver")
	}
	if db.db != existing {
		t.Fatal("second Open replaced the live database handle")
	}
	if err := db.Ping(t.Context()); err != nil {
		t.Fatal(err)
	}
}
