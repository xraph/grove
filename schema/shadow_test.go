package schema

import (
	"testing"
	"time"
)

// EmbeddedBase is exported so grove walks it (it skips unexported embeds),
// mirroring imodels.Entity.
type EmbeddedBase struct {
	ID        string    `grove:"id,pk"`
	UpdatedAt time.Time `grove:"updated_at,notnull" json:"updatedAt"`
}

// shadowModel embeds EmbeddedBase and overrides UpdatedAt with a different json
// tag — the same embed+override pattern real models use. Go resolves
// shadowModel.UpdatedAt to the outer field; the schema must do likewise and
// emit a single updated_at column.
type shadowModel struct {
	EmbeddedBase
	Name      string    `grove:"name,notnull"`
	UpdatedAt time.Time `grove:"updated_at,notnull" json:"updated_at"`
}

func TestNewTable_shadowedEmbeddedColumnDeduped(t *testing.T) {
	tbl, err := NewTable((*shadowModel)(nil))
	if err != nil {
		t.Fatalf("NewTable: %v", err)
	}

	count := 0
	var updatedAt *Field
	for _, f := range tbl.Fields {
		if f.Options.Column == "updated_at" {
			count++
			updatedAt = f
		}
	}
	if count != 1 {
		t.Fatalf("updated_at column appears %d times; want 1 (Go shadowing)", count)
	}
	// The winner must be the outer (shallower) field — index length 1, not the
	// embedded field's length 2.
	if updatedAt != nil && len(updatedAt.GoIndex) != 1 {
		t.Errorf("updated_at resolved to a field with GoIndex len %d; want 1 (outer field wins)", len(updatedAt.GoIndex))
	}

	if _, ok := tbl.FieldsByColumn["updated_at"]; !ok {
		t.Error("FieldsByColumn missing updated_at")
	}
	if len(tbl.PKFields) != 1 || tbl.PKFields[0].Options.Column != "id" {
		t.Errorf("PKFields = %v; want single id after dedupe", tbl.PKFields)
	}
}
