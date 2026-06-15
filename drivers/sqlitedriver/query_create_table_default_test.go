package sqlitedriver

import "testing"

func TestFormatDefaultValue(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		// The struct-tag parser strips quotes, so string defaults arrive bare
		// and must be re-quoted — otherwise SQLite rejects the bare identifier.
		{"bare string", "provisioning", "'provisioning'"},
		{"bare string with space", "in progress", "'in progress'"},
		{"string with apostrophe", "o'brien", "'o''brien'"},

		// Non-string defaults must stay verbatim.
		{"boolean true", "true", "true"},
		{"boolean false", "false", "false"},
		{"null", "null", "null"},
		{"uppercase keyword", "CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP"},
		{"integer", "0", "0"},
		{"negative integer", "-5", "-5"},
		{"decimal", "3.14", "3.14"},
		{"function call", "now()", "now()"},
		{"function call args", "gen_random_uuid()", "gen_random_uuid()"},
		{"cast expression", "'{}'::jsonb", "'{}'::jsonb"},

		// Already-quoted literals pass through untouched.
		{"already quoted", "'active'", "'active'"},
		{"empty", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatDefaultValue(tt.in); got != tt.want {
				t.Errorf("formatDefaultValue(%q) = %q; want %q", tt.in, got, tt.want)
			}
		})
	}
}
