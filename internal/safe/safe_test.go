package safe

import "testing"

func TestQuoteIdent(t *testing.T) {
	tests := []struct {
		name  string
		ident string
		want  string
	}{
		{
			name:  "simple identifier",
			ident: "users",
			want:  `"users"`,
		},
		{
			name:  "identifier with underscore",
			ident: "user_name",
			want:  `"user_name"`,
		},
		{
			name:  "identifier with embedded double quote",
			ident: `my"table`,
			want:  `"my""table"`,
		},
		{
			name:  "identifier with multiple double quotes",
			ident: `a"b"c`,
			want:  `"a""b""c"`,
		},
		{
			name:  "empty string",
			ident: "",
			want:  `""`,
		},
		{
			name:  "identifier with spaces",
			ident: "my table",
			want:  `"my table"`,
		},
		{
			name:  "identifier with special characters",
			ident: "user@name",
			want:  `"user@name"`,
		},
		{
			name:  "uppercase identifier",
			ident: "Users",
			want:  `"Users"`,
		},
		{
			name:  "identifier starting with digit",
			ident: "123abc",
			want:  `"123abc"`,
		},
		{
			name:  "SQL injection attempt with semicolon",
			ident: "users; DROP TABLE users; --",
			want:  `"users; DROP TABLE users; --"`,
		},
		{
			name:  "SQL injection attempt with double quote escape",
			ident: `users"; DROP TABLE users; --`,
			want:  `"users""; DROP TABLE users; --"`,
		},
		{
			name:  "SQL injection with nested quotes",
			ident: `""; DROP TABLE users; ""`,
			want:  `"""""; DROP TABLE users; """""`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := QuoteIdent(tt.ident)
			if got != tt.want {
				t.Errorf("QuoteIdent(%q) = %q, want %q", tt.ident, got, tt.want)
			}
		})
	}
}

func TestIsValidIdent(t *testing.T) {
	tests := []struct {
		name  string
		ident string
		want  bool
	}{
		{
			name:  "simple lowercase",
			ident: "users",
			want:  true,
		},
		{
			name:  "with underscore",
			ident: "user_name",
			want:  true,
		},
		{
			name:  "starts with underscore",
			ident: "_private",
			want:  true,
		},
		{
			name:  "with digits",
			ident: "table1",
			want:  true,
		},
		{
			name:  "uppercase",
			ident: "Users",
			want:  true,
		},
		{
			name:  "all uppercase",
			ident: "USERS",
			want:  true,
		},
		{
			name:  "mixed case with digits and underscores",
			ident: "User_Table_v2",
			want:  true,
		},
		{
			name:  "single letter",
			ident: "a",
			want:  true,
		},
		{
			name:  "single underscore",
			ident: "_",
			want:  true,
		},
		{
			name:  "empty string",
			ident: "",
			want:  false,
		},
		{
			name:  "starts with digit",
			ident: "1table",
			want:  false,
		},
		{
			name:  "contains space",
			ident: "my table",
			want:  false,
		},
		{
			name:  "contains hyphen",
			ident: "my-table",
			want:  false,
		},
		{
			name:  "contains dot",
			ident: "schema.table",
			want:  false,
		},
		{
			name:  "contains at sign",
			ident: "user@name",
			want:  false,
		},
		{
			name:  "contains semicolon",
			ident: "users;",
			want:  false,
		},
		{
			name:  "contains double quote",
			ident: `user"name`,
			want:  false,
		},
		{
			name:  "contains single quote",
			ident: "user'name",
			want:  false,
		},
		{
			name:  "SQL injection attempt",
			ident: "users; DROP TABLE users; --",
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsValidIdent(tt.ident)
			if got != tt.want {
				t.Errorf("IsValidIdent(%q) = %v, want %v", tt.ident, got, tt.want)
			}
		})
	}
}

func TestSanitize(t *testing.T) {
	tests := []struct {
		name  string
		ident string
		want  string
	}{
		{
			name:  "already valid",
			ident: "users",
			want:  "users",
		},
		{
			name:  "with underscore",
			ident: "user_name",
			want:  "user_name",
		},
		{
			name:  "removes spaces",
			ident: "my table",
			want:  "mytable",
		},
		{
			name:  "removes special characters",
			ident: "user@name!#$%",
			want:  "username",
		},
		{
			name:  "removes hyphens",
			ident: "my-table-name",
			want:  "mytablename",
		},
		{
			name:  "removes dots",
			ident: "schema.table",
			want:  "schematable",
		},
		{
			name:  "preserves digits",
			ident: "table123",
			want:  "table123",
		},
		{
			name:  "prepends underscore when starts with digit",
			ident: "123abc",
			want:  "_123abc",
		},
		{
			name:  "empty string",
			ident: "",
			want:  "",
		},
		{
			name:  "all special characters",
			ident: "!@#$%^&*()",
			want:  "",
		},
		{
			name:  "SQL injection attempt",
			ident: "users; DROP TABLE users; --",
			want:  "usersDROPTABLEusers",
		},
		{
			name:  "preserves uppercase",
			ident: "MyTable",
			want:  "MyTable",
		},
		{
			name:  "unicode characters removed",
			ident: "tab\u00e9l\u00e9",
			want:  "tabl",
		},
		{
			name:  "only digits prepends underscore",
			ident: "123",
			want:  "_123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Sanitize(tt.ident)
			if got != tt.want {
				t.Errorf("Sanitize(%q) = %q, want %q", tt.ident, got, tt.want)
			}
		})
	}
}

func TestSQLInjectionPrevention(t *testing.T) {
	// Verify that various SQL injection attempts are properly neutralized
	// through quoting.
	injections := []struct {
		name  string
		input string
	}{
		{"semicolon injection", `users"; DROP TABLE users; --`},
		{"union injection", `users" UNION SELECT * FROM passwords --`},
		{"comment injection", `users"--`},
		{"nested quote injection", `""; DROP TABLE users; ""`},
		{"null byte injection", "users\x00; DROP TABLE users"},
		{"backslash injection", `users\"; DROP TABLE users`},
	}

	for _, tt := range injections {
		t.Run(tt.name, func(t *testing.T) {
			quoted := QuoteIdent(tt.input)

			// The quoted result must start and end with a double quote.
			if len(quoted) < 2 || quoted[0] != '"' || quoted[len(quoted)-1] != '"' {
				t.Errorf("QuoteIdent(%q) = %q, does not have proper quoting", tt.input, quoted)
			}

			// Extract the inner content (between outer quotes) and verify
			// that all original double quotes have been escaped.
			inner := quoted[1 : len(quoted)-1]

			// Count unescaped double quotes: after escaping, every " becomes "".
			// So there should be no standalone " in the inner content.
			i := 0
			for i < len(inner) {
				if inner[i] == '"' {
					// Must be followed by another ".
					if i+1 >= len(inner) || inner[i+1] != '"' {
						t.Errorf("QuoteIdent(%q) has unescaped quote at position %d in inner %q",
							tt.input, i, inner)
						break
					}
					i += 2 // Skip the escaped pair.
				} else {
					i++
				}
			}
		})
	}
}

func BenchmarkQuoteIdent(b *testing.B) {
	benchmarks := []struct {
		name  string
		ident string
	}{
		{"simple", "users"},
		{"with_quotes", `my"table`},
		{"long", "very_long_identifier_name_for_a_table"},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				QuoteIdent(bm.ident)
			}
		})
	}
}

func BenchmarkIsValidIdent(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		IsValidIdent("user_table_name_v2")
	}
}

func BenchmarkSanitize(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		Sanitize("user@name!#$%^&*()")
	}
}
