package tagparser

import (
	"testing"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name        string
		tag         string
		wantName    string
		wantOptions map[string]string
	}{
		{
			name:        "empty tag",
			tag:         "",
			wantName:    "",
			wantOptions: map[string]string{},
		},
		{
			name:        "simple column name",
			tag:         "id",
			wantName:    "id",
			wantOptions: map[string]string{},
		},
		{
			name:     "column with pk",
			tag:      "id,pk",
			wantName: "id",
			wantOptions: map[string]string{
				"pk": "",
			},
		},
		{
			name:     "column with multiple boolean options",
			tag:      "id,pk,autoincrement,notnull",
			wantName: "id",
			wantOptions: map[string]string{
				"pk":            "",
				"autoincrement": "",
				"notnull":       "",
			},
		},
		{
			name:     "column with type key:value",
			tag:      "metadata,type:jsonb",
			wantName: "metadata",
			wantOptions: map[string]string{
				"type": "jsonb",
			},
		},
		{
			name:     "column with privacy option",
			tag:      "email,notnull,unique,privacy:pii",
			wantName: "email",
			wantOptions: map[string]string{
				"notnull": "",
				"unique":  "",
				"privacy": "pii",
			},
		},
		{
			name:     "quoted default value",
			tag:      "status,notnull,default:'active'",
			wantName: "status",
			wantOptions: map[string]string{
				"notnull": "",
				"default": "active",
			},
		},
		{
			name:     "quoted value with spaces",
			tag:      "bio,default:'hello world'",
			wantName: "bio",
			wantOptions: map[string]string{
				"default": "hello world",
			},
		},
		{
			name:     "quoted value with comma inside quotes",
			tag:      "data,default:'a, b, c'",
			wantName: "data",
			wantOptions: map[string]string{
				"default": "a, b, c",
			},
		},
		{
			name:     "table-level tag",
			tag:      "table:users,alias:u",
			wantName: "",
			wantOptions: map[string]string{
				"table": "users",
				"alias": "u",
			},
		},
		{
			name:        "skip field with dash",
			tag:         "-",
			wantName:    "-",
			wantOptions: map[string]string{},
		},
		{
			name:     "soft_delete option",
			tag:      "deleted_at,soft_delete,nullzero",
			wantName: "deleted_at",
			wantOptions: map[string]string{
				"soft_delete": "",
				"nullzero":    "",
			},
		},
		{
			name:     "scanonly option",
			tag:      "computed_field,scanonly",
			wantName: "computed_field",
			wantOptions: map[string]string{
				"scanonly": "",
			},
		},
		{
			name:     "relation tag with has-many",
			tag:      "rel:has-many,join:id=user_id",
			wantName: "",
			wantOptions: map[string]string{
				"rel":  "has-many",
				"join": "id=user_id",
			},
		},
		{
			name:     "relation tag with belongs-to",
			tag:      "rel:belongs-to,join:user_id=id",
			wantName: "",
			wantOptions: map[string]string{
				"rel":  "belongs-to",
				"join": "user_id=id",
			},
		},
		{
			name:     "relation tag with has-one",
			tag:      "rel:has-one,join:id=profile_id",
			wantName: "",
			wantOptions: map[string]string{
				"rel":  "has-one",
				"join": "id=profile_id",
			},
		},
		{
			name:     "full model field tag",
			tag:      "id,pk,autoincrement,type:bigint",
			wantName: "id",
			wantOptions: map[string]string{
				"pk":            "",
				"autoincrement": "",
				"type":          "bigint",
			},
		},
		{
			name:     "whitespace around tag",
			tag:      "  name , notnull ",
			wantName: "name",
			wantOptions: map[string]string{
				"notnull": "",
			},
		},
		{
			name:     "unique option",
			tag:      "email,notnull,unique",
			wantName: "email",
			wantOptions: map[string]string{
				"notnull": "",
				"unique":  "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Parse(tt.tag)

			if got.Name != tt.wantName {
				t.Errorf("Parse(%q).Name = %q, want %q", tt.tag, got.Name, tt.wantName)
			}

			if len(got.Options) != len(tt.wantOptions) {
				t.Errorf("Parse(%q).Options has %d entries, want %d: got %v, want %v",
					tt.tag, len(got.Options), len(tt.wantOptions), got.Options, tt.wantOptions)
				return
			}

			for k, wantV := range tt.wantOptions {
				gotV, ok := got.Options[k]
				if !ok {
					t.Errorf("Parse(%q).Options missing key %q", tt.tag, k)
					continue
				}
				if gotV != wantV {
					t.Errorf("Parse(%q).Options[%q] = %q, want %q", tt.tag, k, gotV, wantV)
				}
			}
		})
	}
}

func TestTag_HasOption(t *testing.T) {
	tests := []struct {
		name   string
		tag    string
		option string
		want   bool
	}{
		{
			name:   "has pk option",
			tag:    "id,pk",
			option: "pk",
			want:   true,
		},
		{
			name:   "does not have option",
			tag:    "id,pk",
			option: "notnull",
			want:   false,
		},
		{
			name:   "has key:value option",
			tag:    "data,type:jsonb",
			option: "type",
			want:   true,
		},
		{
			name:   "empty tag has no option",
			tag:    "",
			option: "pk",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed := Parse(tt.tag)
			if got := parsed.HasOption(tt.option); got != tt.want {
				t.Errorf("Tag(%q).HasOption(%q) = %v, want %v", tt.tag, tt.option, got, tt.want)
			}
		})
	}
}

func TestTag_GetOption(t *testing.T) {
	tests := []struct {
		name   string
		tag    string
		option string
		want   string
	}{
		{
			name:   "get type value",
			tag:    "data,type:jsonb",
			option: "type",
			want:   "jsonb",
		},
		{
			name:   "get boolean option returns empty string",
			tag:    "id,pk",
			option: "pk",
			want:   "",
		},
		{
			name:   "get missing option returns empty string",
			tag:    "id,pk",
			option: "type",
			want:   "",
		},
		{
			name:   "get relation value",
			tag:    "rel:has-many,join:id=user_id",
			option: "rel",
			want:   "has-many",
		},
		{
			name:   "get join value",
			tag:    "rel:has-many,join:id=user_id",
			option: "join",
			want:   "id=user_id",
		},
		{
			name:   "get quoted default value",
			tag:    "status,default:'pending'",
			option: "default",
			want:   "pending",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed := Parse(tt.tag)
			if got := parsed.GetOption(tt.option); got != tt.want {
				t.Errorf("Tag(%q).GetOption(%q) = %q, want %q", tt.tag, tt.option, got, tt.want)
			}
		})
	}
}

func BenchmarkParse(b *testing.B) {
	benchmarks := []struct {
		name string
		tag  string
	}{
		{"simple", "id,pk"},
		{"medium", "email,notnull,unique,privacy:pii"},
		{"complex", "id,pk,autoincrement,type:bigint,default:'0'"},
		{"table", "table:users,alias:u"},
		{"relation", "rel:has-many,join:id=user_id"},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				Parse(bm.tag)
			}
		})
	}
}
