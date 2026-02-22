package schema

import (
	"reflect"
	"testing"
)

// ---------- ResolveTag tests ----------

func TestResolveTag(t *testing.T) {
	type groveOnly struct {
		F string `grove:"name,pk"`
	}
	type bunOnly struct {
		F string `bun:"name,pk"`
	}
	type bothTags struct {
		F string `grove:"grove_col" bun:"bun_col"`
	}
	type noTags struct {
		F string
	}
	type emptyGrove struct {
		F string `grove:""`
	}

	tests := []struct {
		name       string
		structType reflect.Type
		fieldName  string
		wantTag    string
		wantSource TagSource
	}{
		{
			name:       "grove tag only",
			structType: reflect.TypeOf(groveOnly{}),
			fieldName:  "F",
			wantTag:    "name,pk",
			wantSource: TagSourceGrove,
		},
		{
			name:       "bun tag only",
			structType: reflect.TypeOf(bunOnly{}),
			fieldName:  "F",
			wantTag:    "name,pk",
			wantSource: TagSourceBun,
		},
		{
			name:       "both tags grove wins",
			structType: reflect.TypeOf(bothTags{}),
			fieldName:  "F",
			wantTag:    "grove_col",
			wantSource: TagSourceGrove,
		},
		{
			name:       "no tags",
			structType: reflect.TypeOf(noTags{}),
			fieldName:  "F",
			wantTag:    "",
			wantSource: TagSourceNone,
		},
		{
			name:       "empty grove tag",
			structType: reflect.TypeOf(emptyGrove{}),
			fieldName:  "F",
			wantTag:    "",
			wantSource: TagSourceGrove,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sf, ok := tt.structType.FieldByName(tt.fieldName)
			if !ok {
				t.Fatalf("field %q not found", tt.fieldName)
			}

			gotTag, gotSource := ResolveTag(sf)
			if gotTag != tt.wantTag {
				t.Errorf("tag = %q, want %q", gotTag, tt.wantTag)
			}
			if gotSource != tt.wantSource {
				t.Errorf("source = %d, want %d", gotSource, tt.wantSource)
			}
		})
	}
}

// ---------- ParseTag tests ----------

func TestParseTag(t *testing.T) {
	tests := []struct {
		name       string
		raw        string
		wantName   string
		wantHasOpt string
		wantOptVal string // for a key:value option
		optKey     string
		valKey     string
	}{
		{
			name:       "column with pk",
			raw:        "id,pk",
			wantName:   "id",
			wantHasOpt: "pk",
		},
		{
			name:       "column with type option",
			raw:        "data,type:jsonb",
			wantName:   "data",
			optKey:     "type",
			wantOptVal: "jsonb",
		},
		{
			name:     "table-level options",
			raw:      "table:users,alias:u",
			wantName: "",
			optKey:   "table",
		},
		{
			name:     "empty string",
			raw:      "",
			wantName: "",
		},
		{
			name:       "skip tag",
			raw:        "-",
			wantName:   "-",
			wantHasOpt: "",
		},
		{
			name:       "column with default quoted value",
			raw:        "status,default:'active'",
			wantName:   "status",
			optKey:     "default",
			wantOptVal: "active",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tag := ParseTag(tt.raw)
			if tag.Name != tt.wantName {
				t.Errorf("Name = %q, want %q", tag.Name, tt.wantName)
			}
			if tt.wantHasOpt != "" && !tag.HasOption(tt.wantHasOpt) {
				t.Errorf("expected option %q to be present", tt.wantHasOpt)
			}
			if tt.optKey != "" {
				got := tag.GetOption(tt.optKey)
				if tt.wantOptVal != "" && got != tt.wantOptVal {
					t.Errorf("option %q = %q, want %q", tt.optKey, got, tt.wantOptVal)
				}
			}
		})
	}
}

// ---------- ToSnakeCase tests ----------

func TestToSnakeCase(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"", ""},
		{"ID", "id"},
		{"UserID", "user_id"},
		{"HTMLParser", "html_parser"},
		{"SimpleTest", "simple_test"},
		{"APIKeyURL", "api_key_url"},
		{"Name", "name"},
		{"firstName", "first_name"},
		{"SSN", "ssn"},
		{"MySSNNumber", "my_ssn_number"},
		{"A", "a"},
		{"already_snake", "already_snake"},
		{"XMLHTTPRequest", "xmlhttp_request"},
		{"User", "user"},
		{"CreatedAt", "created_at"},
		{"DeletedAt", "deleted_at"},
		{"GoIndex", "go_index"},
		{"JSONData", "json_data"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := ToSnakeCase(tt.input)
			if got != tt.want {
				t.Errorf("ToSnakeCase(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}
