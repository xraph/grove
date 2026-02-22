package schema

import (
	"errors"
	"reflect"
	"testing"
)

func TestParseRelation(t *testing.T) {
	dummyField := &Field{
		GoName: "Posts",
		GoType: reflect.TypeOf([]struct{}{}),
	}

	tests := []struct {
		name       string
		tag        string
		field      *Field
		wantType   RelationType
		wantBase   string
		wantJoin   string
		wantTable  string
		wantErr    bool
		errContain string
	}{
		{
			name:     "has-one",
			tag:      "rel:has-one,join:id=profile_id",
			field:    dummyField,
			wantType: HasOne,
			wantBase: "id",
			wantJoin: "profile_id",
		},
		{
			name:     "has-many",
			tag:      "rel:has-many,join:id=user_id",
			field:    dummyField,
			wantType: HasMany,
			wantBase: "id",
			wantJoin: "user_id",
		},
		{
			name:     "belongs-to",
			tag:      "rel:belongs-to,join:author_id=id",
			field:    dummyField,
			wantType: BelongsTo,
			wantBase: "author_id",
			wantJoin: "id",
		},
		{
			name:      "many-to-many",
			tag:       "rel:many-to-many,join_table:user_roles,join:id=user_id",
			field:     dummyField,
			wantType:  ManyToMany,
			wantBase:  "id",
			wantJoin:  "user_id",
			wantTable: "user_roles",
		},
		{
			name:     "hasone alternate spelling",
			tag:      "rel:hasone,join:id=profile_id",
			field:    dummyField,
			wantType: HasOne,
			wantBase: "id",
			wantJoin: "profile_id",
		},
		{
			name:      "m2m shorthand",
			tag:       "rel:m2m,join_table:user_tags,join:id=user_id",
			field:     dummyField,
			wantType:  ManyToMany,
			wantBase:  "id",
			wantJoin:  "user_id",
			wantTable: "user_tags",
		},
		{
			name:       "missing rel option",
			tag:        "join:id=user_id",
			field:      dummyField,
			wantErr:    true,
			errContain: "missing 'rel'",
		},
		{
			name:       "unknown relation type",
			tag:        "rel:unknown,join:id=user_id",
			field:      dummyField,
			wantErr:    true,
			errContain: "unknown relation type",
		},
		{
			name:       "invalid join format",
			tag:        "rel:has-one,join:noequalssign",
			field:      dummyField,
			wantErr:    true,
			errContain: "invalid join format",
		},
		{
			name:       "many-to-many without join_table",
			tag:        "rel:many-to-many,join:id=user_id",
			field:      dummyField,
			wantErr:    true,
			errContain: "join_table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rel, err := ParseRelation(tt.tag, tt.field)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tt.errContain != "" && !containsStr(err.Error(), tt.errContain) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.errContain)
				}
				if !errors.Is(err, ErrInvalidRelation) {
					t.Errorf("expected error to wrap ErrInvalidRelation")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if rel.Type != tt.wantType {
				t.Errorf("Type = %v, want %v", rel.Type, tt.wantType)
			}
			if rel.BaseColumn != tt.wantBase {
				t.Errorf("BaseColumn = %q, want %q", rel.BaseColumn, tt.wantBase)
			}
			if rel.JoinColumn != tt.wantJoin {
				t.Errorf("JoinColumn = %q, want %q", rel.JoinColumn, tt.wantJoin)
			}
			if rel.JoinTable != tt.wantTable {
				t.Errorf("JoinTable = %q, want %q", rel.JoinTable, tt.wantTable)
			}
			if rel.Field != tt.field {
				t.Error("Field pointer mismatch")
			}
		})
	}
}

func TestRelationTypeString(t *testing.T) {
	tests := []struct {
		rt   RelationType
		want string
	}{
		{HasOne, "has-one"},
		{HasMany, "has-many"},
		{BelongsTo, "belongs-to"},
		{ManyToMany, "many-to-many"},
		{RelationType(99), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := tt.rt.String()
			if got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

// containsStr is a simple substring check helper.
func containsStr(s, substr string) bool {
	return len(s) >= len(substr) && searchStr(s, substr)
}

func searchStr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
