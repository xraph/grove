package schema

import (
	"fmt"
	"strings"
)

// RelationType identifies the kind of relation.
type RelationType int

const (
	// HasOne indicates a one-to-one relationship where the related model
	// holds the foreign key.
	HasOne RelationType = iota
	// HasMany indicates a one-to-many relationship where multiple related
	// models hold a foreign key back to this model.
	HasMany
	// BelongsTo indicates a many-to-one relationship where this model
	// holds the foreign key to the related model.
	BelongsTo
	// ManyToMany indicates a many-to-many relationship through a join table.
	ManyToMany
)

// String returns the human-readable name of the relation type.
func (rt RelationType) String() string {
	switch rt {
	case HasOne:
		return "has-one"
	case HasMany:
		return "has-many"
	case BelongsTo:
		return "belongs-to"
	case ManyToMany:
		return "many-to-many"
	default:
		return "unknown"
	}
}

// Relation represents a relationship between two models.
type Relation struct {
	Type       RelationType
	Field      *Field // The struct field holding the relation
	JoinTable  string // For many-to-many: join table name
	BaseColumn string // Column on the base table
	JoinColumn string // Column on the related/join table
}

// ParseRelation parses relation info from a tag-derived options.
//
// The tag format is:
//
//	grove:"rel:has-many,join:id=user_id"
//	grove:"rel:many-to-many,join_table:user_roles,join:id=user_id"
//	grove:"rel:has-one,join:id=profile_id"
//	grove:"rel:belongs-to,join:author_id=id"
//
// The "rel" option specifies the relation type.
// The "join" option specifies the column mapping as "base_col=join_col".
// The "join_table" option is required for many-to-many relations.
func ParseRelation(tag string, field *Field) (*Relation, error) {
	parsed := ParseTag(tag)

	relTypeStr := parsed.GetOption("rel")
	if relTypeStr == "" {
		return nil, fmt.Errorf("%w: missing 'rel' option in tag %q", ErrInvalidRelation, tag)
	}

	var relType RelationType
	switch strings.ToLower(relTypeStr) {
	case "has-one", "hasone":
		relType = HasOne
	case "has-many", "hasmany":
		relType = HasMany
	case "belongs-to", "belongsto":
		relType = BelongsTo
	case "many-to-many", "manytomany", "m2m":
		relType = ManyToMany
	default:
		return nil, fmt.Errorf("%w: unknown relation type %q", ErrInvalidRelation, relTypeStr)
	}

	rel := &Relation{
		Type:  relType,
		Field: field,
	}

	// Parse join column mapping: "base_col=join_col".
	joinStr := parsed.GetOption("join")
	if joinStr != "" {
		parts := strings.SplitN(joinStr, "=", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("%w: invalid join format %q, expected 'base_col=join_col'", ErrInvalidRelation, joinStr)
		}
		rel.BaseColumn = parts[0]
		rel.JoinColumn = parts[1]
	}

	// Parse join table for many-to-many.
	if jt := parsed.GetOption("join_table"); jt != "" {
		rel.JoinTable = jt
	}

	// Validate many-to-many requires a join table.
	if relType == ManyToMany && rel.JoinTable == "" {
		return nil, fmt.Errorf("%w: many-to-many relation requires 'join_table' option", ErrInvalidRelation)
	}

	return rel, nil
}

// ErrInvalidRelation is returned when a relation definition is incomplete or incorrect.
// Re-exported from the root grove package for convenience within the schema package.
var ErrInvalidRelation = fmt.Errorf("grove: invalid relation definition")
