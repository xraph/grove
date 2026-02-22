package grove

// BaseModel is embedded in user structs to mark them as Grove models.
// It carries table-level metadata via struct tags.
//
// Supports both grove:"..." and bun:"..." tags:
//
//	type User struct {
//	    grove.BaseModel `grove:"table:users,alias:u"`
//	    ID   int64  `grove:"id,pk,autoincrement"`
//	    Name string `grove:"name,notnull"`
//	}
//
// When both tags are present on the same struct, grove takes precedence.
// When only bun tags are present, they are used as fallback for
// zero-cost migration from bun.
type BaseModel struct{}
