package mongodriver

// MongoOption configures the MongoDB driver during Open.
type MongoOption func(*mongoOptions)

// mongoOptions holds MongoDB-specific configuration.
type mongoOptions struct {
	// Database overrides the database name from the connection URI.
	Database string
}

func defaultMongoOptions() *mongoOptions {
	return &mongoOptions{}
}

func (o *mongoOptions) apply(opts []MongoOption) {
	for _, fn := range opts {
		fn(o)
	}
}

// WithDatabase overrides the database name extracted from the connection URI.
// If not set, the database name from the URI is used.
func WithDatabase(name string) MongoOption {
	return func(o *mongoOptions) {
		o.Database = name
	}
}
