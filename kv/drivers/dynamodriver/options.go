package dynamodriver

// Option configures a DynamoDB driver.
type Option func(*DynamoDB)

// WithPKAttribute sets the primary key attribute name (default: "pk").
func WithPKAttribute(attr string) Option {
	return func(d *DynamoDB) { d.pkAttr = attr }
}

// WithValueAttribute sets the value attribute name (default: "val").
func WithValueAttribute(attr string) Option {
	return func(d *DynamoDB) { d.valAttr = attr }
}

// WithTTLAttribute sets the TTL attribute name (default: "ttl").
func WithTTLAttribute(attr string) Option {
	return func(d *DynamoDB) { d.ttlAttr = attr }
}
