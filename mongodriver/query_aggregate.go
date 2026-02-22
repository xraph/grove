package mongodriver

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// AggregateQuery builds and executes MongoDB aggregation pipelines.
// Use MongoDB.NewAggregate() to create one.
type AggregateQuery struct {
	db         *MongoDB
	session    *mongo.Session
	collection string
	pipeline   bson.A
	err        error
}

// NewAggregate creates a new aggregation pipeline query for the given collection.
func (db *MongoDB) NewAggregate(collection string) *AggregateQuery {
	return &AggregateQuery{
		db:         db,
		collection: collection,
		pipeline:   bson.A{},
	}
}

// Match adds a $match stage to the pipeline.
func (q *AggregateQuery) Match(filter bson.M) *AggregateQuery {
	q.pipeline = append(q.pipeline, bson.M{"$match": filter})
	return q
}

// Group adds a $group stage to the pipeline.
func (q *AggregateQuery) Group(group bson.M) *AggregateQuery {
	q.pipeline = append(q.pipeline, bson.M{"$group": group})
	return q
}

// Sort adds a $sort stage to the pipeline.
func (q *AggregateQuery) Sort(sort bson.D) *AggregateQuery {
	q.pipeline = append(q.pipeline, bson.M{"$sort": sort})
	return q
}

// Project adds a $project stage to the pipeline.
func (q *AggregateQuery) Project(projection bson.M) *AggregateQuery {
	q.pipeline = append(q.pipeline, bson.M{"$project": projection})
	return q
}

// Unwind adds a $unwind stage to the pipeline.
func (q *AggregateQuery) Unwind(path string) *AggregateQuery {
	q.pipeline = append(q.pipeline, bson.M{"$unwind": path})
	return q
}

// Lookup adds a $lookup stage to the pipeline.
func (q *AggregateQuery) Lookup(lookup bson.M) *AggregateQuery {
	q.pipeline = append(q.pipeline, bson.M{"$lookup": lookup})
	return q
}

// Limit adds a $limit stage to the pipeline.
func (q *AggregateQuery) Limit(n int64) *AggregateQuery {
	q.pipeline = append(q.pipeline, bson.M{"$limit": n})
	return q
}

// Skip adds a $skip stage to the pipeline.
func (q *AggregateQuery) Skip(n int64) *AggregateQuery {
	q.pipeline = append(q.pipeline, bson.M{"$skip": n})
	return q
}

// Stage adds a custom pipeline stage. Use this for stages not covered
// by the convenience methods (e.g., $addFields, $bucket, etc.).
func (q *AggregateQuery) Stage(stage bson.M) *AggregateQuery {
	q.pipeline = append(q.pipeline, stage)
	return q
}

// GetPipeline returns the current aggregation pipeline. Useful for testing.
func (q *AggregateQuery) GetPipeline() bson.A {
	return q.pipeline
}

// GetCollection returns the collection name. Useful for testing.
func (q *AggregateQuery) GetCollection() string {
	return q.collection
}

// Scan executes the aggregation pipeline and decodes results into dest.
// dest should be a pointer to a slice.
func (q *AggregateQuery) Scan(ctx context.Context, dest any) error {
	if q.err != nil {
		return q.err
	}
	if q.collection == "" {
		return fmt.Errorf("mongodriver: no collection specified")
	}

	coll := q.db.Collection(q.collection)

	if q.session != nil {
		ctx = mongo.NewSessionContext(ctx, q.session)
	}

	cursor, err := coll.Aggregate(ctx, q.pipeline)
	if err != nil {
		return fmt.Errorf("mongodriver: aggregate: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	if err := cursor.All(ctx, dest); err != nil {
		return fmt.Errorf("mongodriver: aggregate decode: %w", err)
	}

	return nil
}
