package dynamodriver

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ErrBatchIncomplete means DynamoDB still has unprocessed items after bounded retries.
var ErrBatchIncomplete = errors.New("dynamodb: batch still has unprocessed items")

const maxBatchAttempts = 6

// BatchDelete deletes multiple items using BatchWriteItem.
func (d *DynamoDB) BatchDelete(ctx context.Context, keys []string) error {
	requests := make([]types.WriteRequest, 0, len(keys))
	for _, key := range uniqueKeys(keys) {
		requests = append(requests, types.WriteRequest{DeleteRequest: &types.DeleteRequest{Key: d.primaryKey(key)}})
	}
	return d.batchWrite(ctx, requests)
}

// BatchPut puts multiple items using BatchWriteItem.
func (d *DynamoDB) BatchPut(ctx context.Context, items []map[string]types.AttributeValue) error {
	requests := make([]types.WriteRequest, 0, len(items))
	for _, item := range items {
		requests = append(requests, types.WriteRequest{PutRequest: &types.PutRequest{Item: item}})
	}
	return d.batchWrite(ctx, requests)
}

func (d *DynamoDB) batchWrite(ctx context.Context, requests []types.WriteRequest) error {
	for start := 0; start < len(requests); start += 25 {
		pending := requests[start:min(start+25, len(requests))]
		for attempt := 0; len(pending) > 0; attempt++ {
			if err := waitBatchRetry(ctx, attempt); err != nil {
				return fmt.Errorf("dynamodb: batch write: %d unprocessed items: %w", len(pending), err)
			}
			out, err := d.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{d.tableName: pending},
			})
			if err != nil {
				return fmt.Errorf("dynamodb: batch write: %w", err)
			}
			pending = out.UnprocessedItems[d.tableName]
		}
	}
	return nil
}

// BatchGet retrieves unique keys with full attribute access. It retries only
// unprocessed keys, with backoff, and reports incomplete batches as errors.
func (d *DynamoDB) BatchGet(ctx context.Context, keys []string) ([]map[string]types.AttributeValue, error) {
	return d.batchGet(ctx, keys, nil, nil)
}

func (d *DynamoDB) batchGet(ctx context.Context, keys []string, projection *string, names map[string]string) ([]map[string]types.AttributeValue, error) {
	var allItems []map[string]types.AttributeValue
	reqKeys := make([]map[string]types.AttributeValue, 0, len(keys))
	for _, key := range uniqueKeys(keys) {
		reqKeys = append(reqKeys, d.primaryKey(key))
	}
	for start := 0; start < len(reqKeys); start += 100 {
		pending := reqKeys[start:min(start+100, len(reqKeys))]
		for attempt := 0; len(pending) > 0; attempt++ {
			if err := waitBatchRetry(ctx, attempt); err != nil {
				return nil, fmt.Errorf("dynamodb: batch get: %d unprocessed keys: %w", len(pending), err)
			}
			out, err := d.client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
				RequestItems: map[string]types.KeysAndAttributes{d.tableName: {
					Keys: pending, ProjectionExpression: projection, ExpressionAttributeNames: names,
				}},
			})
			if err != nil {
				return nil, fmt.Errorf("dynamodb: batch get: %w", err)
			}
			allItems = append(allItems, out.Responses[d.tableName]...)
			pending = out.UnprocessedKeys[d.tableName].Keys
		}
	}
	return allItems, nil
}

func uniqueKeys(keys []string) []string {
	seen := make(map[string]struct{}, len(keys))
	unique := make([]string, 0, len(keys))
	for _, key := range keys {
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, key)
	}
	return unique
}

// Partial batches return HTTP 200, so the SDK's request retryer does not back off.
func waitBatchRetry(ctx context.Context, attempt int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if attempt == 0 {
		return nil
	}
	if attempt >= maxBatchAttempts {
		return ErrBatchIncomplete
	}
	delay := (25 * time.Millisecond) << (attempt - 1)
	delay = delay/2 + time.Duration(rand.Int64N(int64(delay/2))) //nolint:gosec // Retry jitter does not require cryptographic randomness.
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// CreateTable creates the DynamoDB table with the configured schema.
func (d *DynamoDB) CreateTable(ctx context.Context, opts ...CreateTableOption) error {
	cfg := createTableConfig{
		readCapacity:  5,
		writeCapacity: 5,
		billingMode:   types.BillingModeProvisioned,
	}
	for _, o := range opts {
		o(&cfg)
	}

	input := &dynamodb.CreateTableInput{
		TableName: aws.String(d.tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String(d.pkAttr), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String(d.pkAttr), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: cfg.billingMode,
	}

	if cfg.billingMode == types.BillingModeProvisioned {
		input.ProvisionedThroughput = &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(cfg.readCapacity),
			WriteCapacityUnits: aws.Int64(cfg.writeCapacity),
		}
	}

	_, err := d.client.CreateTable(ctx, input)
	if err != nil {
		return fmt.Errorf("dynamodb: create table: %w", err)
	}
	return nil
}

type createTableConfig struct {
	readCapacity  int64
	writeCapacity int64
	billingMode   types.BillingMode
}

// CreateTableOption configures table creation.
type CreateTableOption func(*createTableConfig)

// WithPayPerRequest sets the billing mode to PAY_PER_REQUEST.
func WithPayPerRequest() CreateTableOption {
	return func(c *createTableConfig) {
		c.billingMode = types.BillingModePayPerRequest
	}
}

// WithProvisionedCapacity sets provisioned read/write capacity.
func WithProvisionedCapacity(read, write int64) CreateTableOption {
	return func(c *createTableConfig) {
		c.billingMode = types.BillingModeProvisioned
		c.readCapacity = read
		c.writeCapacity = write
	}
}
