package dynamodriver

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// BatchDelete deletes multiple items using BatchWriteItem.
func (d *DynamoDB) BatchDelete(ctx context.Context, keys []string) error {
	var requests []types.WriteRequest
	for _, key := range keys {
		requests = append(requests, types.WriteRequest{
			DeleteRequest: &types.DeleteRequest{
				Key: d.primaryKey(key),
			},
		})
	}

	// BatchWriteItem supports max 25 items per request.
	for start := 0; start < len(requests); start += 25 {
		end := start + 25
		if end > len(requests) {
			end = len(requests)
		}
		_, err := d.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				d.tableName: requests[start:end],
			},
		})
		if err != nil {
			return fmt.Errorf("dynamodb: batch delete: %w", err)
		}
	}
	return nil
}

// BatchPut puts multiple items using BatchWriteItem.
func (d *DynamoDB) BatchPut(ctx context.Context, items []map[string]types.AttributeValue) error {
	var requests []types.WriteRequest
	for _, item := range items {
		requests = append(requests, types.WriteRequest{
			PutRequest: &types.PutRequest{Item: item},
		})
	}

	for start := 0; start < len(requests); start += 25 {
		end := start + 25
		if end > len(requests) {
			end = len(requests)
		}
		_, err := d.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				d.tableName: requests[start:end],
			},
		})
		if err != nil {
			return fmt.Errorf("dynamodb: batch put: %w", err)
		}
	}
	return nil
}

// BatchGet retrieves multiple items using BatchGetItem with full attribute access.
func (d *DynamoDB) BatchGet(ctx context.Context, keys []string) ([]map[string]types.AttributeValue, error) {
	var allItems []map[string]types.AttributeValue

	reqKeys := make([]map[string]types.AttributeValue, 0, len(keys))
	for _, key := range keys {
		reqKeys = append(reqKeys, d.primaryKey(key))
	}

	for start := 0; start < len(reqKeys); start += 100 {
		end := start + 100
		if end > len(reqKeys) {
			end = len(reqKeys)
		}

		out, err := d.client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
			RequestItems: map[string]types.KeysAndAttributes{
				d.tableName: {Keys: reqKeys[start:end]},
			},
		})
		if err != nil {
			return nil, fmt.Errorf("dynamodb: batch get: %w", err)
		}

		allItems = append(allItems, out.Responses[d.tableName]...)

		// Handle unprocessed keys with retry.
		unprocessed := out.UnprocessedKeys
		for len(unprocessed) > 0 && len(unprocessed[d.tableName].Keys) > 0 {
			retryOut, retryErr := d.client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
				RequestItems: unprocessed,
			})
			if retryErr != nil {
				return nil, fmt.Errorf("dynamodb: batch get retry: %w", retryErr)
			}
			allItems = append(allItems, retryOut.Responses[d.tableName]...)
			unprocessed = retryOut.UnprocessedKeys
		}
	}

	return allItems, nil
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
