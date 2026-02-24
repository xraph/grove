// Package dynamodriver provides an AWS DynamoDB KV driver for Grove KV.
//
// DynamoDB is a fully managed, serverless NoSQL database service.
// This driver stores values as binary attributes and supports TTL via
// DynamoDB's native time-to-live feature.
package dynamodriver

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/driver"
)

const (
	defaultPKAttr  = "pk"
	defaultValAttr = "val"
	defaultTTLAttr = "ttl"
)

// DynamoDB implements driver.Driver backed by AWS DynamoDB.
type DynamoDB struct {
	client    *dynamodb.Client
	tableName string
	pkAttr    string
	valAttr   string
	ttlAttr   string
	opts      *driver.DriverOptions
}

var (
	_ driver.Driver      = (*DynamoDB)(nil)
	_ driver.TTLDriver   = (*DynamoDB)(nil)
	_ driver.BatchDriver = (*DynamoDB)(nil)
	_ driver.CASDriver   = (*DynamoDB)(nil)
)

// New creates a new DynamoDB driver with the given client.
// The dsn parameter in Open() is used as the table name.
func New(client *dynamodb.Client, opts ...Option) *DynamoDB {
	d := &DynamoDB{
		client:  client,
		pkAttr:  defaultPKAttr,
		valAttr: defaultValAttr,
		ttlAttr: defaultTTLAttr,
	}
	for _, o := range opts {
		o(d)
	}
	return d
}

// Name returns the driver name.
func (d *DynamoDB) Name() string { return "dynamodb" }

// Open sets the table name from the dsn parameter.
func (d *DynamoDB) Open(_ context.Context, dsn string, opts ...driver.Option) error {
	d.opts = driver.ApplyOptions(opts)
	d.tableName = dsn
	return nil
}

// Close is a no-op for DynamoDB.
func (d *DynamoDB) Close() error { return nil }

// Ping verifies the DynamoDB table is accessible.
func (d *DynamoDB) Ping(ctx context.Context) error {
	_, err := d.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(d.tableName),
	})
	return err
}

// Info returns driver capabilities.
func (d *DynamoDB) Info() driver.DriverInfo {
	return driver.DriverInfo{
		Name:    "dynamodb",
		Version: "2",
		Capabilities: driver.CapTTL | driver.CapBatch |
			driver.CapCAS | driver.CapTransaction,
	}
}

// Get retrieves a value by key.
func (d *DynamoDB) Get(ctx context.Context, key string) ([]byte, error) {
	out, err := d.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(d.tableName),
		Key:       d.primaryKey(key),
	})
	if err != nil {
		return nil, fmt.Errorf("dynamodb: get: %w", err)
	}
	if out.Item == nil {
		return nil, kv.ErrNotFound
	}
	if d.isExpired(out.Item) {
		return nil, kv.ErrNotFound
	}

	valAttr, ok := out.Item[d.valAttr]
	if !ok {
		return nil, kv.ErrNotFound
	}
	bVal, ok := valAttr.(*types.AttributeValueMemberB)
	if !ok {
		return nil, fmt.Errorf("dynamodb: unexpected value type for key %s", key)
	}
	return bVal.Value, nil
}

// Set stores a key-value pair.
func (d *DynamoDB) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	item := map[string]types.AttributeValue{
		d.pkAttr:  &types.AttributeValueMemberS{Value: key},
		d.valAttr: &types.AttributeValueMemberB{Value: value},
	}
	if ttl > 0 {
		expireAt := time.Now().Add(ttl).Unix()
		item[d.ttlAttr] = &types.AttributeValueMemberN{Value: strconv.FormatInt(expireAt, 10)}
	}

	_, err := d.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(d.tableName),
		Item:      item,
	})
	if err != nil {
		return fmt.Errorf("dynamodb: set: %w", err)
	}
	return nil
}

// Delete removes one or more keys and returns the count deleted.
func (d *DynamoDB) Delete(ctx context.Context, keys ...string) (int64, error) {
	var count int64
	for _, key := range keys {
		out, err := d.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName:    aws.String(d.tableName),
			Key:          d.primaryKey(key),
			ReturnValues: types.ReturnValueAllOld,
		})
		if err != nil {
			return count, fmt.Errorf("dynamodb: delete %s: %w", key, err)
		}
		if out.Attributes != nil {
			count++
		}
	}
	return count, nil
}

// Exists checks if keys exist.
func (d *DynamoDB) Exists(ctx context.Context, keys ...string) (int64, error) {
	var count int64
	for _, key := range keys {
		out, err := d.client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName:            aws.String(d.tableName),
			Key:                  d.primaryKey(key),
			ProjectionExpression: aws.String(d.pkAttr),
		})
		if err != nil {
			return 0, fmt.Errorf("dynamodb: exists: %w", err)
		}
		if out.Item != nil && !d.isExpired(out.Item) {
			count++
		}
	}
	return count, nil
}

// --- TTLDriver ---

// TTL returns the remaining time-to-live for a key.
func (d *DynamoDB) TTL(ctx context.Context, key string) (time.Duration, error) {
	out, err := d.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:            aws.String(d.tableName),
		Key:                  d.primaryKey(key),
		ProjectionExpression: aws.String(fmt.Sprintf("%s, %s", d.pkAttr, d.ttlAttr)),
	})
	if err != nil {
		return 0, fmt.Errorf("dynamodb: ttl: %w", err)
	}
	if out.Item == nil {
		return 0, kv.ErrNotFound
	}

	ttlAttr, ok := out.Item[d.ttlAttr]
	if !ok {
		return -1, nil // no expiry
	}
	nVal, ok := ttlAttr.(*types.AttributeValueMemberN)
	if !ok {
		return -1, nil
	}
	expireAt, err := strconv.ParseInt(nVal.Value, 10, 64)
	if err != nil {
		return -1, nil
	}
	remaining := time.Until(time.Unix(expireAt, 0))
	if remaining <= 0 {
		return 0, kv.ErrNotFound
	}
	return remaining, nil
}

// Expire sets a TTL on an existing key.
func (d *DynamoDB) Expire(ctx context.Context, key string, ttl time.Duration) error {
	expireAt := time.Now().Add(ttl).Unix()
	_, err := d.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String(d.tableName),
		Key:              d.primaryKey(key),
		UpdateExpression: aws.String("SET #ttl = :ttl"),
		ExpressionAttributeNames: map[string]string{
			"#ttl": d.ttlAttr,
			"#pk":  d.pkAttr,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":ttl": &types.AttributeValueMemberN{Value: strconv.FormatInt(expireAt, 10)},
		},
		ConditionExpression: aws.String("attribute_exists(#pk)"),
	})
	if err != nil {
		return fmt.Errorf("dynamodb: expire: %w", err)
	}
	return nil
}

// --- BatchDriver ---

// MGet retrieves multiple keys.
func (d *DynamoDB) MGet(ctx context.Context, keys []string) ([][]byte, error) {
	results := make([][]byte, len(keys))
	keyMap := make(map[string]int, len(keys))

	reqKeys := make([]map[string]types.AttributeValue, 0, len(keys))
	for i, key := range keys {
		keyMap[key] = i
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
			return nil, fmt.Errorf("dynamodb: mget: %w", err)
		}

		for _, item := range out.Responses[d.tableName] {
			if d.isExpired(item) {
				continue
			}
			pkAttr, ok := item[d.pkAttr]
			if !ok {
				continue
			}
			sVal, ok := pkAttr.(*types.AttributeValueMemberS)
			if !ok {
				continue
			}
			idx, ok := keyMap[sVal.Value]
			if !ok {
				continue
			}
			valAttr, ok := item[d.valAttr]
			if !ok {
				continue
			}
			bVal, ok := valAttr.(*types.AttributeValueMemberB)
			if !ok {
				continue
			}
			results[idx] = bVal.Value
		}
	}

	return results, nil
}

// MSet stores multiple key-value pairs.
func (d *DynamoDB) MSet(ctx context.Context, pairs map[string][]byte, ttl time.Duration) error {
	var requests []types.WriteRequest
	for key, val := range pairs {
		item := map[string]types.AttributeValue{
			d.pkAttr:  &types.AttributeValueMemberS{Value: key},
			d.valAttr: &types.AttributeValueMemberB{Value: val},
		}
		if ttl > 0 {
			expireAt := time.Now().Add(ttl).Unix()
			item[d.ttlAttr] = &types.AttributeValueMemberN{Value: strconv.FormatInt(expireAt, 10)}
		}
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
			return fmt.Errorf("dynamodb: mset: %w", err)
		}
	}
	return nil
}

// --- CASDriver ---

// SetNX sets a key only if it does not exist.
func (d *DynamoDB) SetNX(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	item := map[string]types.AttributeValue{
		d.pkAttr:  &types.AttributeValueMemberS{Value: key},
		d.valAttr: &types.AttributeValueMemberB{Value: value},
	}
	if ttl > 0 {
		expireAt := time.Now().Add(ttl).Unix()
		item[d.ttlAttr] = &types.AttributeValueMemberN{Value: strconv.FormatInt(expireAt, 10)}
	}

	_, err := d.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(d.tableName),
		Item:                item,
		ConditionExpression: aws.String("attribute_not_exists(#pk)"),
		ExpressionAttributeNames: map[string]string{
			"#pk": d.pkAttr,
		},
	})
	if err != nil {
		if isConditionalCheckFailed(err) {
			return false, nil
		}
		return false, fmt.Errorf("dynamodb: setnx: %w", err)
	}
	return true, nil
}

// SetXX sets a key only if it already exists.
func (d *DynamoDB) SetXX(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	item := map[string]types.AttributeValue{
		d.pkAttr:  &types.AttributeValueMemberS{Value: key},
		d.valAttr: &types.AttributeValueMemberB{Value: value},
	}
	if ttl > 0 {
		expireAt := time.Now().Add(ttl).Unix()
		item[d.ttlAttr] = &types.AttributeValueMemberN{Value: strconv.FormatInt(expireAt, 10)}
	}

	_, err := d.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(d.tableName),
		Item:                item,
		ConditionExpression: aws.String("attribute_exists(#pk)"),
		ExpressionAttributeNames: map[string]string{
			"#pk": d.pkAttr,
		},
	})
	if err != nil {
		if isConditionalCheckFailed(err) {
			return false, nil
		}
		return false, fmt.Errorf("dynamodb: setxx: %w", err)
	}
	return true, nil
}

// Client returns the underlying DynamoDB client.
func (d *DynamoDB) Client() *dynamodb.Client {
	return d.client
}

// Table returns the configured table name.
func (d *DynamoDB) Table() string {
	return d.tableName
}

// --- Helpers ---

func (d *DynamoDB) primaryKey(key string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		d.pkAttr: &types.AttributeValueMemberS{Value: key},
	}
}

func (d *DynamoDB) isExpired(item map[string]types.AttributeValue) bool {
	ttlAttr, ok := item[d.ttlAttr]
	if !ok {
		return false
	}
	nVal, ok := ttlAttr.(*types.AttributeValueMemberN)
	if !ok {
		return false
	}
	expireAt, err := strconv.ParseInt(nVal.Value, 10, 64)
	if err != nil {
		return false
	}
	return time.Now().Unix() > expireAt
}

func isConditionalCheckFailed(err error) bool {
	var ccf *types.ConditionalCheckFailedException
	return errors.As(err, &ccf)
}
