package dynamodriver

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// TransactGet retrieves multiple items in a transaction.
func (d *DynamoDB) TransactGet(ctx context.Context, keys []string) ([]map[string]types.AttributeValue, error) {
	gets := make([]types.TransactGetItem, 0, len(keys))
	for _, key := range keys {
		gets = append(gets, types.TransactGetItem{
			Get: &types.Get{
				TableName: aws.String(d.tableName),
				Key:       d.primaryKey(key),
			},
		})
	}

	out, err := d.client.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{
		TransactItems: gets,
	})
	if err != nil {
		return nil, fmt.Errorf("dynamodb: transact get: %w", err)
	}

	results := make([]map[string]types.AttributeValue, len(keys))
	for i, resp := range out.Responses {
		results[i] = resp.Item
	}
	return results, nil
}

// TransactWriteItem represents a single write operation in a transaction.
type TransactWriteItem struct {
	// Put stores an item.
	Put *TransactPut
	// Delete removes an item.
	Delete *TransactDelete
	// Update modifies an item.
	Update *TransactUpdate
	// ConditionCheck verifies a condition without modifying.
	ConditionCheck *TransactConditionCheck
}

// TransactPut is a put operation within a transaction.
type TransactPut struct {
	Item                map[string]types.AttributeValue
	ConditionExpression *string
	ExpressionNames     map[string]string
	ExpressionValues    map[string]types.AttributeValue
}

// TransactDelete is a delete operation within a transaction.
type TransactDelete struct {
	Key                 map[string]types.AttributeValue
	ConditionExpression *string
	ExpressionNames     map[string]string
	ExpressionValues    map[string]types.AttributeValue
}

// TransactUpdate is an update operation within a transaction.
type TransactUpdate struct {
	Key                 map[string]types.AttributeValue
	UpdateExpression    string
	ConditionExpression *string
	ExpressionNames     map[string]string
	ExpressionValues    map[string]types.AttributeValue
}

// TransactConditionCheck verifies a condition without modifying.
type TransactConditionCheck struct {
	Key                 map[string]types.AttributeValue
	ConditionExpression string
	ExpressionNames     map[string]string
	ExpressionValues    map[string]types.AttributeValue
}

// TransactWrite executes multiple write operations in a transaction.
func (d *DynamoDB) TransactWrite(ctx context.Context, items []TransactWriteItem) error {
	writes := make([]types.TransactWriteItem, 0, len(items))

	for _, item := range items {
		var twi types.TransactWriteItem

		switch {
		case item.Put != nil:
			twi.Put = &types.Put{
				TableName:                 aws.String(d.tableName),
				Item:                      item.Put.Item,
				ConditionExpression:       item.Put.ConditionExpression,
				ExpressionAttributeNames:  item.Put.ExpressionNames,
				ExpressionAttributeValues: item.Put.ExpressionValues,
			}
		case item.Delete != nil:
			twi.Delete = &types.Delete{
				TableName:                 aws.String(d.tableName),
				Key:                       item.Delete.Key,
				ConditionExpression:       item.Delete.ConditionExpression,
				ExpressionAttributeNames:  item.Delete.ExpressionNames,
				ExpressionAttributeValues: item.Delete.ExpressionValues,
			}
		case item.Update != nil:
			twi.Update = &types.Update{
				TableName:                 aws.String(d.tableName),
				Key:                       item.Update.Key,
				UpdateExpression:          aws.String(item.Update.UpdateExpression),
				ConditionExpression:       item.Update.ConditionExpression,
				ExpressionAttributeNames:  item.Update.ExpressionNames,
				ExpressionAttributeValues: item.Update.ExpressionValues,
			}
		case item.ConditionCheck != nil:
			twi.ConditionCheck = &types.ConditionCheck{
				TableName:                 aws.String(d.tableName),
				Key:                       item.ConditionCheck.Key,
				ConditionExpression:       aws.String(item.ConditionCheck.ConditionExpression),
				ExpressionAttributeNames:  item.ConditionCheck.ExpressionNames,
				ExpressionAttributeValues: item.ConditionCheck.ExpressionValues,
			}
		}

		writes = append(writes, twi)
	}

	_, err := d.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: writes,
	})
	if err != nil {
		return fmt.Errorf("dynamodb: transact write: %w", err)
	}
	return nil
}
