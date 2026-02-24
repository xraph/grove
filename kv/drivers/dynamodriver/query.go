package dynamodriver

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// QueryResult holds the results of a Query or Scan operation.
type QueryResult struct {
	Items            []map[string]types.AttributeValue
	Count            int32
	ScannedCount     int32
	LastEvaluatedKey map[string]types.AttributeValue
}

// QueryInput configures a DynamoDB Query operation.
type QueryInput struct {
	IndexName                string
	KeyConditionExpression   string
	FilterExpression         string
	ProjectionExpression     string
	ExpressionAttributeNames map[string]string
	ExpressionValues         map[string]types.AttributeValue
	Limit                    *int32
	ScanForward              *bool
	ExclusiveStartKey        map[string]types.AttributeValue
}

// Query executes a DynamoDB Query operation, typically against a GSI or LSI.
func (d *DynamoDB) Query(ctx context.Context, input QueryInput) (*QueryResult, error) {
	qi := &dynamodb.QueryInput{
		TableName:                 aws.String(d.tableName),
		KeyConditionExpression:    aws.String(input.KeyConditionExpression),
		ExpressionAttributeValues: input.ExpressionValues,
	}
	if input.IndexName != "" {
		qi.IndexName = aws.String(input.IndexName)
	}
	if input.FilterExpression != "" {
		qi.FilterExpression = aws.String(input.FilterExpression)
	}
	if input.ProjectionExpression != "" {
		qi.ProjectionExpression = aws.String(input.ProjectionExpression)
	}
	if len(input.ExpressionAttributeNames) > 0 {
		qi.ExpressionAttributeNames = input.ExpressionAttributeNames
	}
	if input.Limit != nil {
		qi.Limit = input.Limit
	}
	if input.ScanForward != nil {
		qi.ScanIndexForward = input.ScanForward
	}
	if input.ExclusiveStartKey != nil {
		qi.ExclusiveStartKey = input.ExclusiveStartKey
	}

	out, err := d.client.Query(ctx, qi)
	if err != nil {
		return nil, fmt.Errorf("dynamodb: query: %w", err)
	}

	return &QueryResult{
		Items:            out.Items,
		Count:            out.Count,
		ScannedCount:     out.ScannedCount,
		LastEvaluatedKey: out.LastEvaluatedKey,
	}, nil
}

// ScanInput configures a DynamoDB Scan operation.
type ScanInput struct {
	IndexName                string
	FilterExpression         string
	ProjectionExpression     string
	ExpressionAttributeNames map[string]string
	ExpressionValues         map[string]types.AttributeValue
	Limit                    *int32
	ExclusiveStartKey        map[string]types.AttributeValue
}

// ScanTable executes a DynamoDB Scan operation.
func (d *DynamoDB) ScanTable(ctx context.Context, input ScanInput) (*QueryResult, error) {
	si := &dynamodb.ScanInput{
		TableName: aws.String(d.tableName),
	}
	if input.IndexName != "" {
		si.IndexName = aws.String(input.IndexName)
	}
	if input.FilterExpression != "" {
		si.FilterExpression = aws.String(input.FilterExpression)
		si.ExpressionAttributeValues = input.ExpressionValues
	}
	if input.ProjectionExpression != "" {
		si.ProjectionExpression = aws.String(input.ProjectionExpression)
	}
	if len(input.ExpressionAttributeNames) > 0 {
		si.ExpressionAttributeNames = input.ExpressionAttributeNames
	}
	if input.Limit != nil {
		si.Limit = input.Limit
	}
	if input.ExclusiveStartKey != nil {
		si.ExclusiveStartKey = input.ExclusiveStartKey
	}

	out, err := d.client.Scan(ctx, si)
	if err != nil {
		return nil, fmt.Errorf("dynamodb: scan: %w", err)
	}

	return &QueryResult{
		Items:            out.Items,
		Count:            out.Count,
		ScannedCount:     out.ScannedCount,
		LastEvaluatedKey: out.LastEvaluatedKey,
	}, nil
}
