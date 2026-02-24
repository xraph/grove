package dynamodriver

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// UpdateBuilder provides a fluent API for building DynamoDB UpdateItem expressions.
type UpdateBuilder struct {
	d          *DynamoDB
	key        string
	setExprs   []string
	removeExpr []string
	names      map[string]string
	values     map[string]types.AttributeValue
	condition  *string
	counter    int
}

// Update starts building an UpdateItem expression for the given key.
func (d *DynamoDB) Update(key string) *UpdateBuilder {
	return &UpdateBuilder{
		d:      d,
		key:    key,
		names:  make(map[string]string),
		values: make(map[string]types.AttributeValue),
	}
}

// Set adds a SET expression.
func (ub *UpdateBuilder) Set(attr string, value types.AttributeValue) *UpdateBuilder {
	nameRef := fmt.Sprintf("#a%d", ub.counter)
	valRef := fmt.Sprintf(":v%d", ub.counter)
	ub.counter++
	ub.names[nameRef] = attr
	ub.values[valRef] = value
	ub.setExprs = append(ub.setExprs, fmt.Sprintf("%s = %s", nameRef, valRef))
	return ub
}

// Remove adds a REMOVE expression.
func (ub *UpdateBuilder) Remove(attr string) *UpdateBuilder {
	nameRef := fmt.Sprintf("#a%d", ub.counter)
	ub.counter++
	ub.names[nameRef] = attr
	ub.removeExpr = append(ub.removeExpr, nameRef)
	return ub
}

// Condition sets a condition expression.
func (ub *UpdateBuilder) Condition(expr string) *UpdateBuilder {
	ub.condition = aws.String(expr)
	return ub
}

// Exec executes the update.
func (ub *UpdateBuilder) Exec(ctx context.Context) error {
	var updateExpr string
	if len(ub.setExprs) > 0 {
		updateExpr = "SET "
		for i, s := range ub.setExprs {
			if i > 0 {
				updateExpr += ", "
			}
			updateExpr += s
		}
	}
	if len(ub.removeExpr) > 0 {
		if updateExpr != "" {
			updateExpr += " "
		}
		updateExpr += "REMOVE "
		for i, r := range ub.removeExpr {
			if i > 0 {
				updateExpr += ", "
			}
			updateExpr += r
		}
	}

	input := &dynamodb.UpdateItemInput{
		TableName:                 aws.String(ub.d.tableName),
		Key:                       ub.d.primaryKey(ub.key),
		UpdateExpression:          aws.String(updateExpr),
		ExpressionAttributeNames:  ub.names,
		ExpressionAttributeValues: ub.values,
		ConditionExpression:       ub.condition,
	}

	_, err := ub.d.client.UpdateItem(ctx, input)
	if err != nil {
		return fmt.Errorf("dynamodb: update: %w", err)
	}
	return nil
}
