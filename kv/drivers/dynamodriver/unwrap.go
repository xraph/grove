package dynamodriver

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/xraph/grove/kv"
)

// Unwrap extracts the underlying DynamoDB driver from a Store.
func Unwrap(store *kv.Store) *DynamoDB {
	if d, ok := store.Driver().(*DynamoDB); ok {
		return d
	}
	return nil
}

// UnwrapClient extracts the underlying DynamoDB client from a Store.
func UnwrapClient(store *kv.Store) *dynamodb.Client {
	if d := Unwrap(store); d != nil {
		return d.Client()
	}
	return nil
}
