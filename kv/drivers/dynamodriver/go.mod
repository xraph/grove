module github.com/xraph/grove/kv/drivers/dynamodriver

go 1.25.7

replace (
	github.com/xraph/grove => ../../../
	github.com/xraph/grove/kv => ../../
)

require (
	github.com/aws/aws-sdk-go-v2 v1.41.2
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.56.0
	github.com/xraph/grove/kv v0.0.0
)

require (
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.18 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.18 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.11.18 // indirect
	github.com/aws/smithy-go v1.24.1 // indirect
	github.com/vmihailenco/msgpack/v5 v5.4.1 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	github.com/xraph/grove v0.0.0 // indirect
	google.golang.org/protobuf v1.36.6 // indirect
)
