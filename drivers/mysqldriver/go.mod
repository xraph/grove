module github.com/xraph/grove/drivers/mysqldriver

go 1.25.7

replace github.com/xraph/grove => ../../

require (
	github.com/go-sql-driver/mysql v1.9.3
	github.com/stretchr/testify v1.11.1
	github.com/xraph/grove v0.0.0
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/xraph/go-utils v1.1.1 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
