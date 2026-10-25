package clickhousemigrate

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/migrate"
)

func TestExecutor_DriverName(t *testing.T) {
	name, ok := migrate.DriverNameOf(New(nil))
	require.True(t, ok)
	assert.Equal(t, "clickhouse", name)
}

// There is no ClickHouse schema renderer yet. A builder has to say so and name the
// driver, not fail on an unknown executor, and it must stop before the
// executor, which here has no driver behind it.
func TestExecutor_buildersAreUnsupported(t *testing.T) {
	db := migrate.NewDB(New(nil))

	err := db.NewDropTable("t").Exec(context.Background())

	require.ErrorIs(t, err, migrate.ErrSchemaUnsupported)
	assert.NotErrorIs(t, err, migrate.ErrUnknownDriver)
	assert.Contains(t, err.Error(), "clickhouse")
}
