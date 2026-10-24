package migrate_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/migrate/internal/addtest"
)

// The addtest package registers its migration from an init in a file named
// 20240102030405_create_widgets.go. An in-package test can't import it
// without a cycle, so the real call site gets checked from out here.
func TestAdd_takesVersionAndNameFromFile(t *testing.T) {
	ms := addtest.Group.Migrations()
	require.Len(t, ms, 1)

	m := ms[0]
	assert.Equal(t, "20240102030405", m.Version)
	assert.Equal(t, "create_widgets", m.Name)
	assert.Equal(t, "addtest", m.Group)
	assert.True(t, m.Transactional)
	assert.NotNil(t, m.Up)
	assert.Nil(t, m.Down)
}
