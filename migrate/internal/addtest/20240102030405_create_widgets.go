// Package addtest holds one migration registered with Group.Add from a file
// named for its version. The migrate tests import it to check Add against a
// real call site, which an in-package test can't do without an import cycle.
package addtest

import (
	"context"

	"github.com/xraph/grove/migrate"
)

// Group is the group the migration in this package registers into.
var Group = migrate.NewGroup("addtest")

func init() {
	Group.Add(up, nil)
}

func up(_ context.Context, _ *migrate.DB) error {
	return nil
}
