package migrate

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"sync"
)

// ErrSchemaUnsupported is wrapped by the error a driver returns when it can't
// carry out a schema operation, or has no schema renderer at all.
var ErrSchemaUnsupported = errors.New("migrate: schema operation not supported")

// ErrUnknownDriver is wrapped by the error a builder returns when it can't
// tell which driver its executor runs on.
var ErrUnknownDriver = errors.New("migrate: cannot tell which driver this executor runs on")

// Unsupported reports that driverName can't do feature. The error wraps
// [ErrSchemaUnsupported], so callers match it with errors.Is.
func Unsupported(driverName, feature string) error {
	return fmt.Errorf("%w: %s does not support %s", ErrSchemaUnsupported, driverName, feature)
}

// Renderer turns schema operations into work on one driver. A driver's migrate
// package registers one with [RegisterRenderer]. Apply runs the work through
// exec, the executor the migration was handed, so a wrapper around it still
// sees every statement.
type Renderer interface {
	Apply(ctx context.Context, exec Executor, op Operation) error
}

var (
	renderersMu sync.RWMutex
	renderers   = make(map[string]Renderer)
)

// RegisterRenderer registers the schema renderer for a driver name, the same
// name the driver's executor reports through [DriverNamer]. A driver's migrate
// package usually calls it from init, next to [RegisterExecutor].
//
// Subsequent calls with the same name overwrite the previous registration, and
// a nil renderer counts as none registered.
func RegisterRenderer(driverName string, r Renderer) {
	renderersMu.Lock()
	defer renderersMu.Unlock()
	renderers[driverName] = r
}

func rendererFor(driverName string) (Renderer, bool) {
	renderersMu.RLock()
	r, ok := renderers[driverName]
	renderersMu.RUnlock()
	return r, ok && r != nil
}

// DriverNamer is implemented by an executor that knows which driver it runs
// on, such as "pg" or "mongo". It picks the renderer for that driver's schema
// operations.
type DriverNamer interface {
	DriverName() string
}

// Unwrapper is implemented by an executor that wraps another one, such as a
// test double that counts statements. It lets [DriverNameOf] and [ExecutorAs]
// see through the wrapper to the executor inside.
type Unwrapper interface {
	Unwrap() Executor
}

// maxUnwrapDepth stops a wrapper whose Unwrap points back at itself from
// hanging the lookup. No real stack of wrappers comes close.
const maxUnwrapDepth = 32

// unwrapChain yields exec and then each executor it unwraps to, outermost
// first, and stops at the first one with no Unwrap or a nil one.
func unwrapChain(exec Executor) iter.Seq[Executor] {
	return func(yield func(Executor) bool) {
		for range maxUnwrapDepth {
			if exec == nil || !yield(exec) {
				return
			}
			u, ok := exec.(Unwrapper)
			if !ok {
				return
			}
			exec = u.Unwrap()
		}
	}
}

// resolveDriver follows Unwrap from exec until something names its driver.
// When nothing does, last is the executor the search ended on: the one to
// blame, since it neither names a driver nor unwraps further.
func resolveDriver(exec Executor) (name string, last Executor, ok bool) {
	for e := range unwrapChain(exec) {
		last = e
		if namer, isNamer := e.(DriverNamer); isNamer {
			return namer.DriverName(), e, true
		}
	}
	return "", last, false
}

// DriverNameOf returns the driver name of exec, following [Unwrapper] through
// any wrappers until an executor implements [DriverNamer]. The bool is false
// when none does.
func DriverNameOf(exec Executor) (string, bool) {
	name, _, ok := resolveDriver(exec)
	return name, ok
}

// ExecutorAs finds the first executor in exec's wrapper chain that is a T,
// following [Unwrapper]. A driver's own helpers use it to reach their concrete
// executor even when a test wrapper sits in front.
func ExecutorAs[T any](exec Executor) (T, bool) {
	for e := range unwrapChain(exec) {
		if t, ok := e.(T); ok {
			return t, true
		}
	}
	var zero T
	return zero, false
}
