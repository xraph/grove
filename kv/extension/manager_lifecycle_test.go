package extension

import (
	"errors"
	"sync"
	"testing"

	"github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/kvtest"
)

type replacementDriver struct {
	*kvtest.MockDriver
	closes   int
	closeErr error
}

func (d *replacementDriver) Close() error { d.closes++; return d.closeErr }

func TestStoreManagerReplacementAndAliases(t *testing.T) {
	first, second := &replacementDriver{MockDriver: kvtest.NewMockDriver()}, &replacementDriver{MockDriver: kvtest.NewMockDriver()}
	a, err := kv.Open(first)
	if err != nil {
		t.Fatal(err)
	}
	b, err := kv.Open(second)
	if err != nil {
		t.Fatal(err)
	}
	m := NewStoreManager()
	m.Add("primary", a)
	m.Add("alias", a)
	m.Add("primary", a)
	if first.closes != 0 {
		t.Fatal("same handle was closed")
	}
	m.Add("primary", b)
	if first.closes != 0 {
		t.Fatal("replacement closed a live alias")
	}
	m.Add("alias", b)
	if first.closes != 1 {
		t.Fatalf("replaced handle closed %d times", first.closes)
	}
	current, err := m.Default()
	if err != nil || current != b {
		t.Fatalf("default lost: %v", err)
	}
	if err = m.Close(); err != nil {
		t.Fatal(err)
	}
	if err = m.Close(); err != nil {
		t.Fatal(err)
	}
	if first.closes != 1 || second.closes != 1 {
		t.Fatalf("close counts: %d, %d", first.closes, second.closes)
	}
	if m.Len() != 0 {
		t.Fatal("closed manager retained handles")
	}
	incoming := &replacementDriver{MockDriver: kvtest.NewMockDriver()}
	c, err := kv.Open(incoming)
	if err != nil {
		t.Fatal(err)
	}
	m.Add("late", c)
	if incoming.closes != 1 || m.Len() != 0 {
		t.Fatal("Add after Close leaked incoming handle")
	}
}

func TestStoreManagerReportsReplacementErrors(t *testing.T) {
	failure := errors.New("close failed")
	old := &replacementDriver{MockDriver: kvtest.NewMockDriver(), closeErr: failure}
	a, err := kv.Open(old)
	if err != nil {
		t.Fatal(err)
	}
	m := NewStoreManager()
	m.Add("primary", a)
	m.Add("primary", openMockStore(t))
	if old.closes != 1 {
		t.Fatal("replacement did not close old handle")
	}
	if err = m.Close(); !errors.Is(err, failure) {
		t.Fatalf("replacement error lost: %v", err)
	}
	if err = m.Close(); !errors.Is(err, failure) {
		t.Fatalf("repeated Close lost error: %v", err)
	}
}

func TestStoreManagerConcurrentReplacementAndClose(t *testing.T) {
	m := NewStoreManager()
	drivers := make([]*replacementDriver, 20)
	var workers sync.WaitGroup
	for i := range drivers {
		drivers[i] = &replacementDriver{MockDriver: kvtest.NewMockDriver()}
		handle, err := kv.Open(drivers[i])
		if err != nil {
			t.Fatal(err)
		}
		workers.Add(1)
		go func() { defer workers.Done(); m.Add("primary", handle) }()
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	workers.Wait()
	for _, driver := range drivers {
		if driver.closes != 1 {
			t.Fatalf("handle closed %d times", driver.closes)
		}
	}
}
