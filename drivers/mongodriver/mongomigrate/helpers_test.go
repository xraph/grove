package mongomigrate

import (
	"testing"
)

func TestDefaultCollectionOpts(t *testing.T) {
	opts := defaultCollectionOpts()

	if !opts.ifNotExists {
		t.Error("expected ifNotExists to default to true")
	}
	if opts.validationLevel != "strict" {
		t.Errorf("expected validationLevel=strict, got %q", opts.validationLevel)
	}
	if opts.validationAction != "error" {
		t.Errorf("expected validationAction=error, got %q", opts.validationAction)
	}
	if opts.additionalProps != nil {
		t.Error("expected additionalProps to default to nil")
	}
	if opts.collection != "" {
		t.Errorf("expected empty collection, got %q", opts.collection)
	}
}

func TestCollectionOptions(t *testing.T) {
	opts := defaultCollectionOpts()

	WithIfNotExists(false)(&opts)
	if opts.ifNotExists {
		t.Error("expected ifNotExists=false after WithIfNotExists(false)")
	}

	WithValidationLevel("moderate")(&opts)
	if opts.validationLevel != "moderate" {
		t.Errorf("expected validationLevel=moderate, got %q", opts.validationLevel)
	}

	WithValidationAction("warn")(&opts)
	if opts.validationAction != "warn" {
		t.Errorf("expected validationAction=warn, got %q", opts.validationAction)
	}

	WithAdditionalProperties(false)(&opts)
	if opts.additionalProps == nil || *opts.additionalProps {
		t.Error("expected additionalProps=false")
	}

	WithCollection("custom_col")(&opts)
	if opts.collection != "custom_col" {
		t.Errorf("expected collection=custom_col, got %q", opts.collection)
	}
}
