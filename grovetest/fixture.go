package grovetest

import (
	"encoding/json"
	"fmt"
	"os"
)

// Fixture holds test fixture data.
type Fixture struct {
	Table   string           `json:"table"`
	Records []map[string]any `json:"records"`
}

// LoadFixtures reads fixtures from a JSON file.
// Expected format:
//
//	[{"table": "users", "records": [{"name": "Alice"}, ...]}]
func LoadFixtures(path string) ([]Fixture, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("grovetest: read fixture %s: %w", path, err)
	}

	var fixtures []Fixture
	if err := json.Unmarshal(data, &fixtures); err != nil {
		return nil, fmt.Errorf("grovetest: parse fixture %s: %w", path, err)
	}

	return fixtures, nil
}
