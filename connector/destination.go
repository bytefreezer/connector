package connector

import (
	"context"
	"fmt"
	"sync"
)

// Record represents a single exported record
type Record map[string]interface{}

// Batch is a collection of records to send to a destination
type Batch struct {
	Records   []Record
	Partition string // Source partition path
	File      string // Source parquet file
}

// Destination is the interface for export targets.
// Implement this interface to add new destinations (Splunk, Snowflake, etc.)
type Destination interface {
	// Name returns the destination name for logging
	Name() string
	// Init initializes the destination with its config
	Init(config map[string]interface{}) error
	// Send sends a batch of records to the destination
	Send(ctx context.Context, batch Batch) error
	// Close cleans up resources
	Close() error
}

// Registry holds available destination implementations
var (
	registryMu  sync.RWMutex
	destinations = map[string]func() Destination{}
)

// RegisterDestination registers a destination factory
func RegisterDestination(name string, factory func() Destination) {
	registryMu.Lock()
	defer registryMu.Unlock()
	destinations[name] = factory
}

// GetDestination creates a destination by name
func GetDestination(name string) (Destination, error) {
	registryMu.RLock()
	defer registryMu.RUnlock()

	factory, ok := destinations[name]
	if !ok {
		available := make([]string, 0, len(destinations))
		for k := range destinations {
			available = append(available, k)
		}
		return nil, fmt.Errorf("unknown destination %q, available: %v", name, available)
	}
	return factory(), nil
}
