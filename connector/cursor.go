package connector

import (
	"encoding/json"
	"os"
	"sync"
	"time"

	"github.com/bytefreezer/goodies/log"
)

// Cursor tracks export progress to enable at-least-once delivery
type Cursor struct {
	mu       sync.Mutex
	filePath string
	state    CursorState
}

// CursorState is persisted to disk
type CursorState struct {
	LastPartition string    `json:"last_partition"` // e.g. "year=2026/month=03/day=05/hour=14"
	LastFile      string    `json:"last_file"`      // Last fully exported parquet file path
	LastOffset    int64     `json:"last_offset"`    // Row offset within last file (for resume)
	TotalExported int64     `json:"total_exported"` // Cumulative records exported
	UpdatedAt     time.Time `json:"updated_at"`
}

// NewCursor creates or loads a cursor from the given file path
func NewCursor(filePath string) *Cursor {
	c := &Cursor{filePath: filePath}
	c.load()
	return c
}

// Get returns the current cursor state
func (c *Cursor) Get() CursorState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

// Update sets the cursor position and persists to disk
func (c *Cursor) Update(partition, file string, offset, totalExported int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.state.LastPartition = partition
	c.state.LastFile = file
	c.state.LastOffset = offset
	c.state.TotalExported = totalExported
	c.state.UpdatedAt = time.Now()

	return c.save()
}

// Reset clears the cursor (re-export from beginning)
func (c *Cursor) Reset() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.state = CursorState{}
	return c.save()
}

func (c *Cursor) load() {
	data, err := os.ReadFile(c.filePath)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Warnf("Failed to read cursor file %s: %v", c.filePath, err)
		}
		return
	}

	if err := json.Unmarshal(data, &c.state); err != nil {
		log.Warnf("Failed to parse cursor file %s: %v", c.filePath, err)
	}
}

func (c *Cursor) save() error {
	data, err := json.MarshalIndent(c.state, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(c.filePath, data, 0644)
}
