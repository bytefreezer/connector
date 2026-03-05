package destinations

import (
	"context"
	"fmt"
	"os"

	"github.com/bytedance/sonic"
	"github.com/bytefreezer/connector/connector"
)

func init() {
	connector.RegisterDestination("stdout", func() connector.Destination {
		return &StdoutDestination{}
	})
}

// StdoutDestination writes records as JSON lines to stdout
type StdoutDestination struct{}

func (d *StdoutDestination) Name() string { return "stdout" }

func (d *StdoutDestination) Init(_ map[string]interface{}) error { return nil }

func (d *StdoutDestination) Send(_ context.Context, batch connector.Batch) error {
	for _, record := range batch.Records {
		data, err := sonic.Marshal(record)
		if err != nil {
			return fmt.Errorf("failed to marshal record: %w", err)
		}
		fmt.Fprintln(os.Stdout, string(data))
	}
	return nil
}

func (d *StdoutDestination) Close() error { return nil }
