// Package collector defines source-side ingestion contracts.
package collector

import (
	"context"

	"github.com/Kale105/streamforge/internal/event"
)

// EmitFunc synchronously hands an immutable event to the collector's owner.
// Returning an error stops collection.
type EmitFunc func(context.Context, event.Event) error

// Collector produces events until completion, cancellation, or failure.
// Collect blocks; the runtime owns any goroutine used to call it.
type Collector interface {
	Collect(context.Context, EmitFunc) error
}
