// Package sink defines destination-side ingestion contracts.
package sink

import (
	"context"

	"github.com/Kale105/streamforge/internal/event"
)

// Sink handles one immutable event synchronously.
type Sink interface {
	Handle(context.Context, event.Event) error
}
