// Package replay defines storage-neutral dead-letter inspection and explicit
// replay contracts. A failed materialization remains a durable DLQ entry until
// a caller requests a replay with an idempotency key. Requesting replay returns
// the retained raw event, but never processes it: the caller must deliberately
// submit that event to a materializer. The idempotency key makes repeated admin
// requests safe while a single replay is outstanding.
package replay

import (
	"context"
	"time"

	"github.com/Kale105/streamforge/internal/event"
)

// FailedMaterialization is a durable DLQ view. Its cursor is ordered by
// UpdatedAt, Source, EventID, Dataset, and DatasetVersion.
type FailedMaterialization struct {
	Source         string
	EventID        string
	Dataset        string
	DatasetVersion string
	Attempts       int
	Diagnostic     string
	FailedAt       time.Time
	UpdatedAt      time.Time
}

// Cursor is an exclusive keyset cursor for failed materializations.
type Cursor struct {
	UpdatedAt      time.Time
	Source         string
	EventID        string
	Dataset        string
	DatasetVersion string
}

// ListRequest bounds a DLQ query. Limit must be positive.
type ListRequest struct {
	After *Cursor
	Limit int
}

// Page is a bounded DLQ page. Next is nil when there are no more rows.
type Page struct {
	Items []FailedMaterialization
	Next  *Cursor
}

// Request names one failed materialization and supplies an idempotency key.
// RequestID is an administrative request identifier, not the source event ID.
type Request struct {
	Source         string
	EventID        string
	Dataset        string
	DatasetVersion string
	RequestID      string
	Actor          string
	Reason         string
}

// Result contains the immutable raw event which the caller explicitly chose
// to replay. Accepted is false only for an idempotent duplicate request.
type Result struct {
	Event    event.Event
	Accepted bool
}

// Store is implemented by durable stores that offer DLQ inspection and replay.
type Store interface {
	ListFailedMaterializations(context.Context, ListRequest) (Page, error)
	RequestMaterializationReplay(context.Context, Request) (Result, error)
}
