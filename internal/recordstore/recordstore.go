// Package recordstore defines storage-neutral normalized record contracts.
package recordstore

import (
	"context"
	"encoding/json"
	"time"
)

const (
	MaxDatasetBytes        = 256
	MaxDatasetVersionBytes = 128
	MaxRecordIDBytes       = 1024
	MaxRecordDataBytes     = 16 << 20
	MaxFilters             = 8
	MaxFilterValueBytes    = 4096
	// MaxPageItems is an absolute public-query ceiling. Dataset configuration
	// may select a smaller value but can never raise this process-wide bound.
	MaxPageItems = 500
	// MaxResponseBytes bounds the encoded JSON response body, including record
	// data and cursor metadata. It prevents a legal page of large records from
	// becoming an unbounded allocation or network write.
	MaxResponseBytes = 4 << 20
)

type Record struct {
	Dataset        string          `json:"dataset"`
	DatasetVersion string          `json:"dataset_version"`
	ID             string          `json:"id"`
	Data           json.RawMessage `json:"data"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

// Cursor is the exclusive lower bound for stable keyset pagination.
type Cursor struct {
	UpdatedAt         time.Time
	ID                string
	DatasetVersion    string
	Sort              string
	Direction         string
	FilterFingerprint string
}

type Page struct {
	Records []Record
	Next    *Cursor
}

type SortField string

const (
	SortUpdatedAt SortField = "updated_at"
	SortID        SortField = "id"
)

type SortDirection string

const (
	SortAscending  SortDirection = "asc"
	SortDescending SortDirection = "desc"
)

// Filter is one exact top-level JSON scalar equality. Field names are not SQL
// expressions; query adapters must match them against a dataset allow-list.
type Filter struct {
	Field string
	Value json.RawMessage
}

// Query makes all user-selectable query shape explicit. Cursor metadata binds
// it to this exact revision, sort, direction, and normalized filter set.
type Query struct {
	Dataset           string
	DatasetVersion    string
	AllowedFields     []string
	Filters           []Filter
	Sort              SortField
	Direction         SortDirection
	FilterFingerprint string
	After             *Cursor
}

type Reader interface {
	// List returns records for exactly one dataset revision.
	List(context.Context, string, string, *Cursor, int) (Page, error)
}

// QueryReader is the optional advanced reader contract. Reader remains for
// personal-mode compatibility; API handlers use this interface when present.
type QueryReader interface {
	Query(context.Context, Query, int) (Page, error)
}
