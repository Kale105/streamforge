package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode"

	"github.com/Kale105/streamforge/internal/recordstore"
)

// Query executes the bounded public query contract. Every client value,
// including JSON keys, is a PostgreSQL parameter; the only interpolated SQL is
// one of four constant reviewed ordering fragments.
func (s *Store) Query(ctx context.Context, request recordstore.Query, limit int) (recordstore.Page, error) {
	// Reject caller-controlled page sizes before acquiring a pool connection or
	// allocating the result slice.
	if limit <= 0 {
		return recordstore.Page{}, errors.New("query limit must be positive")
	}
	if limit > recordstore.MaxPageItems {
		return recordstore.Page{}, fmt.Errorf("query limit cannot exceed hard page cap of %d", recordstore.MaxPageItems)
	}
	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	if err := s.valid(ctx); err != nil {
		return recordstore.Page{}, err
	}
	if request.Dataset == "" {
		return recordstore.Page{}, ErrEmptyDataset
	}
	if request.DatasetVersion == "" {
		return recordstore.Page{}, errors.New("dataset version is required")
	}
	if len(request.Filters) > recordstore.MaxFilters {
		return recordstore.Page{}, fmt.Errorf("query filters cannot exceed %d", recordstore.MaxFilters)
	}
	if request.Sort == "" {
		request.Sort = recordstore.SortUpdatedAt
	}
	if request.Direction == "" {
		request.Direction = recordstore.SortAscending
	}
	if request.Sort != recordstore.SortUpdatedAt && request.Sort != recordstore.SortID {
		return recordstore.Page{}, errors.New("query sort is invalid")
	}
	if request.Direction != recordstore.SortAscending && request.Direction != recordstore.SortDescending {
		return recordstore.Page{}, errors.New("query direction is invalid")
	}
	allowed := make(map[string]struct{}, len(request.AllowedFields))
	for _, field := range request.AllowedFields {
		if !validJSONKey(field) {
			return recordstore.Page{}, errors.New("query allow-list contains invalid JSON key")
		}
		allowed[field] = struct{}{}
	}
	seen := make(map[string]struct{}, len(request.Filters))
	args := []any{request.Dataset, request.DatasetVersion}
	where := []string{"dataset = $1", "dataset_version = $2"}
	for _, filter := range request.Filters {
		if !validJSONKey(filter.Field) {
			return recordstore.Page{}, errors.New("query filter field is invalid")
		}
		if _, ok := allowed[filter.Field]; !ok {
			return recordstore.Page{}, fmt.Errorf("query filter %q is not allow-listed", filter.Field)
		}
		if _, duplicate := seen[filter.Field]; duplicate {
			return recordstore.Page{}, fmt.Errorf("query filter %q is duplicated", filter.Field)
		}
		seen[filter.Field] = struct{}{}
		if len(filter.Value) == 0 || len(filter.Value) > recordstore.MaxFilterValueBytes || !jsonScalar(filter.Value) {
			return recordstore.Page{}, fmt.Errorf("query filter %q must be a bounded JSON scalar", filter.Field)
		}
		// JSONB containment is semantically exact for one top-level scalar and
		// lets PostgreSQL use the normalized_records_data_gin index. Marshalling
		// the object keeps both the key and raw scalar parameterized.
		contained, err := filterContainment(filter.Field, filter.Value)
		if err != nil {
			return recordstore.Page{}, fmt.Errorf("encode query filter %q: %w", filter.Field, err)
		}
		args = append(args, contained)
		where = append(where, fmt.Sprintf("data @> $%d::jsonb", len(args)))
	}
	order, cursorClause, err := queryOrder(request, len(args)+1)
	if err != nil {
		return recordstore.Page{}, err
	}
	if cursorClause != "" {
		where = append(where, cursorClause)
		if request.Sort == recordstore.SortUpdatedAt {
			args = append(args, request.After.UpdatedAt, request.After.ID)
		} else {
			args = append(args, request.After.ID)
		}
	}
	args = append(args, limit+1)
	sql := `SELECT dataset, dataset_version, id, data, updated_at FROM normalized_records WHERE ` + strings.Join(where, " AND ") + ` ORDER BY ` + order + fmt.Sprintf(" LIMIT $%d", len(args))
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return recordstore.Page{}, fmt.Errorf("query records: %w", err)
	}
	defer rows.Close()
	page := recordstore.Page{Records: make([]recordstore.Record, 0, limit)}
	for rows.Next() {
		var record recordstore.Record
		if err := rows.Scan(&record.Dataset, &record.DatasetVersion, &record.ID, &record.Data, &record.UpdatedAt); err != nil {
			return recordstore.Page{}, fmt.Errorf("scan query record: %w", err)
		}
		if len(page.Records) == limit {
			last := page.Records[len(page.Records)-1]
			page.Next = &recordstore.Cursor{UpdatedAt: last.UpdatedAt, ID: last.ID}
			break
		}
		page.Records = append(page.Records, record)
	}
	if err := rows.Err(); err != nil {
		return recordstore.Page{}, fmt.Errorf("iterate query records: %w", err)
	}
	return page, nil
}

func filterContainment(field string, value json.RawMessage) (string, error) {
	contained, err := json.Marshal(map[string]json.RawMessage{field: value})
	if err != nil {
		return "", err
	}
	return string(contained), nil
}

func queryOrder(request recordstore.Query, cursorPosition int) (order, cursor string, err error) {
	if request.After == nil {
		switch request.Sort {
		case recordstore.SortUpdatedAt:
			if request.Direction == recordstore.SortDescending {
				return "updated_at DESC, id DESC", "", nil
			}
			return "updated_at ASC, id ASC", "", nil
		case recordstore.SortID:
			if request.Direction == recordstore.SortDescending {
				return "id DESC", "", nil
			}
			return "id ASC", "", nil
		}
	}
	if request.After.ID == "" || (request.Sort == recordstore.SortUpdatedAt && request.After.UpdatedAt.IsZero()) {
		return "", "", errors.New("query cursor is incomplete")
	}
	descending := request.Direction == recordstore.SortDescending
	comparison := ">"
	if descending {
		comparison = "<"
	}
	switch request.Sort {
	case recordstore.SortUpdatedAt:
		order = "updated_at ASC, id ASC"
		if descending {
			order = "updated_at DESC, id DESC"
		}
		return order, fmt.Sprintf("(updated_at, id) %s ($%d, $%d)", comparison, cursorPosition, cursorPosition+1), nil
	case recordstore.SortID:
		order = "id ASC"
		if descending {
			order = "id DESC"
		}
		return order, fmt.Sprintf("id %s $%d", comparison, cursorPosition), nil
	default:
		return "", "", errors.New("query sort is invalid")
	}
}

func validJSONKey(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for i, r := range value {
		if !(r == '_' || unicode.IsLetter(r) || (i > 0 && unicode.IsDigit(r))) {
			return false
		}
	}
	return true
}

func jsonScalar(value json.RawMessage) bool {
	var decoded any
	decoder := json.NewDecoder(strings.NewReader(string(value)))
	decoder.UseNumber()
	if decoder.Decode(&decoded) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return false
	}
	switch decoded.(type) {
	case nil, bool, string, json.Number:
		return true
	default:
		return false
	}
}
