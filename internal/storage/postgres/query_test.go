package postgres

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Kale105/streamforge/internal/recordstore"
)

func TestQueryOrderSupportsStableAscendingAndDescendingKeysets(t *testing.T) {
	stamp := time.Date(2026, 9, 17, 0, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		query                 recordstore.Query
		wantOrder, wantCursor string
	}{
		{recordstore.Query{Sort: recordstore.SortUpdatedAt, Direction: recordstore.SortAscending, After: &recordstore.Cursor{UpdatedAt: stamp, ID: "b"}}, "updated_at ASC, id ASC", "(updated_at, id) > ($3, $4)"},
		{recordstore.Query{Sort: recordstore.SortUpdatedAt, Direction: recordstore.SortDescending, After: &recordstore.Cursor{UpdatedAt: stamp, ID: "b"}}, "updated_at DESC, id DESC", "(updated_at, id) < ($3, $4)"},
		{recordstore.Query{Sort: recordstore.SortID, Direction: recordstore.SortAscending, After: &recordstore.Cursor{ID: "b"}}, "id ASC", "id > $3"},
		{recordstore.Query{Sort: recordstore.SortID, Direction: recordstore.SortDescending, After: &recordstore.Cursor{ID: "b"}}, "id DESC", "id < $3"},
	} {
		order, cursor, err := queryOrder(test.query, 3)
		if err != nil || order != test.wantOrder || cursor != test.wantCursor {
			t.Fatalf("queryOrder(%#v) = %q, %q, %v", test.query, order, cursor, err)
		}
	}
}

func TestQueryInputGuards(t *testing.T) {
	if !validJSONKey("score_2") || validJSONKey("score->'x'") {
		t.Fatal("JSON key validation is unsafe")
	}
	for _, value := range []json.RawMessage{json.RawMessage(`1`), json.RawMessage(`"x"`), json.RawMessage(`true`), json.RawMessage(`null`)} {
		if !jsonScalar(value) {
			t.Fatalf("scalar %s rejected", value)
		}
	}
	for _, value := range []json.RawMessage{json.RawMessage(`{}`), json.RawMessage(`[]`), json.RawMessage(`1 trailing`)} {
		if jsonScalar(value) {
			t.Fatalf("non-scalar %s accepted", value)
		}
	}
}

func TestFilterContainmentBuildsParameterizedJSONBObject(t *testing.T) {
	contained, err := filterContainment("status", json.RawMessage(`"live"`))
	if err != nil {
		t.Fatal(err)
	}
	if contained != `{"status":"live"}` {
		t.Fatalf("containment = %s", contained)
	}
}

func TestQueryRejectsHardPageCapBeforePoolAccess(t *testing.T) {
	store := &Store{}
	_, err := store.Query(t.Context(), recordstore.Query{}, recordstore.MaxPageItems+1)
	if err == nil {
		t.Fatal("Query accepted page size above hard cap")
	}
}
