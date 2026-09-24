package main

import (
	"reflect"
	"testing"

	"github.com/Kale105/streamforge/internal/dataset"
	"github.com/Kale105/streamforge/internal/recordstore"
)

func TestPublicDatasetPolicyCopiesConfiguredQueryAllowlists(t *testing.T) {
	spec := dataset.Dataset{
		Metadata: dataset.Metadata{Name: "games", Version: "v2"},
		API: dataset.API{
			Path: "/v1/games", Fields: []string{"id", "status"},
			Filters: []string{"status"}, Sorts: []string{"updated_at", "id"},
		},
	}

	policy := publicDatasetPolicy(spec)
	if !reflect.DeepEqual(policy.Filters, []string{"status"}) {
		t.Fatalf("policy filters = %#v", policy.Filters)
	}
	if !reflect.DeepEqual(policy.Sorts, []recordstore.SortField{recordstore.SortUpdatedAt, recordstore.SortID}) {
		t.Fatalf("policy sorts = %#v", policy.Sorts)
	}
	// The policy must not alias the decoded configuration. A later in-memory
	// mutation cannot enlarge an already running endpoint's allow-list.
	spec.API.Filters[0] = "id"
	spec.API.Sorts[0] = "id"
	if policy.Filters[0] != "status" || policy.Sorts[0] != recordstore.SortUpdatedAt {
		t.Fatalf("policy aliases dataset configuration: %#v", policy)
	}
}
