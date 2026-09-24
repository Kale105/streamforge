package coordination

import (
	"errors"
	"testing"
	"time"
)

func TestValidation(t *testing.T) {
	valid := Assignment{Dataset: "games", Source: "nba", Partition: "east"}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	if !errors.Is((Assignment{}).Validate(), ErrInvalidAssignment) {
		t.Fatal("empty assignment accepted")
	}
	if err := ValidateDuration(time.Second); err != nil {
		t.Fatal(err)
	}
	if err := ValidateDuration(time.Millisecond); err == nil {
		t.Fatal("short duration accepted")
	}
	if err := (Progress{Checkpoint: Checkpoint{Sequence: 1, Cursor: []byte("1")}}).Validate(); err != nil {
		t.Fatal(err)
	}
	if !errors.Is((Progress{}).Validate(), ErrInvalidCheckpoint) {
		t.Fatal("empty progress accepted")
	}
}
