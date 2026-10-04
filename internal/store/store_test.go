package store

import (
	"testing"

	"github.com/google/uuid"
)

func TestIsCurrentExhaustion(t *testing.T) {
	gen := int64(3)
	item := &OutboxItem{
		ID:                   uuid.New(),
		ExhaustionGeneration: &gen,
		Payload:              []byte(`{"total":100,"used":100}`),
	}

	u := User{TotalBytes: 100, UsedBytes: 100, LimitReached: true, ExhaustionGeneration: 3}
	current, err := (&Store{}).IsCurrentExhaustion(item, u)
	if err != nil || !current {
		t.Fatalf("expected current exhaustion, current=%v err=%v", current, err)
	}

	u.TotalBytes = 200
	current, err = (&Store{}).IsCurrentExhaustion(item, u)
	if err != nil || current {
		t.Fatalf("expected stale exhaustion after quota change, current=%v err=%v", current, err)
	}

	u.TotalBytes = 100
	u.ExhaustionGeneration = 4
	current, err = (&Store{}).IsCurrentExhaustion(item, u)
	if err != nil || current {
		t.Fatalf("expected stale exhaustion after generation change, current=%v err=%v", current, err)
	}
}

func TestIsCurrentExhaustionLegacy(t *testing.T) {
	item := &OutboxItem{
		ID:      uuid.New(),
		Payload: []byte(`{"total":100,"used":110}`),
	}

	u := User{TotalBytes: 100, UsedBytes: 120, LimitReached: true, ExhaustionGeneration: 0}
	current, err := (&Store{}).IsCurrentExhaustion(item, u)
	if err != nil || !current {
		t.Fatalf("expected legacy exhaustion to remain current, current=%v err=%v", current, err)
	}

	u.ExhaustionGeneration = 1
	current, err = (&Store{}).IsCurrentExhaustion(item, u)
	if err != nil || current {
		t.Fatalf("expected legacy event to be stale for non-zero generation, current=%v err=%v", current, err)
	}
}
