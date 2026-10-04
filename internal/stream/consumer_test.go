package stream

import (
	"testing"
	"time"
)

func TestParseMessage(t *testing.T) {
	values := map[string]any{
		"v":       "1",
		"nodeId":  "2",
		"ts":      "1760000000000",
		"records": "10:100;11:200;10:50;bad;12:-1;13:0",
	}

	eventAt, nodeID, records, err := parseMessage("1760000000123-0", values)
	if err != nil {
		t.Fatalf("parseMessage() error = %v", err)
	}
	if nodeID != 2 {
		t.Fatalf("nodeID = %d, want 2", nodeID)
	}
	want := time.UnixMilli(1760000000000).UTC()
	if !eventAt.Equal(want) {
		t.Fatalf("eventAt = %s, want %s", eventAt, want)
	}
	if records[10] != 150 || records[11] != 200 || len(records) != 2 {
		t.Fatalf("records = %#v, want map[10:150 11:200]", records)
	}
}

func TestParseMessageFallsBackToStreamID(t *testing.T) {
	values := map[string]any{
		"nodeId":  1,
		"records": "10:42",
	}
	got, _, _, err := parseMessage("1760000000123-7", values)
	if err != nil {
		t.Fatalf("parseMessage() error = %v", err)
	}
	want := time.UnixMilli(1760000000123).UTC()
	if !got.Equal(want) {
		t.Fatalf("eventAt = %s, want %s", got, want)
	}
}
