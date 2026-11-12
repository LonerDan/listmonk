package edge

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestClientPullEventsAndAck(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		switch r.URL.Path {
		case "/internal/v1/events":
			if r.URL.Query().Get("after") != "7" || r.URL.Query().Get("limit") != "500" {
				t.Errorf("unexpected query: %s", r.URL.RawQuery)
			}
			json.NewEncoder(w).Encode(map[string]any{
				"edge_id": "edge-1", "last_seq": 8,
				"events": []map[string]any{{"seq": 8, "type": "click", "at": "2026-11-12T12:00:00Z",
					"camp_uuid": "c", "sub_uuid": "s", "link_uuid": "l"}},
			})

		case "/internal/v1/events/ack":
			var body map[string]int64
			json.NewDecoder(r.Body).Decode(&body)
			if body["up_to"] != 8 {
				t.Errorf("unexpected ack body: %v", body)
			}
			w.WriteHeader(http.StatusNoContent)

		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := New(srv.URL+"/", "secret", 5*time.Second)

	batch, err := c.PullEvents(7, 500)
	if err != nil {
		t.Fatalf("PullEvents: %v", err)
	}
	if batch.EdgeID != "edge-1" || len(batch.Events) != 1 || batch.Events[0].LinkUUID != "l" {
		t.Fatalf("unexpected batch: %+v", batch)
	}

	if err := c.AckEvents(8); err != nil {
		t.Fatalf("AckEvents: %v", err)
	}
}

func TestClientReturnsErrorOnBadStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusUnauthorized)
	}))
	defer srv.Close()

	c := New(srv.URL, "wrong", 5*time.Second)
	if err := c.PutLink("u", "https://example.com"); err == nil {
		t.Fatal("expected error on 401 response")
	}
}
