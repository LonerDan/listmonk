// Package edge is the client for the tracking edge's internal API. The main
// instance only dials out to the edge over the VPN; it never listens for it.
package edge

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Config is the tracking behaviour pushed to the edge.
type Config struct {
	DisableTracking    bool `json:"disable_tracking"`
	IndividualTracking bool `json:"individual_tracking"`
}

// Event is a campaign view or link click recorded by the edge.
type Event struct {
	Seq      int64     `json:"seq"`
	Type     string    `json:"type"`
	At       time.Time `json:"at"`
	CampUUID string    `json:"camp_uuid"`
	SubUUID  string    `json:"sub_uuid"`
	LinkUUID string    `json:"link_uuid,omitempty"`
}

// EventBatch is a page of events returned by the edge.
type EventBatch struct {
	EdgeID  string  `json:"edge_id"`
	LastSeq int64   `json:"last_seq"`
	Events  []Event `json:"events"`
}

// Health is the edge's status response.
type Health struct {
	EdgeID       string `json:"edge_id"`
	Links        int64  `json:"links"`
	QueuedEvents int64  `json:"queued_events"`
	LastSeq      int64  `json:"last_seq"`
}

// Client talks to a tracking edge's internal API.
type Client struct {
	baseURL string
	token   string
	hc      *http.Client
}

// New returns a client for the edge's internal API at baseURL.
func New(baseURL, token string, timeout time.Duration) *Client {
	return &Client{
		baseURL: strings.TrimSuffix(baseURL, "/"),
		token:   token,
		hc:      &http.Client{Timeout: timeout},
	}
}

// PutLink registers a tracked link (UUID to URL) on the edge.
func (c *Client) PutLink(uuid, url string) error {
	body := map[string]any{"links": []map[string]string{{"uuid": uuid, "url": url}}}
	return c.do(http.MethodPut, "/internal/v1/links", body, nil)
}

// PutConfig pushes the tracking behaviour to the edge.
func (c *Client) PutConfig(cfg Config) error {
	return c.do(http.MethodPut, "/internal/v1/config", cfg, nil)
}

// Health returns the edge's status, including its edge ID.
func (c *Client) Health() (*Health, error) {
	var out Health
	if err := c.do(http.MethodGet, "/internal/v1/health", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// PullEvents fetches up to limit events with a sequence number greater than after.
func (c *Client) PullEvents(after int64, limit int) (*EventBatch, error) {
	path := "/internal/v1/events?after=" + strconv.FormatInt(after, 10) + "&limit=" + strconv.Itoa(limit)

	var out EventBatch
	if err := c.do(http.MethodGet, path, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// AckEvents tells the edge that all events up to and including upTo are processed.
func (c *Client) AckEvents(upTo int64) error {
	return c.do(http.MethodPost, "/internal/v1/events/ack", map[string]int64{"up_to": upTo}, nil)
}

func (c *Client) do(method, path string, body, out any) error {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		r = bytes.NewReader(b)
	}

	req, err := http.NewRequest(method, c.baseURL+path, r)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("edge %s %s: status %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(msg)))
	}

	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}
