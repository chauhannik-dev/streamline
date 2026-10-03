package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestStreamlineRedisFlow(t *testing.T) {
	redisClient := NewRedisClient()
	defer redisClient.Close()

	// Ping Redis to make sure it's up
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		t.Fatalf("Redis is not available: %v", err)
	}

	broker := &Broker{
		subscribers: make(map[chan string]bool),
	}

	// Start consumer goroutine for 'deployments'
	consumerCtx, consumerCancel := context.WithCancel(context.Background())
	defer consumerCancel()
	go startConsumer(consumerCtx, redisClient, broker, "deployments")

	// Set up HTTP test server
	mux := http.NewServeMux()
	mux.HandleFunc("/stream", func(w http.ResponseWriter, r *http.Request) {
		streamHandler(broker, w, r)
	})
	mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		publishEventsHandler(broker, redisClient, w, r)
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	// 1. Subscribe to SSE endpoint
	req, err := http.NewRequest("GET", server.URL+"/stream", nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("failed to connect to SSE stream: %v", err)
	}
	defer resp.Body.Close()

	if resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("expected text/event-stream, got %s", resp.Header.Get("Content-Type"))
	}

	// Read SSE messages in background channel
	msgChan := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "data: ") {
				msgChan <- strings.TrimPrefix(line, "data: ")
				return
			}
		}
	}()

	// Allow consumer to start XREAD loop
	time.Sleep(100 * time.Millisecond)

	// 2. POST an event to /events
	eventData := Event{
		Topic:   "deployments",
		Type:    "DEPLOY_SUCCESS",
		Payload: "v1.2.3 deployed successfully",
	}
	body, _ := json.Marshal(eventData)

	postResp, err := http.Post(server.URL+"/events", "application/json", bytes.NewBuffer(body))
	if err != nil {
		t.Fatalf("failed to post event: %v", err)
	}
	defer postResp.Body.Close()

	if postResp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", postResp.StatusCode)
	}

	// 3. Wait for event to arrive over SSE stream
	select {
	case msg := <-msgChan:
		expected := fmt.Sprintf("{type: %s, payload: %s}", eventData.Type, eventData.Payload)
		if msg != expected {
			t.Errorf("expected msg %q, got %q", expected, msg)
		} else {
			t.Logf("Success! Received event over SSE: %s", msg)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for event on SSE stream")
	}
}
