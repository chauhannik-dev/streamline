package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
)

func healthHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("request received at health handler")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func streamHandler(broker *Broker, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	flusher.Flush()

	ch := make(chan string)
	broker.Subscribe(ch)

	for {

		select {
		case <-ctx.Done():
			fmt.Println("client disconnected")
			broker.Unsubscribe(ch)
			return
		case msg := <-ch:
			w.Write([]byte(fmt.Sprintf("data: %s\n\n", msg)))
			flusher.Flush()
		}
	}
}

func testBroadcastHandler(broker *Broker, w http.ResponseWriter) {
	broker.Broadcast("message sent at " + time.Now().String())

	fmt.Fprintf(w, "event sent")
}

func publishEventsHandler(broker *Broker, redis *redis.Client, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event Event
	err := json.NewDecoder(r.Body).Decode(&event)
	if err != nil {
		fmt.Print(err.Error())
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	err = event.validate()
	if err != nil {
		fmt.Print(err.Error())
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	// broker.Broadcast(fmt.Sprintf("{type: %s, payload: %s}", event.Type, event.Payload))

	ctx := r.Context()
	err = publishEvent(ctx, redis, event.Topic, event)
	if err != nil {
		fmt.Println("failed to publish event:", err.Error())
		http.Error(w, "failed to publish event", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func startServer(port string) error {
	fmt.Printf("streamline server starting on port %s...\n", port)

	broker := &Broker{
		subscribers: make(map[chan string]bool),
	}

	redisClient := NewRedisClient()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/stream", func(w http.ResponseWriter, r *http.Request) { streamHandler(broker, w, r) })
	mux.HandleFunc("/test-broadcast", func(w http.ResponseWriter, r *http.Request) { testBroadcastHandler(broker, w) })
	mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) { publishEventsHandler(broker, redisClient, w, r) })

	// Start the consumer
	go startConsumer(context.Background(), redisClient, broker, "deployments")

	if err := http.ListenAndServe(":"+port, mux); err != nil {
		return err
	}
	return nil
}

func main() {
	port := flag.String("port", "8080", "port to listen on")
	flag.Parse()

	err := startServer(*port)
	if err != nil {
		fmt.Println("server failed to start:", err.Error())
	}
}
