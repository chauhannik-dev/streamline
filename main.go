package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

func healthHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("request received at health handler")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func streamHandler(broker *Broker, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	w.Header().Set("Content-Type", "text/event-stream")

	ch := make(chan string)
	broker.Subscribe(ch)

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

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

func publishEventsHandler(broker *Broker, w http.ResponseWriter, r *http.Request) {
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

	broker.Broadcast(fmt.Sprintf("{type: %s, payload: %s}", event.Type, event.Payload))

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func startServer() error {
	fmt.Println("streamline server starting...")

	broker := &Broker{
		subscribers: make(map[chan string]bool),
	}

	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/stream", func(w http.ResponseWriter, r *http.Request) { streamHandler(broker, w, r) })
	http.HandleFunc("/test-broadcast", func(w http.ResponseWriter, r *http.Request) { testBroadcastHandler(broker, w) })
	http.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) { publishEventsHandler(broker, w, r) })

	if err := http.ListenAndServe(":8080", nil); err != nil {
		return err
	}
	return nil
}

func main() {
	err := startServer()
	if err != nil {
		fmt.Println("server failed to start", err.Error())
	}
}
