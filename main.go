package main

import (
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

func testBroadcastHandler(broker *Broker, w http.ResponseWriter, r *http.Request) {
	broker.Broadcast("message sent at " + time.Now().String())

	fmt.Fprintf(w, "event sent")
}

func startServer() error {
	fmt.Println("streamline server starting...")

	broker := &Broker{
		subscribers: make(map[chan string]bool),
	}

	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/stream", func(w http.ResponseWriter, r *http.Request) { streamHandler(broker, w, r) })
	http.HandleFunc("/test-broadcast", func(w http.ResponseWriter, r *http.Request) { testBroadcastHandler(broker, w, r) })

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
