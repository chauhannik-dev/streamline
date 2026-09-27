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

func streamHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	w.Header().Set("Content-Type", "text/event-stream")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	for {

		select {
		case <-ctx.Done():
			fmt.Println("client disconnected")
			return
		case <-time.After(2 * time.Second):
			w.Write([]byte("data: your message here\n\n"))
			flusher.Flush()
		}
	}
}

func startServer() error {
	fmt.Println("streamline server starting...")

	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/stream", streamHandler)

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
