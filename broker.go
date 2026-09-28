package main

import "sync"

type Broker struct {
	subscribers map[chan string]bool
	mu          sync.RWMutex
}

func (b *Broker) Subscribe(ch chan string) {
	b.mu.Lock()

	b.subscribers[ch] = true

	b.mu.Unlock()
}

func (b *Broker) Unsubscribe(ch chan string) {
	b.mu.Lock()

	delete(b.subscribers, ch)
	close(ch)

	b.mu.Unlock()
}

func (b *Broker) Broadcast(msg string) {
	b.mu.RLock()

	for channel, _ := range b.subscribers {
		channel <- msg
	}

	b.mu.RUnlock()
}
