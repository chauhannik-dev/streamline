# Streamline

A distributed real-time event streaming system built with Go, Server-Sent Events (SSE), and Redis Streams.

## Overview

Streamline is a server-to-client event delivery system. Producers publish events to topics, and Streamline delivers them in real time to subscribed clients over persistent HTTP connections using SSE.

```
Event Producer
      │
      │ HTTP POST
      ▼
┌───────────┐
│  Go API   │
└─────┬─────┘
      │
      ▼
┌──────────────┐
│ Redis Streams│
└──────┬───────┘
       │
  ┌────┴─────┐
  ▼          ▼
SSE Server  SSE Server
  │          │
  ▼          ▼
Client A   Client B
```

## Features

- **Real-time event delivery** via Server-Sent Events
- **Topic-based subscriptions** — clients subscribe to specific event topics
- **Multi-instance support** — horizontal scaling with Redis Streams as shared event infrastructure
- **Event replay** — clients reconnect with `Last-Event-ID` to recover missed events
- **Backpressure handling** — bounded per-client buffers with slow-consumer disconnection
- **Heartbeat-based connection management** — keeps long-lived connections alive through proxies
- **Graceful shutdown** — clean connection teardown on server stop

## API

### Publish an event

```http
POST /events
Content-Type: application/json

{
  "topic": "deployments",
  "type": "DEPLOYMENT_STARTED",
  "payload": {
    "service": "payments",
    "version": "v1.8.2"
  }
}
```

### Subscribe to a topic

```http
GET /stream?topic=deployments
```

Returns a persistent `text/event-stream` connection:

```
id: 182
event: DEPLOYMENT_STARTED
data: {"service":"payments","version":"v1.8.2"}

```

### Health check

```http
GET /health
```

## Tech Stack

- **Go** — HTTP server, concurrency (goroutines, channels), connection management
- **Server-Sent Events** — real-time server-to-client transport
- **Redis Streams** — distributed event storage, ordering, and replay
- **Docker / Docker Compose** — multi-instance local deployment

## Running

```bash
go run main.go
```

The server starts on port `8080`.

```bash
# Health check
curl http://localhost:8080/health

# Subscribe to events
curl -N http://localhost:8080/stream?topic=deployments
```
