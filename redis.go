package main

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

func NewRedisClient() *redis.Client {
	return redis.NewClient(&redis.Options{Addr: "localhost:6379"})
}

func publishEvent(ctx context.Context, client *redis.Client, topic string, event Event) error {
	_, err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: "events:" + topic,
		Values: map[string]interface{}{
			"type":    event.Type,
			"payload": event.Payload,
		},
	}).Result()

	return err
}

func startConsumer(ctx context.Context, client *redis.Client, broker *Broker, topic string) (err error) {
	lastId := "$"
	for {
		results, err := client.XRead(ctx, &redis.XReadArgs{
			Streams: []string{fmt.Sprintf("events:%s", topic), lastId},
			Block:   0,
		}).Result()

		if err != nil {
			return err
		}

		for _, stream := range results {
			for _, message := range stream.Messages {
				eventType := message.Values["type"].(string)
				payload := message.Values["payload"].(string)

				broker.Broadcast(fmt.Sprintf("{type: %s, payload: %s}", eventType, payload))

				lastId = message.ID
			}
		}
	}

	return nil
}
