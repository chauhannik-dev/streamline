package main

import (
	"errors"
	"strings"
)

type Event struct {
	Topic   string `json:"topic"`
	Type    string `json:"type"`
	Payload string `json:"payload"`
}

func (e *Event) validate() error {
	if len(strings.TrimSpace(e.Topic)) == 0 || len(strings.TrimSpace(e.Type)) == 0 {
		return errors.New("Topic or Type cannot be empty")
	}
	return nil
}
