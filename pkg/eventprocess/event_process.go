package eventprocess

import (
	"errors"
	"fmt"
)

type EventMessage struct {
	EventType string `json:"event_type"`
	ClientId  string `json:"client_id"`
	Message   string `json:"message"`
	MessageId string
}

func (e *EventMessage) ValidateEmptyFields() error {
	emptyFields := make([]string, 0)
	if e.ClientId == "" {
		emptyFields = append(emptyFields, "ClientId")
	}
	if e.EventType == "" {
		emptyFields = append(emptyFields, "EventType")
	}
	if e.Message == "" {
		emptyFields = append(emptyFields, "Message")
	}
	if len(emptyFields) > 0 {
		msg := fmt.Sprintln("empty fields in event message: ", emptyFields)
		return errors.New(msg)
	}
	return nil
}
