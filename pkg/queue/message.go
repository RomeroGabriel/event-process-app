package queue

type MessageQueue struct {
	EventType string `json:"event_type"`
	ClientId  string `json:"client_id"`
	Message   string `json:"message"`
	MessageId string
}
