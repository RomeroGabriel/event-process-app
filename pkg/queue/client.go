package queue

type QueueMessage struct {
	MessageId   string
	MessageData any
}

type QueueClient interface {
	ReceiveMessage() (<-chan QueueMessage, error)
	DeleteMessage(QueueMessage) error
}
