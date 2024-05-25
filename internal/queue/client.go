package queue

import (
	"context"

	pkgQueue "github.com/RomeroGabriel/event-process-app/pkg/queue"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type SQSQueueClient struct {
	sqsClient sqs.Client
	queueUrl  string
}

func NewSQSQueueClient(sqsClient sqs.Client, queueUrl string) *SQSQueueClient {
	return &SQSQueueClient{
		sqsClient: sqsClient,
		queueUrl:  queueUrl,
	}
}

func (s SQSQueueClient) ReceiveMessage() (<-chan pkgQueue.QueueMessage, error) {
	receiveParams := &sqs.ReceiveMessageInput{
		MaxNumberOfMessages: *aws.Int32(10),
		QueueUrl:            aws.String(s.queueUrl),
		WaitTimeSeconds:     *aws.Int32(1),
	}
	msgs, err := s.sqsClient.ReceiveMessage(context.Background(), receiveParams)
	if err != nil {
		return nil, err
	}
	out := make(chan pkgQueue.QueueMessage)
	var msgQ pkgQueue.QueueMessage
	go func() {
		for _, msg := range msgs.Messages {
			msgQ = pkgQueue.QueueMessage{MessageId: *msg.MessageId, MessageData: msg}
			out <- msgQ
		}
		close(out)
	}()
	return out, nil
}

func (s SQSQueueClient) DeleteMessage(pkgQueue.QueueMessage) error {
	return nil
}
