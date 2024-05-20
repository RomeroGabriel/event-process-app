package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/RomeroGabriel/event-process-app/pkg/queue"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/lib/pq"
)

type ProcessorContainer struct {
	sqsClient   *sqs.Client
	queueUrl    string
	processorDb *ProcessorRepository
}

func NewProcessorContainer(sqsClient *sqs.Client, queueUrl string, processorDb *ProcessorRepository) *ProcessorContainer {
	return &ProcessorContainer{
		sqsClient:   sqsClient,
		queueUrl:    queueUrl,
		processorDb: processorDb,
	}
}

func StartProcessor(container ProcessorContainer) {
	log.Println("Starting the PROCESSOR EVENTS----->")

	receiveParams := &sqs.ReceiveMessageInput{
		MaxNumberOfMessages: *aws.Int32(1),
		QueueUrl:            aws.String(container.queueUrl),
		WaitTimeSeconds:     *aws.Int32(3),
	}

	log.Println("Listening queue messages")
	for {
		result, err := container.sqsClient.ReceiveMessage(context.Background(), receiveParams)
		if err != nil {
			log.Fatalf("Error receiving a message: %s", err)
		}
		log.Printf("Received %d messages.", len(result.Messages))

		for _, msg := range result.Messages {
			go ValidateMessage(container, msg)
		}
	}
}

func ValidateMessage(container ProcessorContainer, msg types.Message) {
	fmt.Println("Starting the Validate Phase: ", string(*msg.Body))
	deleteMsgFunc := func(sqsClient sqs.Client, queueUrl, ReceiptHandle string) {
		_, err := sqsClient.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{
			QueueUrl:      aws.String(queueUrl),
			ReceiptHandle: aws.String(ReceiptHandle),
		})
		if err != nil {
			log.Println("Error deleting message: ", err)
		}
	}

	var msgQueue queue.MessageQueue
	err := json.Unmarshal([]byte(*msg.Body), &msgQueue)
	if err != nil {
		log.Printf("Error unmarshalling message. Err: %s.\nDeleting the message on the queue.", err)
		deleteMsgFunc(*container.sqsClient, container.queueUrl, *msg.ReceiptHandle)
		return
	}
	msgQueue.MessageId = *msg.MessageId

	if msgQueue.ClientId == "" {
		log.Println("Empty ClientId. Deleting the message on the queue.")
		deleteMsgFunc(*container.sqsClient, container.queueUrl, *msg.ReceiptHandle)
		return
	}
	if msgQueue.EventType == "" {
		log.Println("Empty EventType. Deleting the message on the queue.")
		deleteMsgFunc(*container.sqsClient, container.queueUrl, *msg.ReceiptHandle)
		return
	}
	if msgQueue.Message == "" {
		log.Println("Empty Message. Deleting the message on the queue.")
		deleteMsgFunc(*container.sqsClient, container.queueUrl, *msg.ReceiptHandle)
		return
	}

	go PersistMessage(container, msgQueue, *msg.ReceiptHandle)
}

var class23IntegrityConstraintViolationErrors = "23"

func PersistMessage(container ProcessorContainer, msg queue.MessageQueue, ReceiptHandle string) {
	fmt.Println("Starting Persist Message: ", msg)
	err := container.processorDb.SaveMessage(msg)
	if err != nil {
		if err, ok := err.(*pq.Error); ok {
			// If the error is something about constraint violation, delete the message
			if err.Code.Class() == pq.ErrorClass(class23IntegrityConstraintViolationErrors) {
				log.Println("Error saving message. Errors message: ", err, ". Deleting the message on the queue.")
				_, err := container.sqsClient.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{
					QueueUrl:      aws.String(container.queueUrl),
					ReceiptHandle: aws.String(ReceiptHandle),
				})
				if err != nil {
					log.Println("Error deleting message: ", err)
				}
			}
			return
		}
		log.Println("Error saving message: ", err)
	}
}
