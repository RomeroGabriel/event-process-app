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

	// postgres
	_ "github.com/lib/pq"
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

// Criar um "container" com as config para serem injetadas
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
	var msgQueue queue.MessageQueue
	fmt.Println("Starting the Validate Phase: ", string(*msg.Body))
	err := json.Unmarshal([]byte(*msg.Body), &msgQueue)
	if err != nil {
		log.Printf("Error unmarshalling message. Err: %s.\nDeleting the message on the queue.", err)
		_, errD := container.sqsClient.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{
			QueueUrl:      aws.String(container.queueUrl),
			ReceiptHandle: msg.ReceiptHandle,
		})
		if errD != nil {
			log.Println("Error deleting message: ", errD)
		}
	}
	allClients, err := container.processorDb.FindAllClient()
	fmt.Println(allClients)
	fmt.Println(err)
	// Validate client exist
	// Validete event type
	// Validate if msg is repetead
}
