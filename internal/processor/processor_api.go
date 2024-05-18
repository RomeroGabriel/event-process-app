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
)

// Criar um "container" com as config para serem injetadas
func StartProcessor(sqsClient sqs.Client, queueName string) {
	log.Println("Starting the PROCESSOR EVENTS----->")
	queueUrl, err := queue.GetOrCreateQueueUrl(sqsClient, queueName)
	if err != nil {
		log.Fatal("Couldn't create/get queue ", queueName, " Error: ", err)
	}
	receiveParams := &sqs.ReceiveMessageInput{
		MaxNumberOfMessages: *aws.Int32(1),
		QueueUrl:            aws.String(queueUrl),
		WaitTimeSeconds:     *aws.Int32(3),
	}

	log.Println("Listening queue messages")
	for {
		result, err := sqsClient.ReceiveMessage(context.Background(), receiveParams)
		if err != nil {
			log.Fatalf("Error receiving a message: %s", err)
		}
		log.Printf("Received %d messages.", len(result.Messages))
		for _, msg := range result.Messages {
			go validateMessage(&sqsClient, msg, queueUrl)
		}
	}
}

func validateMessage(sqsClient *sqs.Client, msg types.Message, queueUrl string) {
	var msgQueue queue.MessageQueue
	fmt.Println("Starting the Validate Phase: ", string(*msg.Body))
	err := json.Unmarshal([]byte(*msg.Body), &msgQueue)
	if err != nil {
		log.Printf("Error unmarshalling message. Err: %s.\nDeleting the message on the queue.", err)
		_, errD := sqsClient.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{
			QueueUrl:      aws.String(queueUrl),
			ReceiptHandle: msg.ReceiptHandle,
		})
		if errD != nil {
			log.Println("Error deleting message: ", queueUrl)
		}
	}
	// Validate client exist
	// Validete event type
	// Validate if msg is repetead
}
