package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/RomeroGabriel/event-process-app/pkg/queue"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func StartProcessor(sqsClient sqs.Client, queueName string) {
	queueUrl, err := queue.GetOrCreateQueueUrl(sqsClient, queueName)
	if err != nil {
		log.Fatal("Couldn't create/get queue ", queueName, " Error: ", err)
	}
	receiveParams := &sqs.ReceiveMessageInput{
		MaxNumberOfMessages: *aws.Int32(1),
		QueueUrl:            aws.String(queueUrl),
		WaitTimeSeconds:     *aws.Int32(5),
	}

	var msgQueue queue.MessageQueue
	for {
		result, err := sqsClient.ReceiveMessage(context.Background(), receiveParams)
		if err != nil {
			log.Fatalf("Error receiving a message: %s", err)
		}
		for _, msg := range result.Messages {
			err = json.Unmarshal([]byte(*msg.Body), &msgQueue)
			if err != nil {
				fmt.Println("ERROR UNMARSHAL: ", err)
				// delete message if it's not in right format
			}
			go validateMessage(msgQueue)
		}
		fmt.Println("===============================")
		fmt.Println()
	}
}

func validateMessage(msg queue.MessageQueue) {
	fmt.Println("Starting the Validate Phase: ", msg)
}
