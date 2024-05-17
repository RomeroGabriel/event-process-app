package processor

import (
	"context"
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

	for {
		result, err := sqsClient.ReceiveMessage(context.Background(), receiveParams)
		if err != nil {
			log.Fatalf("Error receiving a message: %s", err)
		}
		for _, msg := range result.Messages {
			fmt.Println("Message RECEIVED: ", msg)
			fmt.Println("Text: ", *msg.Body)
			// deleteParams := &sqs.DeleteMessageInput{
			// 	QueueUrl:      aws.String("http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/CorinthiansQueue"),
			// 	ReceiptHandle: msg.ReceiptHandle,
			// }
			// _, err = sqsClient.DeleteMessage(context.Background(), deleteParams)
			// if err != nil {
			// 	log.Fatalf("Error deleting msg: %s", err)
			// }
		}
		fmt.Println("===============================")
		fmt.Println()
	}
}
