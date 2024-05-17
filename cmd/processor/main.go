package main

import (
	"fmt"
	"log"
	"os"

	"github.com/RomeroGabriel/event-process-app/configs"
	"github.com/RomeroGabriel/event-process-app/internal/processor"
)

func main() {
	fmt.Println("Starting the Application")

	sqsClient, err := configs.CreateQueueClient()
	if err != nil {
		log.Fatal("Error creating queue client: ", err)
	}

	queueName := os.Getenv("QUEUE_NAME")
	if queueName == "" {
		panic("no QUEUE_NAME specified")
	}
	processor.StartProcessor(sqsClient, queueName)

	// Managing Queue
	// var queueUrls []string
	// paginator := sqs.NewListQueuesPaginator(&sqsClient, &sqs.ListQueuesInput{})

	// for paginator.HasMorePages() {
	// 	output, err := paginator.NextPage(context.TODO())
	// 	if err != nil {
	// 		log.Println("Error listing Queue: ", err)
	// 		continue
	// 	}
	// 	queueUrls = append(queueUrls, output.QueueUrls...)
	// }

	// if len(queueUrls) == 0 {
	// 	fmt.Println("You don't have any queues!")
	// } else {
	// 	for _, queueUrl := range queueUrls {
	// 		fmt.Printf("\t%v\n", queueUrl)
	// 		sqsClient.DeleteQueue(context.TODO(), &sqs.DeleteQueueInput{QueueUrl: aws.String(queueUrl)})
	// 	}
	// }
}
