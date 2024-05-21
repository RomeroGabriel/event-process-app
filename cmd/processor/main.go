package main

import (
	"database/sql"
	"log"
	"os"

	"github.com/RomeroGabriel/event-process-app/configs"
	"github.com/RomeroGabriel/event-process-app/internal/processor"
	"github.com/RomeroGabriel/event-process-app/pkg/queue"

	// postgres
	_ "github.com/lib/pq"
)

func main() {
	sqsClient, err := configs.CreateSqsClient()
	if err != nil {
		log.Fatal("Error creating queue client: ", err)
	}
	queueName := os.Getenv("QUEUE_NAME")
	if queueName == "" {
		panic("no QUEUE_NAME specified")
	}
	queueUrl, err := queue.GetOrCreateQueueUrl(sqsClient, queueName)
	if err != nil {
		log.Fatal("Couldn't create/get queue ", queueName, " Error: ", err)
	}

	dbDriver := os.Getenv("DB_DRIVER")
	dbConnStr := os.Getenv("DB_CONNECTION")
	if dbDriver == "" {
		panic("no DB_DRIVER specified")
	}
	if dbConnStr == "" {
		panic("no DB_CONNECTION specified")
	}
	database, err := sql.Open(dbDriver, dbConnStr)
	if err != nil {
		panic(err)
	}
	defer database.Close()
	err = database.Ping()
	if err != nil {
		log.Fatal("Error connecting: ", err)
	}

	processorDb, err := processor.NewProcessorRepository(database)
	if err != nil {
		log.Fatal("Error creating repository: ", err)
	}
	app := processor.NewProcessorApp(sqsClient, queueUrl, processorDb)
	app.Execute()

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
	// 		// sqsClient.DeleteQueue(context.TODO(), &sqs.DeleteQueueInput{QueueUrl: aws.String(queueUrl)})
	// 	}
	// }
}
