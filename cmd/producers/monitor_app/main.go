package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/RomeroGabriel/event-process-app/configs"
	"github.com/RomeroGabriel/event-process-app/pkg/queue"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func sendMsg(sqsClient *sqs.Client, queueUrl string, randInt int) {

	finalRand := rand.Intn(50)
	finalRand = finalRand + randInt
	log.Println("Sending Messages Start: ", randInt, finalRand)

	sendParams := sqs.SendMessageInput{
		MessageBody: aws.String(fmt.Sprintf("Hi Corinthians! %d", rand.Intn(100))),
		QueueUrl:    aws.String(queueUrl),
	}
	if finalRand%2 == 0 {
		_, err := sqsClient.SendMessage(context.Background(), &sendParams)
		if err != nil {
			log.Fatalf("Error sending a message: %s", err)
		}
	} else {
		for i := 0; i < finalRand; i++ {
			sendParams.MessageBody = aws.String(fmt.Sprintf("Hi Corinthians! %d", i))
			_, err := sqsClient.SendMessage(context.Background(), &sendParams)
			if err != nil {
				log.Fatalf("Error sending a message: %s", err)
			}
		}
	}
}

func main() {
	log.Println("Starting Monitor App ------>")
	sqsClient, err := configs.CreateQueueClient()
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

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	for {
		select {
		case reason := <-signalCh:
			fmt.Println("Shout Down Monitor app. Reason: ", reason.String())
			return
		default:
			randInt := rand.Intn(50)
			go sendMsg(&sqsClient, queueUrl, randInt)
			time.Sleep(time.Duration(randInt) * time.Second)
		}
	}
}
