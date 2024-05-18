package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/RomeroGabriel/event-process-app/configs"
	"github.com/RomeroGabriel/event-process-app/pkg/queue"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func fakeMonitorProducer(sqsClient *sqs.Client, queueUrl string) {
	randInt := rand.Intn(100000)
	newMsg := queue.MessageQueue{
		EventType: "monitor-app",
		ClientId:  "client-1",
		Message:   fmt.Sprintf("SALVE O CORINTHIANS! %d", randInt),
	}
	msgBytes, _ := json.Marshal(newMsg)
	sendParams := sqs.SendMessageInput{
		QueueUrl:    aws.String(queueUrl),
		MessageBody: aws.String(string(msgBytes)),
	}
	log.Println("Sending Messages: ", newMsg)
	_, err := sqsClient.SendMessage(context.Background(), &sendParams)
	if err != nil {
		log.Fatalf("Error sending a message: %s", err)
	}
}

func fakeTransactionProducer(sqsClient *sqs.Client, queueUrl string) {
	randInt := rand.Intn(100000)
	newMsg := queue.MessageQueue{
		EventType: "transaction-app",
		ClientId:  "client-2",
		Message:   fmt.Sprintf("LET'S GO KNICKS: %d", randInt),
	}
	msgBytes, _ := json.Marshal(newMsg)
	sendParams := sqs.SendMessageInput{
		QueueUrl:    aws.String(queueUrl),
		MessageBody: aws.String(string(msgBytes)),
	}
	log.Println("Sending Messages: ", newMsg)
	_, err := sqsClient.SendMessage(context.Background(), &sendParams)
	if err != nil {
		log.Fatalf("Error sending a message: %s", err)
	}
}

func fakeUserProducer(sqsClient *sqs.Client, queueUrl string) {
	randInt := rand.Intn(100000)
	newMsg := queue.MessageQueue{
		EventType: "user-app",
		ClientId:  "client-3",
		Message:   fmt.Sprintf("YNWA LIVEPOOL: %d", randInt),
	}
	msgBytes, _ := json.Marshal(newMsg)
	sendParams := sqs.SendMessageInput{
		QueueUrl:    aws.String(queueUrl),
		MessageBody: aws.String(string(msgBytes)),
	}
	log.Println("Sending Messages: ", newMsg)
	_, err := sqsClient.SendMessage(context.Background(), &sendParams)
	if err != nil {
		log.Fatalf("Error sending a message: %s", err)
	}
}

func fakeBadMessage(sqsClient *sqs.Client, queueUrl string) {
	randInt := rand.Intn(100)
	if randInt <= 10 {
		sendParams := sqs.SendMessageInput{
			QueueUrl:    aws.String(queueUrl),
			MessageBody: aws.String("Hello, I'm a bad message"),
		}
		log.Println("Sending BAD Message")
		_, err := sqsClient.SendMessage(context.Background(), &sendParams)
		if err != nil {
			log.Fatalf("Error sending a message: %s", err)
		}
	}
}

func main() {
	log.Println("Starting Fake Producers ------>")
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
	fakeTimeOut, _ := strconv.Atoi(os.Getenv("PRODUCERS_FAKE_TIMEOUT"))
	if fakeTimeOut == 0 {
		fakeTimeOut = 10
	}
	for {
		select {
		case reason := <-signalCh:
			fmt.Println("Shout Down Monitor app. Reason: ", reason.String())
			return
		default:
			go fakeMonitorProducer(&sqsClient, queueUrl)
			go fakeTransactionProducer(&sqsClient, queueUrl)
			go fakeUserProducer(&sqsClient, queueUrl)
			go fakeBadMessage(&sqsClient, queueUrl)
			time.Sleep(time.Duration(fakeTimeOut) * time.Second)
		}
	}
}
