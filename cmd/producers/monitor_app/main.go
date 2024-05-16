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

	"github.com/RomeroGabriel/event-process-app/pkg/queue"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

var QueueName string = "SQS_QUEUE"

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

	awsEndpoint := "http://localhost:4566"
	awsRegion := "us-east-1"

	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		if awsEndpoint != "" {
			return aws.Endpoint{
				PartitionID:   "aws",
				URL:           awsEndpoint,
				SigningRegion: awsRegion,
			}, nil
		}
		// returning EndpointNotFoundError will allow the service to fallback to its default resolution
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})

	awsCfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(awsRegion),
		config.WithEndpointResolverWithOptions(customResolver),
	)
	if err != nil {
		log.Fatalf("Cannot load the AWS configs: %s", err)
	}
	sqsClient := sqs.NewFromConfig(awsCfg)
	queueUrl, err := queue.GetOrCreateQueueUrl(*sqsClient, QueueName)
	if err != nil {
		log.Fatalf("Couldn't create queue %v. Here's why: %v\n", QueueName, err)
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
			go sendMsg(sqsClient, queueUrl, randInt)
			time.Sleep(time.Duration(randInt) * time.Second)
		}
	}
}
