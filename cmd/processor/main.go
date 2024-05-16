package main

import (
	"context"
	"fmt"
	"log"

	"github.com/RomeroGabriel/event-process-app/internal/processor"
	// "github.com/RomeroGabriel/event-process-app/internal/processor"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func main() {
	fmt.Println("Starting the Application")

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
	processor.StartProcessor(*sqsClient)

	// Managing Queue
	// var queueUrls []string
	// paginator := sqs.NewListQueuesPaginator(sqsClient, &sqs.ListQueuesInput{})

	// for paginator.HasMorePages() {
	// 	output, _ := paginator.NextPage(context.TODO())
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
