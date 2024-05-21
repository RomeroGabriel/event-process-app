package configs

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func CreateSqsClient() (sqs.Client, error) {
	awsEndpoint := os.Getenv("AWS_ENDPOINT")
	awsRegion := os.Getenv("AWS_REGION")

	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		if awsEndpoint != "" {
			return aws.Endpoint{
				PartitionID:   "aws",
				URL:           awsEndpoint,
				SigningRegion: awsRegion,
			}, nil
		}
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})

	awsCfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(awsRegion),
		config.WithEndpointResolverWithOptions(customResolver),
	)
	if err != nil {
		log.Fatalf("Cannot load the AWS configs: %s", err)
		return sqs.Client{}, err
	}
	sqsClient := sqs.NewFromConfig(awsCfg)
	return *sqsClient, nil
}

func GetOrCreateQueueUrl(sqsClient sqs.Client, queueName string) (string, error) {
	queueAttributes := map[string]string{}
	queueAttributes["DelaySeconds"] = "0"
	queueAttributes["MessageRetentionPeriod"] = "86400" // 24 hours
	redrivePolicyJSON := map[string]string{
		"maxReceiveCount": "5",
	}
	redrivePolicy, _ := json.Marshal(redrivePolicyJSON)
	queueAttributes["RedrivePolicy"] = string(redrivePolicy)
	queueConfig := &sqs.CreateQueueInput{
		QueueName:  aws.String(queueName),
		Attributes: queueAttributes,
	}

	result, err := sqsClient.CreateQueue(context.Background(), queueConfig)
	if err != nil {
		var already *types.QueueNameExists
		if errors.As(err, &already) {
			url, err := sqsClient.GetQueueUrl(context.Background(), &sqs.GetQueueUrlInput{QueueName: &queueName})
			if err != nil {
				return "", err
			}
			return *url.QueueUrl, nil
		}
		return "", err
	}
	return *result.QueueUrl, nil
}
