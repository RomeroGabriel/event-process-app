package queue

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

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
