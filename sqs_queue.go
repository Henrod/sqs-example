package main

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type SQSQueue struct {
	sqs    *sqs.Client
	sqsURL *string
}

const (
	sqsEndpoint                 = "http://localhost:44566"
	sqsRegion                   = "us-east-1"
	sqsVisibilityTimeoutSeconds = 10
	waitTime                    = time.Second
)

func NewSQSQueue(ctx context.Context, sqsName string) (*SQSQueue, error) {
	localResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:   "aws",
			URL:           sqsEndpoint,
			SigningRegion: sqsRegion,
		}, nil
	})

	cfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithSharedConfigProfile("default"),
		config.WithEndpointResolverWithOptions(localResolver),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load default config: %w", err)
	}

	sqsInstance := sqs.NewFromConfig(cfg)

	urlResult, err := sqsInstance.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(sqsName),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get queue URL: %w", err)
	}

	return &SQSQueue{
		sqs:    sqsInstance,
		sqsURL: urlResult.QueueUrl,
	}, nil
}

func (queue *SQSQueue) Produce(ctx context.Context, message string) error {
	_, err := queue.sqs.SendMessage(ctx, &sqs.SendMessageInput{
		MessageBody: aws.String(message),
		QueueUrl:    queue.sqsURL,
	})
	if err != nil {
		return fmt.Errorf("failed to enqueue message: %w", err)
	}

	return nil
}

func (queue *SQSQueue) Consume(ctx context.Context, consumer ConsumerFunc) chan error {
	errChan := make(chan error)

	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				err := queue.consume(ctx, consumer)
				if err != nil {
					errChan <- err
				}
			}
		}
	}(ctx)

	return errChan
}

func (queue *SQSQueue) consume(ctx context.Context, consumer ConsumerFunc) error {
	messageResult, err := queue.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		MaxNumberOfMessages: 1,
		QueueUrl:            queue.sqsURL,
		VisibilityTimeout:   sqsVisibilityTimeoutSeconds,
		WaitTimeSeconds:     int32(waitTime.Seconds()),
	})
	if err != nil {
		return fmt.Errorf("failed to receive message from sqs queue: %w", err)
	}

	if len(messageResult.Messages) == 0 {
		return nil
	}

	message := *messageResult.Messages[0].Body
	err = consumer(ctx, message)
	if err != nil {
		return fmt.Errorf("failed to consume message: %w", err)
	}

	if _, err = queue.sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      queue.sqsURL,
		ReceiptHandle: messageResult.Messages[0].ReceiptHandle,
	}); err != nil {
		return fmt.Errorf("failed to delete message from sqs queue: %w", err)
	}

	return nil
}
