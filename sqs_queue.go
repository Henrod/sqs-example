package main

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type SQSQueue struct {
	sqs    *sqs.SQS
	sqsURL *string
}

const (
	sqsEndpoint                 = "localhost:44566"
	sqsRegion                   = "us-east-1"
	sqsVisibilityTimeoutSeconds = 10
)

func NewSQSQueue(sqsName string) (*SQSQueue, error) {
	sessionInstance, err := session.NewSessionWithOptions(session.Options{
		Profile: "default",
		Config: aws.Config{
			Endpoint:   aws.String(sqsEndpoint),
			Region:     aws.String(sqsRegion),
			DisableSSL: aws.Bool(true),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create aws session: %w", err)
	}

	sqsInstance := sqs.New(sessionInstance)

	urlResult, err := sqsInstance.GetQueueUrl(&sqs.GetQueueUrlInput{
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
	_, err := queue.sqs.SendMessageWithContext(ctx, &sqs.SendMessageInput{
		MessageBody: aws.String(message),
		QueueUrl:    queue.sqsURL,
	})
	if err != nil {
		return fmt.Errorf("failed to enqueue message: %w", err)
	}

	return nil
}

func (queue *SQSQueue) Consume(ctx context.Context, consumer ConsumerFunc) chan error {
	ticker := time.NewTicker(time.Second)
	errChan := make(chan error)

	go func(ctx context.Context) {
		for {
			select {
			case <-ticker.C:
				err := queue.consume(ctx, consumer)
				if err != nil {
					errChan <- err
				}
			case <-ctx.Done():
				return
			}
		}
	}(ctx)

	return errChan
}

func (queue *SQSQueue) consume(ctx context.Context, consumer ConsumerFunc) error {
	messageResult, err := queue.sqs.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
		MaxNumberOfMessages: aws.Int64(1),
		QueueUrl:            queue.sqsURL,
		VisibilityTimeout:   aws.Int64(sqsVisibilityTimeoutSeconds),
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

	if _, err = queue.sqs.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      queue.sqsURL,
		ReceiptHandle: messageResult.Messages[0].ReceiptHandle,
	}); err != nil {
		return fmt.Errorf("failed to delete message from sqs queue: %w", err)
	}

	return nil
}
