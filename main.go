package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func getSQSService() (*sqs.SQS, error) {
	sessionInstance, err := session.NewSessionWithOptions(session.Options{
		Profile: "default",
		Config: aws.Config{
			Endpoint:   aws.String("localhost:44566"),
			Region:     aws.String("us-east-1"),
			DisableSSL: aws.Bool(true),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create aws session: %w", err)
	}

	svc := sqs.New(sessionInstance)

	return svc, nil
}

func createQueue() error {
	svc, err := getSQSService()
	if err != nil {
		return fmt.Errorf("failed to get sqs service: %w", err)
	}

	_, err = svc.CreateQueue(&sqs.CreateQueueInput{
		QueueName: aws.String("henrod"),
		Attributes: map[string]*string{
			"DelaySeconds":           aws.String("60"),
			"MessageRetentionPeriod": aws.String("86400"),
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create sqs queue: %w", err)
	}

	return nil
}

func sendMessage() error {
	svc, err := getSQSService()
	if err != nil {
		return fmt.Errorf("failed to get sqs service: %w", err)
	}

	urlResult, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String("henrod"),
	})
	if err != nil {
		return fmt.Errorf("failed to get queue url: %w", err)
	}

	_, err = svc.SendMessage(&sqs.SendMessageInput{
		MessageBody: aws.String("I'm Henrod"),
		QueueUrl:    urlResult.QueueUrl,
	})
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	return nil
}

func receiveMessage() error {
	svc, err := getSQSService()
	if err != nil {
		return fmt.Errorf("failed to get sqs service: %w", err)
	}

	urlResult, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String("henrod"),
	})
	if err != nil {
		return fmt.Errorf("failed to get queue url: %w", err)
	}

	messageResult, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
		MaxNumberOfMessages: aws.Int64(1),
		QueueUrl:            urlResult.QueueUrl,
		VisibilityTimeout:   aws.Int64(10),
	})
	if err != nil {
		return fmt.Errorf("failed to receive message: %w", err)
	}

	print(messageResult.GoString())

	if len(messageResult.Messages) > 0 {
		if _, err = svc.DeleteMessage(&sqs.DeleteMessageInput{
			QueueUrl:      urlResult.QueueUrl,
			ReceiptHandle: messageResult.Messages[0].ReceiptHandle,
		}); err != nil {
			return fmt.Errorf("failed to delete message: %w", err)
		}
	}

	return nil
}

const (
	SendMessageCommand = iota
	CreateQueueCommand
	ReceiveMessageCommand
	RunProducerCommand
	RunConsumerCommand
)

func runProducer() {
	ticker := time.NewTicker(time.Second)

	for {
		select {
		case <-ticker.C:
			print("producing\n")
			err := sendMessage()
			if err != nil {
				print("producer:", err.Error())
			}
		}
	}
}

func runConsumer() {
	ticker := time.NewTicker(time.Second)

	for {
		select {
		case <-ticker.C:
			print("receiving\n")
			err := receiveMessage()
			if err != nil {
				print("receiver:", err.Error())
			}
		}
	}
}

func main() {
	var (
		err     error
		command = RunConsumerCommand
	)

	switch command {
	case SendMessageCommand:
		err = sendMessage()
	case CreateQueueCommand:
		err = createQueue()
	case ReceiveMessageCommand:
		err = receiveMessage()
	case RunProducerCommand:
		runProducer()
	case RunConsumerCommand:
		runConsumer()
	}

	if err != nil {
		panic(err)
	}
}
