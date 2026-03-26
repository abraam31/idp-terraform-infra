package queue

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type SQSQueue struct {
	client   *sqs.Client
	queueURL string
}

type SQSMessage struct {
	Body          []byte
	ReceiptHandle string
	MessageID     string
}

func NewSQSQueue(ctx context.Context, region, queueURL string) (*SQSQueue, error) {
	if queueURL == "" {
		return nil, errors.New("queueURL is required")
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, err
	}
	return &SQSQueue{
		client:   sqs.NewFromConfig(cfg),
		queueURL: queueURL,
	}, nil
}

func (q *SQSQueue) Enqueue(ctx context.Context, kind string, payload []byte) (string, error) {
	id := newID()
	env := JobEnvelope{
		ID:        id,
		Kind:      kind,
		CreatedAt: time.Now().UTC(),
		Payload:   append([]byte(nil), payload...),
	}
	b, err := json.Marshal(env)
	if err != nil {
		return "", err
	}
	out, err := q.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(q.queueURL),
		MessageBody: aws.String(string(b)),
	})
	if err != nil {
		return "", err
	}
	if out.MessageId != nil && *out.MessageId != "" {
		return *out.MessageId, nil
	}
	return id, nil
}

func (q *SQSQueue) Dequeue(ctx context.Context) (*SQSMessage, error) {
	out, err := q.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(q.queueURL),
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     10,
		VisibilityTimeout:   30,
	})
	if err != nil {
		return nil, err
	}
	if len(out.Messages) == 0 {
		return nil, fs.ErrNotExist
	}

	msg := out.Messages[0]
	body := []byte(aws.ToString(msg.Body))
	parsedBody := body
	var env JobEnvelope
	if err := json.Unmarshal(body, &env); err == nil && len(env.Payload) > 0 {
		parsedBody = env.Payload
	}

	return &SQSMessage{
		Body:          parsedBody,
		ReceiptHandle: aws.ToString(msg.ReceiptHandle),
		MessageID:     aws.ToString(msg.MessageId),
	}, nil
}

func (q *SQSQueue) MarkDone(ctx context.Context, receiptHandle string) error {
	if receiptHandle == "" {
		return nil
	}
	_, err := q.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(q.queueURL),
		ReceiptHandle: aws.String(receiptHandle),
	})
	return err
}
