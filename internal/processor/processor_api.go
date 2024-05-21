package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/RomeroGabriel/event-process-app/pkg/eventprocess"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/lib/pq"
)

type EventProcessorDb interface {
	SaveMessage(msg eventprocess.EventMessage) error
}

type ProcessorApp struct {
	queueClient sqs.Client
	queueUrl    string
	db          EventProcessorDb
	wg          sync.WaitGroup
}

func NewProcessorApp(queueClient sqs.Client, queueUrl string, db EventProcessorDb) *ProcessorApp {
	return &ProcessorApp{
		queueClient: queueClient,
		queueUrl:    queueUrl,
		db:          db,
		wg:          sync.WaitGroup{},
	}
}

func (p *ProcessorApp) Execute() {
	ctx, cancel := context.WithCancel(context.Background())
	go p.ReceiveMessage(ctx)

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
	reason := <-signalCh
	log.Println("\nStarting gracefully shutting down the app. Reason: ", reason)
	cancel()
	p.wg.Wait()
	log.Println("Finish shutdown the app.")
}

func (p *ProcessorApp) ReceiveMessage(ctx context.Context) {
	p.wg.Add(1)
	defer p.wg.Done()

	receiveParams := &sqs.ReceiveMessageInput{
		MaxNumberOfMessages: *aws.Int32(1),
		QueueUrl:            aws.String(p.queueUrl),
		WaitTimeSeconds:     *aws.Int32(3),
	}
	for {
		select {
		case <-ctx.Done():
			log.Println("Stop listening to new messages! The app being shut down")
			return
		default:
			result, err := p.queueClient.ReceiveMessage(context.Background(), receiveParams)
			if err != nil {
				log.Fatalf("Error receiving a message: %s", err)
			}
			log.Printf("Received %d messages.", len(result.Messages))
			if len(result.Messages) > 0 {
				for _, msg := range result.Messages {
					go p.ValidateMessage(msg)
				}
			}
		}
	}
}

var deleteMsgFunc = func(sqsClient sqs.Client, queueUrl, ReceiptHandle string) error {
	_, err := sqsClient.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(queueUrl),
		ReceiptHandle: aws.String(ReceiptHandle),
	})
	return err
}

func (p *ProcessorApp) ValidateMessage(msg types.Message) {
	// foreach msg
	p.wg.Add(1)
	defer p.wg.Done()
	fmt.Println("Starting the Validate Phase: ", string(*msg.Body))
	var eventMsg eventprocess.EventMessage
	err := json.Unmarshal([]byte(*msg.Body), &eventMsg)

	if err != nil {
		log.Printf("Error unmarshalling message. Err: %s.\nDeleting the message on the queue.", err)
		deleteMsgFunc(p.queueClient, p.queueUrl, *msg.ReceiptHandle)
		return
	}
	eventMsg.MessageId = *msg.MessageId
	if errV := eventMsg.ValidateEmptyFields(); errV != nil {
		log.Println(errV)
		deleteMsgFunc(p.queueClient, p.queueUrl, *msg.ReceiptHandle)
		return
	}

	go p.PersistMessage(eventMsg, msg)
	fmt.Println("Finishing Validate Message: ", eventMsg.Message)
}

var class23IntegrityConstraintViolationErrors = "23"

func (p *ProcessorApp) PersistMessage(msg eventprocess.EventMessage, queueMessage types.Message) {
	// foreach msg
	deleteMsg := true
	p.wg.Add(1)
	defer p.wg.Done()
	fmt.Println("Starting Persist Message: ", msg)
	err := p.db.SaveMessage(msg)
	if err != nil {
		if err, ok := err.(*pq.Error); ok {
			// If the error is something about constraint violation, delete the message
			if err.Code.Class() == pq.ErrorClass(class23IntegrityConstraintViolationErrors) {
				log.Println("Error saving message. Errors message: ", err, ". Deleting the message on the queue.")
			} else {
				deleteMsg = false
				log.Println("Error saving message: ", err)
			}
		}
	}
	if deleteMsg {
		deleteMsgFunc(p.queueClient, p.queueUrl, *queueMessage.ReceiptHandle)
		if err != nil {
			log.Println("Error deleting message: ", err)
		}
	}
}
