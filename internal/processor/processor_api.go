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
	"github.com/RomeroGabriel/event-process-app/pkg/queue"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/lib/pq"
)

type EventProcessorDb interface {
	SaveMessage(msg eventprocess.EventMessage) error
}

type ProcessorApp struct {
	queueClient queue.QueueClient
	queueUrl    string
	db          EventProcessorDb
	wg          sync.WaitGroup
}

func NewProcessorApp(queueClient queue.QueueClient, queueUrl string, db EventProcessorDb) *ProcessorApp {
	return &ProcessorApp{
		queueClient: queueClient,
		queueUrl:    queueUrl,
		db:          db,
		wg:          sync.WaitGroup{},
	}
}

func (p *ProcessorApp) Execute() {
	ctx, cancel := context.WithCancel(context.Background())
	log.Println("Starting the application!")
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

	for {
		select {
		case <-ctx.Done():
			log.Println("Stop listening to new messages! The app being shut down")
			return
		default:
			// result, err := p.queueClient.ReceiveMessage(context.Background(), receiveParams)
			result, err := p.queueClient.ReceiveMessage()
			if err != nil {
				log.Fatalf("Error receiving a message: %s", err)
			}
			go fakePrint(result)
		}
	}
}

func fakePrint(msg <-chan queue.QueueMessage) {
	data := <-msg
	msgData, ok := data.MessageData.(types.Message)
	if ok {
		fmt.Println("Message: ", data, *msgData.Body)
	} else {
		fmt.Println("Empty Message")
	}
}

var deleteMsgFunc = func(sqsClient queue.QueueClient, queueUrl, ReceiptHandle string) error {
	// _, err := sqsClient.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{
	// 	QueueUrl:      aws.String(queueUrl),
	// 	ReceiptHandle: aws.String(ReceiptHandle),
	// })
	// return err
	return nil
}

func (p *ProcessorApp) ValidateMessage(msg types.Message) {
	// foreach msg
	p.wg.Add(1)
	defer p.wg.Done()
	log.Println("Validating message: ", *msg.MessageId)
	var eventMsg eventprocess.EventMessage
	err := json.Unmarshal([]byte(*msg.Body), &eventMsg)

	if err != nil {
		log.Printf("Error unmarshalling message. Err: %s.\nDeleting the message %s on the queue.", err, *msg.MessageId)
		deleteMsgFunc(p.queueClient, p.queueUrl, *msg.ReceiptHandle)
		return
	}
	eventMsg.MessageId = *msg.MessageId
	if errV := eventMsg.ValidateEmptyFields(); errV != nil {
		log.Println(errV, *msg.MessageId)
		deleteMsgFunc(p.queueClient, p.queueUrl, *msg.ReceiptHandle)
		return
	}

	go p.PersistMessage(eventMsg, msg)
}

var class23IntegrityConstraintViolationErrors = "23"

func (p *ProcessorApp) PersistMessage(msg eventprocess.EventMessage, queueMessage types.Message) {
	// foreach msg
	deleteMsg := true
	p.wg.Add(1)
	defer p.wg.Done()
	log.Println("Persisting message: ", msg.MessageId)
	err := p.db.SaveMessage(msg)
	if err != nil {
		if err, ok := err.(*pq.Error); ok {
			// If the error is something about constraint violation, delete the message
			if err.Code.Class() == pq.ErrorClass(class23IntegrityConstraintViolationErrors) {
				log.Println("Error saving message. Errors message: ", err, ". Deleting the message ", msg.MessageId, "on the queue.")
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
