package sqs

import (
	"context"
	"fmt"
	"github.com/ThreeDotsLabs/watermill"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/davecgh/go-spew/spew"
	"github.com/renstrom/shortuuid"
	"strconv"
	"sync"
	"testing"
	"time"
)

// TODO wkpo remove!!

func TestWkpo(t *testing.T) {
	//testWkpoRedis()
	testWkpoAws()
}

// TODO wkpo seems it needs the queue to already exist!
func testWkpoAws() {
	testWkpoGeneric(
		func() (message.Publisher, error) {
			return NewPublisher(PublisherConfig{
				AWSConfig: awsConfig(),
				Marshaler: DefaultMarshalerUnmarshaler{},
			}, logger())
		},
		func() (message.Subscriber, error) {
			return NewSubsciber(SubscriberConfig{
				AWSConfig:   awsConfig(),
				Unmarshaler: DefaultMarshalerUnmarshaler{},
			}, logger())
		},
	)
}

func awsConfig() aws.Config {
	region := "us-west-2"
	return aws.Config{
		Credentials: nil,
		Region:      &region,
	}
}

func logger() watermill.LoggerAdapter {
	return watermill.NewStdLogger(
		true,  // debug
		false, // trace
	)
}

func testWkpoGeneric(pubFactory func() (message.Publisher, error), subFactory func() (message.Subscriber, error)) {
	nSubscribers := 10
	nPublishers := 10
	nMsgsPerPublisher := 100

	// TODO wkpo actual test below

	topicName := "wkpotpic" // TODO wkpo what is that sh*t used for???

	nMsgs := nPublishers * nMsgsPerPublisher

	messages := make([]*message.Message, nMsgs)
	for i := 0; i < nMsgs; i++ {
		messages[i] = message.NewMessage(shortuuid.New(), []byte(strconv.Itoa(i)))
	}

	// TODO wkpo play with order starting pub or sub before or after?

	var wgPub sync.WaitGroup
	wgPub.Add(nPublishers)

	startPublishers := func() {
		for i := 0; i < nPublishers; i++ {
			go func(pubId int) {
				pub, err := pubFactory()
				if err != nil {
					p("unable to create publisher %d: %v", pubId, err)
				}

				for j := 0; j < nMsgsPerPublisher; j++ {
					if err := pub.Publish(topicName, messages[pubId*nMsgsPerPublisher+j]); err != nil {
						p("error publishing message %d in pub %d: %v", j, pubId, err)
					}
				}
				wgPub.Done()
			}(i)
		}
	}

	var wgSub sync.WaitGroup
	wgSub.Add(nSubscribers)
	receivedIdsChan := make(chan int, nMsgs*nSubscribers) // TODO wkpo wtf??
	subContext, subContextCancel := context.WithCancel(context.Background())
	startSubscribers := func() {
		for i := 0; i < nSubscribers; i++ {
			go func(subId int) {
				sub, err := subFactory()
				if err != nil {
					p("unable to create sub %d: %v", subId, err)
				}

				router, err := message.NewRouter(message.RouterConfig{}, logger())
				if err != nil {
					p("cannot build router in sub %d: %v", subId, err)
				}

				router.AddHandler(
					fmt.Sprintf("handler-%s", shortuuid.New()),
					topicName,
					sub,
					"",
					nil,
					func(msg *message.Message) ([]*message.Message, error) {
						msgId, err := strconv.Atoi(string(msg.Payload))
						if err != nil {
							p("no ID on message?? %q", msg.UUID)
						}

						select {
						case receivedIdsChan <- msgId:
						default:
							p("unable to send from sub %d: %v", subId, err)
						}

						return nil, nil
					},
				)

				if err := router.Run(subContext); err != nil {
					p("sub %d error: %v", subId, err)
				}

				wgSub.Done()
			}(i)
		}
	}

	startSubscribers()
	startPublishers()

	wgPub.Wait()

	receivedIds := make([]int, nMsgs)

	for {
		select {
		case msgId := <-receivedIdsChan:
			receivedIds[msgId]++
		case <-time.After(3 * time.Second):
			goto b
		}
	}
b:
	spew.Dump(receivedIds)

	time.Sleep(5 * time.Second)
	if len(receivedIdsChan) != 0 {
		spew.Dump(receivedIdsChan)
		p("left over messages")
	}

	subContextCancel()
	wgSub.Wait()
}

func p(format string, a ...any) {
	panic(fmt.Sprintf(format, a...))
}
