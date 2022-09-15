package main

import (
	"context"
	"data-queue/pkg/common"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"time"
)

func main() {
	log.Info("Starting up Consumer Client")

	opt := parseOptions()

	natsConsumerGetSubject := fmt.Sprintf("%s.%s.%s.%s", opt.natsIncomingSubjectPrefix, opt.natsConsumerSubjectPrefix,
		opt.natsGetSubjectSuffix, opt.bucket)
	natsConsumerAckSubject := fmt.Sprintf("%s.%s.%s.%s", opt.natsIncomingSubjectPrefix, opt.natsConsumerSubjectPrefix,
		opt.natsAckSubjectSuffix, opt.bucket)
	natsConsumerAnnSubject := fmt.Sprintf("%s.%s.%s.%s", opt.natsOutgoingSubjectPrefix, opt.natsConsumerSubjectPrefix,
		opt.natsAnnSubjectSuffix, opt.bucket)
	natsReplySubjectPrefix := opt.natsOutgoingSubjectPrefix

	log.Infof("Connecting to NATS '%s' as '%s'", opt.natsUrl, opt.natsName)
	natsConnection, err := common.ConnectToNats(opt.natsUrl, opt.natsName)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		log.Info("Closing User NATS connection")
		natsConnection.Close()
	}()

	jumpToNextItem := make(chan bool, 1)

	log.Infof("Subscribing to '%s'", natsConsumerAckSubject)
	consumerAckSubjectSubscription, err := natsConnection.Subscribe(natsConsumerAnnSubject,
		getConsumerAnnHandler(jumpToNextItem))
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		log.Infof("Unsubscribing from '%s'", natsConsumerAckSubject)

		err = consumerAckSubjectSubscription.Unsubscribe()
		if err != nil {
			log.Error(err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	setJumpToNextItem(jumpToNextItem)

	go func() {
		for {
			select {
			case <-jumpToNextItem:
				log.Info("-> Next")
				jumpToNext := processItem(natsConnection, natsConsumerGetSubject, natsConsumerAckSubject, natsReplySubjectPrefix)
				if jumpToNext {
					setJumpToNextItem(jumpToNextItem)
				}
			case <-ctx.Done():
				log.Info("-> Done")
				return
			case <-time.After(time.Duration(opt.pollTimeoutInMs) * time.Millisecond):
				log.Info("-> Time")
				jumpToNext := processItem(natsConnection, natsConsumerGetSubject, natsConsumerAckSubject, natsReplySubjectPrefix)
				if jumpToNext {
					setJumpToNextItem(jumpToNextItem)
				}
			}
		}
	}()

	exitChannel := make(chan os.Signal, 1)
	signal.Notify(exitChannel, os.Interrupt)

	<-exitChannel

	log.Info("Shutting down Consumer Client")
}

func getConsumerAnnHandler(jumpToNextItem chan bool) func(msg *nats.Msg) {
	return func(msg *nats.Msg) {
		var request common.ConsumerAnnRequest

		log.Infof("Request Consumer Ann [%s]", msg.Subject)

		err := json.Unmarshal(msg.Data, &request)
		if err != nil {
			log.Error(err)
			return
		}

		setJumpToNextItem(jumpToNextItem)
	}
}

func processItem(natsConnection *nats.Conn, natsConsumerGetSubject string, natsConsumerAckSubject string, natsReplySubjectPrefix string) bool {
	request := common.ConsumerGetRequest{}
	buffer, err := json.Marshal(request)
	if err != nil {
		log.Fatal(err)
	}

	log.Infof("Request Consumer Get [%s]", natsConsumerGetSubject)
	msg, err := common.Request(natsConnection, natsConsumerGetSubject, natsReplySubjectPrefix, buffer, 3*time.Second)
	if err != nil {
		if err == nats.ErrTimeout {
			log.Error(err)
			return false
		}

		if err == nats.ErrNoResponders {
			log.Error(err)
			return false
		}

		log.Fatal(err)
	}

	var reply common.ConsumerGetReply
	err = json.Unmarshal(msg.Data, &reply)
	if err != nil {
		log.Error(err)
		return false
	}
	log.Infof("Reply Consumer Get [PacketId: %s, Error: %s]", reply.PacketId, reply.Error)

	if reply.Error != "" {
		err = errors.New(reply.Error)
		log.Error(err)
		return false
	}

	if reply.Data == "" {
		log.Infof("No new data")
		return false
	}

	ackRequest := common.ConsumerAckRequest{PacketId: reply.PacketId}
	buffer, err = json.Marshal(ackRequest)
	if err != nil {
		log.Fatal(err)
	}

	log.Infof("Request Consumer Ack [%s]", natsConsumerAckSubject)
	msg, err = common.Request(natsConnection, natsConsumerAckSubject, natsReplySubjectPrefix, buffer, 3*time.Second)
	if err != nil {
		if err == nats.ErrTimeout {
			log.Error(err)
			return false
		}

		if err == nats.ErrNoResponders {
			log.Error(err)
			return false
		}

		log.Fatal(err)
	}

	var ackReply common.ConsumerAckReply
	err = json.Unmarshal(msg.Data, &ackReply)
	if err != nil {
		log.Error(err)
		return false
	}
	log.Infof("Reply Consumer Ack [Error: %s]", ackReply.Error)

	if ackReply.Error != "" {
		err = errors.New(reply.Error)
		log.Error(err)
		return false
	}

	return true
}

func setJumpToNextItem(jumpToNextItem chan bool) {
	select {
	case jumpToNextItem <- true:
		log.Info("Set JumpToNextItem")
	default:
		log.Info("JumpToNextItem already set")
	}
}
