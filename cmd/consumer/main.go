package main

import (
	"context"
	"data-queue/pkg/common"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"os"
	"os/signal"
	"time"
)

func main() {
	log.Info("Starting up Consumer Client")

	viper.SetDefault("natsUrl", "nats://leaf_user:leaf_user@127.0.0.1:34111")
	viper.SetDefault("natsName", "consumer1")
	viper.SetDefault("natsConsumerGetSubject", "leaf.data-stream.consumer.get")
	viper.SetDefault("natsConsumerAckSubject", "leaf.data-stream.consumer.ack")
	viper.SetDefault("natsConsumerAnnSubject", "leaf.data-stream.consumer.ann")
	viper.SetDefault("bucket", "bucket1")
	viper.SetDefault("pollTimeoutInMs", 1000)
	viper.SetConfigName("consumer_config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			log.Fatalf("fatal error config file: %s", fmt.Errorf("%w", err))
		}
	}

	pflag.String("natsName", "consumer2", "NATS Connection Name")
	pflag.String("bucket", "bucket2", "Queue bucket name")
	pflag.Parse()
	err := viper.BindPFlags(pflag.CommandLine)
	if err != nil {
		log.Fatal(err)
	}

	natsUrl := viper.GetString("natsUrl")
	natsName := viper.GetString("natsName")
	bucket := viper.GetString("bucket")
	pollTimeoutInMs := viper.GetInt("pollTimeoutInMs")
	natsConsumerGetSubject := viper.GetString("natsConsumerGetSubject") + "." + bucket
	natsConsumerAckSubject := viper.GetString("natsConsumerAckSubject") + "." + bucket
	natsConsumerAnnSubject := viper.GetString("natsConsumerAnnSubject") + "." + bucket

	log.Infof("Connecting to NATS '%s' as '%s'", natsUrl, natsName)
	natsConnection, err := common.ConnectToNats(natsUrl, natsName)
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
				jumpToNext := processItem(natsConnection, natsConsumerGetSubject, natsConsumerAckSubject)
				if jumpToNext {
					setJumpToNextItem(jumpToNextItem)
				}
			case <-ctx.Done():
				log.Info("-> Done")
				return
			case <-time.After(time.Duration(pollTimeoutInMs) * time.Millisecond):
				log.Info("-> Time")
				jumpToNext := processItem(natsConnection, natsConsumerGetSubject, natsConsumerAckSubject)
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

func processItem(natsConnection *nats.Conn, natsConsumerGetSubject string, natsConsumerAckSubject string) bool {
	request := common.ConsumerGetRequest{}
	buffer, err := json.Marshal(request)
	if err != nil {
		log.Fatal(err)
	}

	log.Infof("Request Consumer Get [%s]", natsConsumerGetSubject)
	msg, err := natsConnection.Request(natsConsumerGetSubject, buffer, 3*time.Second)
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
	msg, err = natsConnection.Request(natsConsumerAckSubject, buffer, 3*time.Second)
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
