package main

import (
	"context"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"os"
	"os/signal"
	"time"

	"github.com/nats-io/nats.go"
)

const natsSubjectTimeEvent = "time-event"
const natsSubjectGetTime = "get-time"
const natsSubjectTimeStream = "time-stream"
const streamName = "time-stream"
const consumerDurableName = "time-stream-consumer"
const leafPrefix = "leaf"

func handleGetTime(natsConnection *nats.Conn) {
	log.Printf("Request '%s'", natsSubjectGetTime)
	//getTimeMsg, err := natsConnection.Request(natsSubjectGetTime, nil, time.Second*3)
	getTimeMsg, err := request(natsConnection, fmt.Sprintf("%s.%s", leafPrefix, natsSubjectGetTime), leafPrefix, nil, time.Second*3)
	if err != nil {
		log.Fatal(err)
	}

	var currentTime string
	err = json.Unmarshal(getTimeMsg.Data, &currentTime)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Reply '%s': '%s'", natsSubjectGetTime, currentTime)
}

func handleTimeEvent(natsConnection *nats.Conn) *nats.Subscription {
	log.Infof("Subscribing to '%s'", natsSubjectTimeEvent)
	subscription, err := natsConnection.Subscribe(fmt.Sprintf("%s.%s", leafPrefix, natsSubjectTimeEvent), func(msg *nats.Msg) {
		var message string
		err := json.Unmarshal(msg.Data, &message)
		if err != nil {
			log.Fatal(err)
		}

		log.Infof("Handling '%s': '%s'", natsSubjectTimeEvent, message)
	})

	if err != nil {
		log.Fatal(err)
	}

	return subscription
}

func handleTimeStream(jetStreamContext nats.JetStreamContext) (*nats.Subscription, context.CancelFunc) {
	stream, err := jetStreamContext.StreamInfo(streamName)

	if stream == nil {
		log.Printf("Creating mirror stream '%s'", streamName)

		var apiPrefix = "$JS.leaf.API"
		stream, err = jetStreamContext.AddStream(&nats.StreamConfig{
			Name:    streamName,
			Storage: nats.FileStorage,
			Mirror: &nats.StreamSource{
				Name:     streamName,
				External: &nats.ExternalStream{APIPrefix: apiPrefix}},
		})
		if err != nil {
			log.Fatal(err)
		}
	} else {
		log.Printf("Stream '%s' already exists", streamName)
	}

	log.Printf("Stream info: '%v'", stream)

	consumer, err := jetStreamContext.ConsumerInfo(streamName, consumerDurableName)
	if consumer == nil {
		log.Printf("Creating consumer '%s' for stream '%s'", consumerDurableName, streamName)

		consumer, err = jetStreamContext.AddConsumer(streamName, &nats.ConsumerConfig{
			Durable:   consumerDurableName,
			AckPolicy: nats.AckExplicitPolicy})
		if err != nil {
			log.Fatal(err)
		}
	} else {
		log.Printf("Consumer '%s' already exists", consumerDurableName)
	}

	log.Printf("Consumer info: '%v'", consumer)

	log.Info("Subscribing to TimeStream")

	subscription, err := jetStreamContext.PullSubscribe(natsSubjectTimeStream, consumerDurableName,
		nats.Bind(streamName, consumerDurableName))
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			messages, _ := subscription.Fetch(10, nats.Context(ctx))
			for _, msg := range messages {

				var message string
				err := json.Unmarshal(msg.Data, &message)
				if err != nil {
					log.Fatal(err)
				}

				log.Printf("TimeStream: '%s'", message)
				err = msg.AckSync()
				if err != nil {
					log.Fatal(err)
				}
			}
		}
	}()

	return subscription, cancel
}

func main() {
	log.Info("Starting up client")

	exitChannel := make(chan os.Signal, 1)
	signal.Notify(exitChannel, os.Interrupt)

	viper.SetDefault("natsUserUrl", "nats://hub_user:hub_user@127.0.0.1:34222")
	viper.SetDefault("natsIngressUrl", "nats://leaf_ingress:leaf_ingress@127.0.0.1:34222")
	viper.SetConfigName("consumer_config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			log.Fatalf("fatal error config file: %s", fmt.Errorf("%w", err))
		}
	}
	natsUserUrl := viper.GetString("natsUserUrl")
	natsIngressUrl := viper.GetString("natsIngressUrl")

	log.Infof("Connecting Ingress to NATS '%s'", natsIngressUrl)
	natsIngressConnection, err := connectToNats(natsIngressUrl, "Client Ingress Connection")
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		log.Info("Closing Ingress NATS connection")
		natsIngressConnection.Close()
	}()

	jetStreamContext, err := natsIngressConnection.JetStream()
	if err != nil {
		log.Fatal(err)
	}

	timeStreamSubscription, cancel := handleTimeStream(jetStreamContext)

	defer func() {
		log.Info("Unsubscribing from TimeStream")

		cancel()

		err = timeStreamSubscription.Unsubscribe()
		if err != nil {
			log.Fatal(err)
		}
	}()

	log.Infof("Connecting User to NATS '%s'", natsUserUrl)
	natsUserConnection, err := connectToNats(natsUserUrl, "Client User Connection")
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		log.Info("Closing User NATS connection")
		natsUserConnection.Close()
	}()

	handleGetTime(natsUserConnection)

	timeEventSubscription := handleTimeEvent(natsUserConnection)

	defer func() {
		log.Info("Unsubscribing from TimeEvent")

		err = timeEventSubscription.Unsubscribe()
		if err != nil {
			log.Fatal(err)
		}
	}()

	<-exitChannel

	log.Info("Shutting down client")
}

func connectToNats(natsUserUrl string, connectionName string) (*nats.Conn, error) {
	options := nats.Options{
		Url:  natsUserUrl,
		Name: connectionName,
	}

	var natsConnection *nats.Conn
	var err error

	for index := 0; index < 5; index++ {
		if index > 0 {
			time.Sleep(time.Second)
		}

		log.Info("Attempting to connect to NATS")
		natsConnection, err = options.Connect()
		if err == nil {
			break
		}

		log.Errorf("NATS connection failed [%v]", err)
	}

	return natsConnection, err
}

func request(natsConnection *nats.Conn, subject string, requestInboxPrefix string, data []byte, timeout time.Duration) (*nats.Msg, error) {
	if natsConnection == nil {
		return nil, nats.ErrInvalidConnection
	}

	//reply := fmt.Sprintf("%s.%s", requestInboxPrefix, nats.NewInbox())
	reply := nats.NewInbox()
	log.Infof("Inbox: %s", reply)

	subscription, err := natsConnection.SubscribeSync(reply)
	if err != nil {
		return nil, err
	}

	defer func() {
		err = subscription.Unsubscribe()
		if err != nil {
			log.Errorf("Unsubscribe: %s", fmt.Errorf("%w", err))
		}
	}()

	if err := natsConnection.PublishRequest(subject, reply, data); err != nil {
		return nil, err
	}

	msg, err := subscription.NextMsg(timeout)
	if err != nil {
		return nil, err
	}

	//log.Infof("Reply: %s", msg.Data)
	return msg, nil
}
