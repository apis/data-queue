package main

import (
	"bytes"
	"data-queue/pkg/common"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/beeker1121/goque"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"os"
	"os/signal"
	"strings"
	"time"
)

type storedItem struct {
	PacketId uuid.UUID
	Data     string
}

func main() {
	log.Info("Starting up Data Stream Service")

	viper.SetDefault("natsUrl", "nats://leaf_user:leaf_user@127.0.0.1:34111")
	viper.SetDefault("storagePath", "./.queue")
	viper.SetDefault("natsProducerPutSubject", "leaf.data-stream.producer.put")
	viper.SetDefault("natsConsumerGetSubject", "leaf.data-stream.consumer.get")
	viper.SetDefault("natsConsumerAckSubject", "leaf.data-stream.consumer.ack")
	viper.SetConfigName("producer_config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			log.Fatalf("fatal error config file: %s", fmt.Errorf("%w", err))
		}
	}
	natsUrl := viper.GetString("natsUrl")
	storagePath := viper.GetString("storagePath")
	natsProducerPutSubject := viper.GetString("natsProducerPutSubject")
	natsConsumerGetSubject := viper.GetString("natsConsumerGetSubject")
	natsConsumerAckSubject := viper.GetString("natsConsumerAckSubject")

	log.Info("Opening Persisted Queue")
	queue, err := goque.OpenPrefixQueue(storagePath)

	defer func() {
		log.Info("Closing Persisted Queue")
		err := queue.Close()
		if err != nil {
			log.Error(err)
		}
	}()

	log.Infof("Connecting to NATS '%s'", natsUrl)
	natsConnection, err := connectToNats(natsUrl, "Data Stream Service Connection")
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		log.Info("Closing User NATS connection")
		natsConnection.Close()
	}()

	log.Infof("Subscribing to '%s'", natsProducerPutSubject)
	producerPutSubjectSubscription, err := natsConnection.Subscribe(natsProducerPutSubject, func(msg *nats.Msg) {
		var request common.ProducerPutRequest

		log.Infof("Request Producer Put: %s", string(msg.Data))
		logMessageFormat := "Reply Producer Put: %s"

		err := json.Unmarshal(msg.Data, &request)
		if err != nil {
			log.Warning(err)
			publish(natsConnection, msg.Reply, &common.ProducerPutReply{Error: err.Error(), PacketId: ""}, logMessageFormat)
			return
		}

		if len(strings.TrimSpace(request.BucketId)) == 0 {
			err = errors.New("bucket id is empty")
			log.Warning(err)
			publish(natsConnection, msg.Reply, &common.ProducerPutReply{Error: err.Error(), PacketId: ""}, logMessageFormat)
			return
		}

		if len(request.Data) == 0 {
			err = errors.New("data is empty")
			log.Warning(err)
			publish(natsConnection, msg.Reply, &common.ProducerPutReply{Error: err.Error(), PacketId: ""}, logMessageFormat)
			return
		}

		//data, err := base64.StdEncoding.DecodeString(request.Data)
		//if err != nil {
		//	log.Warning(err)
		//	publish(natsConnection, msg.Reply, &common.ProducerPutReply{Error: err.Error(), PacketId: ""})
		//	return
		//}

		item := &storedItem{PacketId: uuid.New(), Data: request.Data}
		buffer := new(bytes.Buffer)
		encoder := gob.NewEncoder(buffer)
		err = encoder.Encode(item)
		if err != nil {
			log.Warning(err)
			publish(natsConnection, msg.Reply, &common.ProducerPutReply{Error: err.Error(), PacketId: ""}, logMessageFormat)
			return
		}

		_, err = queue.Enqueue([]byte(request.BucketId), buffer.Bytes())
		if err != nil {
			log.Warning(err)
			publish(natsConnection, msg.Reply, &common.ProducerPutReply{Error: err.Error(), PacketId: ""}, logMessageFormat)
			return
		}

		publish(natsConnection, msg.Reply, &common.ProducerPutReply{Error: "", PacketId: item.PacketId.String()}, logMessageFormat)
	})
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		log.Infof("Unsubscribing from '%s'", natsProducerPutSubject)

		err = producerPutSubjectSubscription.Unsubscribe()
		if err != nil {
			log.Error(err)
		}
	}()

	log.Infof("Subscribing to '%s'", natsConsumerGetSubject)
	consumerGetSubjectSubscription, err := natsConnection.Subscribe(natsConsumerGetSubject, func(msg *nats.Msg) {
	})
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		log.Infof("Unsubscribing from '%s'", natsConsumerGetSubject)

		err = consumerGetSubjectSubscription.Unsubscribe()
		if err != nil {
			log.Error(err)
		}
	}()

	log.Infof("Subscribing to '%s'", natsConsumerAckSubject)
	consumerAckSubjectSubscription, err := natsConnection.Subscribe(natsConsumerAckSubject, func(msg *nats.Msg) {
	})
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

	exitChannel := make(chan os.Signal, 1)
	signal.Notify(exitChannel, os.Interrupt)

	<-exitChannel

	log.Info("Shutting Data Stream Service")
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

func publish(connection *nats.Conn, subject string, data any, logMessageFormat string) {
	buffer, err := json.Marshal(data)
	if err != nil {
		log.Fatal(err)
	}

	log.Infof(logMessageFormat, string(buffer))

	err = connection.Publish(subject, buffer)
	if err != nil {
		log.Fatal(err)
	}
}
