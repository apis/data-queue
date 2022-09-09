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
	natsProducerPutSubjectPrefix := viper.GetString("natsProducerPutSubject") + "."
	natsProducerPutSubject := natsProducerPutSubjectPrefix + "*"
	natsConsumerGetSubject := viper.GetString("natsConsumerGetSubject") + ".*"
	natsConsumerAckSubject := viper.GetString("natsConsumerAckSubject") + ".*"

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
	natsConnection, err := common.ConnectToNats(natsUrl, "Data Stream Service Connection")
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

		log.Infof("Request Producer Put [%s] [%s]", msg.Subject, string(msg.Data))
		logMessageFormat := "Reply Producer Put [%s]"

		bucketId, err := getBucketId(msg.Subject, natsProducerPutSubjectPrefix)
		if err != nil {
			log.Warning(err)
			publishProducerPutReplyError(natsConnection, msg.Reply, err, logMessageFormat)
			return
		}

		err = json.Unmarshal(msg.Data, &request)
		if err != nil {
			log.Warning(err)
			publishProducerPutReplyError(natsConnection, msg.Reply, err, logMessageFormat)
			return
		}

		if len(request.Data) == 0 {
			err = errors.New("parameter data is empty")
			log.Warning(err)
			publishProducerPutReplyError(natsConnection, msg.Reply, err, logMessageFormat)
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
			publishProducerPutReplyError(natsConnection, msg.Reply, err, logMessageFormat)
			return
		}

		_, err = queue.Enqueue([]byte(bucketId), buffer.Bytes())
		if err != nil {
			log.Warning(err)
			publishProducerPutReplyError(natsConnection, msg.Reply, err, logMessageFormat)
			return
		}

		common.Publish(natsConnection, msg.Reply, &common.ProducerPutReply{Error: "", PacketId: item.PacketId.String()},
			logMessageFormat)
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
		var request common.ConsumerGetRequest

		log.Infof("Request Consumer Get: %s", string(msg.Data))
		logMessageFormat := "Reply Consumer Get: %s"

		err := json.Unmarshal(msg.Data, &request)
		if err != nil {
			log.Warning(err)
			common.Publish(natsConnection, msg.Reply, &common.ConsumerGetReply{Error: err.Error(), PacketId: "", Data: ""},
				logMessageFormat)
			return
		}

		if len(strings.TrimSpace(request.BucketId)) == 0 {
			err = errors.New("parameter bucket_id is empty")
			log.Warning(err)
			common.Publish(natsConnection, msg.Reply, &common.ConsumerGetReply{Error: err.Error(), PacketId: "", Data: ""},
				logMessageFormat)
			return
		}

		queueItem, err := queue.Peek([]byte(request.BucketId))
		if err != nil {
			if err != goque.ErrEmpty {
				log.Warning(err)
				common.Publish(natsConnection, msg.Reply, &common.ConsumerGetReply{Error: err.Error(), PacketId: "", Data: ""},
					logMessageFormat)
				return
			}

			log.Info("No data available in a queue")
			common.Publish(natsConnection, msg.Reply, &common.ConsumerGetReply{Error: "", PacketId: "", Data: ""},
				logMessageFormat)
			return
		}

		buffer := new(bytes.Buffer)
		_, err = buffer.Write(queueItem.Value)
		if err != nil {
			log.Warning(err)
			common.Publish(natsConnection, msg.Reply, &common.ConsumerGetReply{Error: err.Error(), PacketId: "", Data: ""},
				logMessageFormat)
			return
		}

		var item storedItem
		decoder := gob.NewDecoder(buffer)
		err = decoder.Decode(&item)
		if err != nil {
			log.Warning(err)
			common.Publish(natsConnection, msg.Reply, &common.ConsumerGetReply{Error: err.Error(), PacketId: "", Data: ""},
				logMessageFormat)
			return
		}

		common.Publish(natsConnection, msg.Reply, &common.ConsumerGetReply{Error: "", PacketId: item.PacketId.String(),
			Data: item.Data}, logMessageFormat)
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

func publishProducerPutReplyError(connection *nats.Conn, subject string, err error, logMessageFormat string) {
	common.Publish(connection, subject, &common.ProducerPutReply{Error: err.Error(), PacketId: ""},
		logMessageFormat)
}

func getBucketId(actualSubject string, prefixSubject string) (string, error) {
	bucketId := strings.TrimPrefix(actualSubject, prefixSubject)
	if bucketId == actualSubject {
		err := errors.New("bucket_id was not found")
		return "", err
	}
	return bucketId, nil
}
