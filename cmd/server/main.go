package main

import (
	"data-queue/pkg/common"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/beeker1121/goque"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"os"
	"os/signal"
	"strconv"
	"strings"
)

func main() {
	log.Info("Starting up Data Stream Service")

	viper.SetDefault("natsUrl", "nats://leaf_user:leaf_user@127.0.0.1:34111")
	viper.SetDefault("storagePath", "./.queue")
	viper.SetDefault("natsProducerPutSubject", "leaf.data-stream.producer.put")
	viper.SetDefault("natsConsumerGetSubject", "leaf.data-stream.consumer.get")
	viper.SetDefault("natsConsumerAckSubject", "leaf.data-stream.consumer.ack")
	viper.SetConfigName("server_config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("./")
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			log.Fatalf("fatal error config file: %s", fmt.Errorf("%w", err))
		}
	}
	natsUrl := viper.GetString("natsUrl")
	storagePath := viper.GetString("storagePath")
	natsProducerPutSubjectPrefix := viper.GetString("natsProducerPutSubject") + "."
	natsProducerPutSubject := natsProducerPutSubjectPrefix + "*"
	natsConsumerGetSubjectPrefix := viper.GetString("natsConsumerGetSubject") + "."
	natsConsumerGetSubject := natsConsumerGetSubjectPrefix + "*"
	natsConsumerAckSubjectPrefix := viper.GetString("natsConsumerAckSubject") + "."
	natsConsumerAckSubject := natsConsumerAckSubjectPrefix + "*"

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

		log.Infof("Request Producer Put [%s]", msg.Subject)

		bucketId, err := getBucketId(msg.Subject, natsProducerPutSubjectPrefix)
		if err != nil {
			log.Warning(err)
			publishProducerPutReplyError(natsConnection, msg.Reply, err)
			return
		}

		err = json.Unmarshal(msg.Data, &request)
		if err != nil {
			log.Warning(err)
			publishProducerPutReplyError(natsConnection, msg.Reply, err)
			return
		}

		if len(request.Data) == 0 {
			err = errors.New("parameter data is empty")
			log.Warning(err)
			publishProducerPutReplyError(natsConnection, msg.Reply, err)
			return
		}

		//data, err := base64.StdEncoding.DecodeString(request.Data)
		//if err != nil {
		//	log.Warning(err)
		//	publish(natsConnection, msg.Reply, &common.ProducerPutReply{Error: err.Error(), PacketId: ""})
		//	return
		//}

		item, err := queue.Enqueue([]byte(bucketId), []byte(request.Data))
		if err != nil {
			log.Warning(err)
			publishProducerPutReplyError(natsConnection, msg.Reply, err)
			return
		}

		id := strconv.FormatUint(item.ID, 16)
		log.Infof("Reply Producer Put [PacketId: %s]", id)
		common.Publish(natsConnection, msg.Reply, &common.ProducerPutReply{Error: "", PacketId: id})
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

		log.Infof("Request Consumer Get [%s]", msg.Subject)

		bucketId, err := getBucketId(msg.Subject, natsConsumerGetSubjectPrefix)
		if err != nil {
			log.Warning(err)
			publishConsumerGetReplyError(natsConnection, msg.Reply, err)
			return
		}

		err = json.Unmarshal(msg.Data, &request)
		if err != nil {
			log.Warning(err)
			publishConsumerGetReplyError(natsConnection, msg.Reply, err)
			return
		}

		queueItem, err := queue.Peek([]byte(bucketId))
		if err != nil {
			if err == goque.ErrEmpty || err == goque.ErrOutOfBounds {
				log.Info("No data available in a queue")
				common.Publish(natsConnection, msg.Reply, &common.ConsumerGetReply{Error: "", PacketId: "", Data: ""})
				return
			}

			log.Warning(err)
			publishConsumerGetReplyError(natsConnection, msg.Reply, err)
			return
		}

		id := strconv.FormatUint(queueItem.ID, 16)
		common.Publish(natsConnection, msg.Reply, &common.ConsumerGetReply{Error: "", PacketId: id,
			Data: string(queueItem.Value)})
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
		var request common.ConsumerAckRequest

		log.Infof("Request Consumer Ack [%s]", msg.Subject)

		bucketId, err := getBucketId(msg.Subject, natsConsumerAckSubjectPrefix)
		if err != nil {
			log.Warning(err)
			publishConsumerAckReplyError(natsConnection, msg.Reply, err)
			return
		}

		err = json.Unmarshal(msg.Data, &request)
		if err != nil {
			log.Warning(err)
			publishConsumerAckReplyError(natsConnection, msg.Reply, err)
			return
		}

		queueItem, err := queue.Peek([]byte(bucketId))
		if err != nil {
			log.Warning(err)
			publishConsumerAckReplyError(natsConnection, msg.Reply, err)
			return
		}

		id, err := strconv.ParseUint(request.PacketId, 16, 64)
		if err != nil {
			log.Warning(err)
			publishConsumerAckReplyError(natsConnection, msg.Reply, err)
			return
		}

		if queueItem.ID != id {
			err = errors.New("parameter packet_id is not matching")
			log.Warning(err)
			publishConsumerAckReplyError(natsConnection, msg.Reply, err)
			return
		}

		_, err = queue.Dequeue([]byte(bucketId))
		if err != nil {
			log.Warning(err)
			publishConsumerAckReplyError(natsConnection, msg.Reply, err)
			return
		}

		log.Info("Reply Consumer Ack")
		common.Publish(natsConnection, msg.Reply, &common.ConsumerAckReply{Error: ""})
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

func publishProducerPutReplyError(connection *nats.Conn, subject string, err error) {
	log.Infof("Reply Producer Put [Error: %s]", err.Error())
	common.Publish(connection, subject, &common.ProducerPutReply{Error: err.Error(), PacketId: ""})
}

func publishConsumerGetReplyError(connection *nats.Conn, subject string, err error) {
	log.Infof("Reply Consumer Get [Error: %s]", err.Error())
	common.Publish(connection, subject, &common.ConsumerGetReply{Error: err.Error(), PacketId: "", Data: ""})
}

func publishConsumerAckReplyError(connection *nats.Conn, subject string, err error) {
	log.Infof("Reply Consumer Ack [Error: %s]", err.Error())
	common.Publish(connection, subject, &common.ConsumerAckReply{Error: err.Error()})
}

func getBucketId(actualSubject string, prefixSubject string) (string, error) {
	bucketId := strings.TrimPrefix(actualSubject, prefixSubject)
	if bucketId == actualSubject {
		err := errors.New("bucket_id was not found")
		return "", err
	}
	return bucketId, nil
}
