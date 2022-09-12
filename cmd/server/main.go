package main

import (
	"data-queue/pkg/common"
	"errors"
	"fmt"
	"github.com/beeker1121/goque"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"os"
	"os/signal"
	"strings"
)

func main() {
	log.Info("Starting up Data Stream Service")

	viper.SetDefault("natsUrl", "nats://leaf_user:leaf_user@127.0.0.1:34111")
	viper.SetDefault("storagePath", "./.queue")
	viper.SetDefault("natsProducerPutSubject", "leaf.data-stream.producer.put")
	viper.SetDefault("natsConsumerGetSubject", "leaf.data-stream.consumer.get")
	viper.SetDefault("natsConsumerAckSubject", "leaf.data-stream.consumer.ack")
	viper.SetDefault("natsConsumerAnnSubject", "leaf.data-stream.consumer.ann")
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
	natsConsumerAnnSubjectPrefix := viper.GetString("natsConsumerAnnSubject") + "."

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
	producerPutSubjectSubscription, err := natsConnection.Subscribe(natsProducerPutSubject,
		getProducerPutHandler(natsProducerPutSubjectPrefix, natsConsumerAnnSubjectPrefix, natsConnection, queue))
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
	consumerGetSubjectSubscription, err := natsConnection.Subscribe(natsConsumerGetSubject,
		getConsumerGetHandler(natsConsumerGetSubjectPrefix, natsConnection, queue))
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
	consumerAckSubjectSubscription, err := natsConnection.Subscribe(natsConsumerAckSubject,
		getConsumerAckHandler(natsConsumerAckSubjectPrefix, natsConnection, queue))
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

func getBucketId(actualSubject string, prefixSubject string) (string, error) {
	bucketId := strings.TrimPrefix(actualSubject, prefixSubject)
	if bucketId == actualSubject {
		err := errors.New("bucket_id was not found")
		return "", err
	}
	return bucketId, nil
}
