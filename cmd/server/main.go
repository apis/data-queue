package main

import (
	"data-queue/pkg/common"
	"errors"
	"fmt"
	"github.com/beeker1121/goque"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"os"
	"os/signal"
	"strings"
)

func main() {
	log.Info("Starting up Data Stream Service")

	const natsNameDefault = "NATS Queue Service"
	const storagePathDefault = "./.queue"
	const natsSubjectPrefixDefault = "leaf.data-stream"

	viper.SetDefault("natsUrl", "nats://leaf_user:leaf_user@127.0.0.1:34111")
	viper.SetDefault("natsName", natsNameDefault)
	viper.SetDefault("storagePath", storagePathDefault)
	viper.SetDefault("natsSubjectPrefix", natsSubjectPrefixDefault)
	viper.SetDefault("natsProducerPutSubjectSuffix", "producer.put")
	viper.SetDefault("natsConsumerGetSubjectSuffix", "consumer.get")
	viper.SetDefault("natsConsumerAckSubjectSuffix", "consumer.ack")
	viper.SetDefault("natsConsumerAnnSubjectSuffix", "consumer.ann")
	viper.SetConfigName("server_config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("./")

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			log.Fatalf("fatal error config file: %s", fmt.Errorf("%w", err))
		}
	}

	pflag.String("natsName", natsNameDefault, "NATS connection name")
	pflag.String("storagePath", storagePathDefault, "Storage path directory")
	pflag.String("natsSubjectPrefix", natsSubjectPrefixDefault, "Storage path directory")
	pflag.Parse()
	err := viper.BindPFlags(pflag.CommandLine)
	if err != nil {
		log.Fatal(err)
	}

	natsUrl := viper.GetString("natsUrl")
	natsName := viper.GetString("natsName")
	storagePath := viper.GetString("storagePath")
	natsSubjectPrefix := viper.GetString("natsSubjectPrefix") + "."
	natsProducerPutSubjectPrefix := natsSubjectPrefix + viper.GetString("natsProducerPutSubjectSuffix") + "."
	natsProducerPutSubject := natsProducerPutSubjectPrefix + "*"
	natsConsumerGetSubjectPrefix := natsSubjectPrefix + viper.GetString("natsConsumerGetSubjectSuffix") + "."
	natsConsumerGetSubject := natsConsumerGetSubjectPrefix + "*"
	natsConsumerAckSubjectPrefix := natsSubjectPrefix + viper.GetString("natsConsumerAckSubjectSuffix") + "."
	natsConsumerAckSubject := natsConsumerAckSubjectPrefix + "*"
	natsConsumerAnnSubjectPrefix := natsSubjectPrefix + viper.GetString("natsConsumerAnnSubjectSuffix") + "."

	log.Info("Opening Persisted Queue")
	queue, err := goque.OpenPrefixQueue(storagePath)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		log.Info("Closing Persisted Queue")
		err := queue.Close()
		if err != nil {
			log.Error(err)
		}
	}()

	queueSize := queue.Length()
	log.Infof("Queue size %d", queueSize)
	if queueSize == 0 {
		log.Info("Deleting Queue")
		err = queue.Drop()
		if err != nil {
			log.Fatal(err)
		}

		log.Info("Reopening Persisted Queue")
		queue, err = goque.OpenPrefixQueue(storagePath)
		if err != nil {
			log.Fatal(err)
		}
	}

	log.Infof("Connecting to NATS '%s'", natsUrl)
	natsConnection, err := common.ConnectToNats(natsUrl, natsName)
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
