package main

import (
	"data-queue/cmd/server/ephemeral"
	"data-queue/pkg/common"
	"errors"
	"fmt"
	"github.com/beeker1121/goque"
	"github.com/nats-io/nats.go"
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
	viper.SetDefault("natsProducerSubjectPrefix", "producer")
	viper.SetDefault("natsConsumerSubjectPrefix", "consumer")
	viper.SetDefault("natsEphemeralSubjectPrefix", "ephemeral")
	viper.SetDefault("natsPersistentSubjectPrefix", "persistent")
	viper.SetDefault("natsPutSubjectSuffix", "put")
	viper.SetDefault("natsGetSubjectSuffix", "get")
	viper.SetDefault("natsAckSubjectSuffix", "ack")
	viper.SetDefault("natsAnnSubjectSuffix", "ann")

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
	pflag.String("natsSubjectPrefix", natsSubjectPrefixDefault, "NATS subject prefix")
	pflag.Parse()
	err := viper.BindPFlags(pflag.CommandLine)
	if err != nil {
		log.Fatal(err)
	}

	natsUrl := viper.GetString("natsUrl")
	natsName := viper.GetString("natsName")
	storagePath := viper.GetString("storagePath")
	natsSubjectPrefix := viper.GetString("natsSubjectPrefix")
	natsProducerSubjectPrefix := viper.GetString("natsProducerSubjectPrefix")
	natsConsumerSubjectPrefix := viper.GetString("natsConsumerSubjectPrefix")
	natsEphemeralSubjectPrefix := viper.GetString("natsEphemeralSubjectPrefix")
	natsPersistentSubjectPrefix := viper.GetString("natsPersistentSubjectPrefix")
	natsPutSubjectSuffix := viper.GetString("natsPutSubjectSuffix")
	natsGetSubjectSuffix := viper.GetString("natsGetSubjectSuffix")
	natsAckSubjectSuffix := viper.GetString("natsAckSubjectSuffix")
	natsAnnSubjectSuffix := viper.GetString("natsAnnSubjectSuffix")

	natsProducerPutSubjectPrefix := fmt.Sprintf("%s.%s.%s.", natsSubjectPrefix, natsProducerSubjectPrefix, natsPutSubjectSuffix)
	natsProducerPutSubject := fmt.Sprintf("%s*", natsProducerPutSubjectPrefix)

	natsPersistentConsumerGetSubjectPrefix := fmt.Sprintf("%s.%s.%s.%s.", natsSubjectPrefix, natsConsumerSubjectPrefix, natsPersistentSubjectPrefix, natsGetSubjectSuffix)
	natsPersistentConsumerGetSubject := fmt.Sprintf("%s*", natsPersistentConsumerGetSubjectPrefix)

	natsPersistentConsumerAckSubjectPrefix := fmt.Sprintf("%s.%s.%s.%s.", natsSubjectPrefix, natsConsumerSubjectPrefix, natsPersistentSubjectPrefix, natsAckSubjectSuffix)
	natsPersistentConsumerAckSubject := fmt.Sprintf("%s*", natsPersistentConsumerAckSubjectPrefix)

	natsPersistentConsumerAnnSubjectPrefix := fmt.Sprintf("%s.%s.%s.%s.", natsSubjectPrefix, natsConsumerSubjectPrefix, natsPersistentSubjectPrefix, natsAnnSubjectSuffix)

	natsEphemeralConsumerGetSubjectPrefix := fmt.Sprintf("%s.%s.%s.%s.", natsSubjectPrefix, natsConsumerSubjectPrefix, natsEphemeralSubjectPrefix, natsGetSubjectSuffix)
	natsEphemeralConsumerGetSubject := fmt.Sprintf("%s*", natsEphemeralConsumerGetSubjectPrefix)

	natsEphemeralConsumerAckSubjectPrefix := fmt.Sprintf("%s.%s.%s.%s.", natsSubjectPrefix, natsConsumerSubjectPrefix, natsEphemeralSubjectPrefix, natsAckSubjectSuffix)
	natsEphemeralConsumerAckSubject := fmt.Sprintf("%s*", natsEphemeralConsumerAckSubjectPrefix)

	natsEphemeralConsumerAnnSubjectPrefix := fmt.Sprintf("%s.%s.%s.%s.", natsSubjectPrefix, natsConsumerSubjectPrefix, natsEphemeralSubjectPrefix, natsAnnSubjectSuffix)

	ephemeralQueue := ephemeral.New()

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
		producerPutHandler(natsProducerPutSubjectPrefix, natsPersistentConsumerAnnSubjectPrefix, natsEphemeralConsumerAnnSubjectPrefix, natsConnection, queue))
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

	log.Infof("Subscribing to '%s'", natsPersistentConsumerGetSubject)
	persistentConsumerGetSubjectSubscription, err := natsConnection.Subscribe(natsPersistentConsumerGetSubject,
		consumerPersistentGetHandler(natsPersistentConsumerGetSubjectPrefix, natsConnection, queue))
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		log.Infof("Unsubscribing from '%s'", natsPersistentConsumerGetSubject)

		err = persistentConsumerGetSubjectSubscription.Unsubscribe()
		if err != nil {
			log.Error(err)
		}
	}()

	log.Infof("Subscribing to '%s'", natsEphemeralConsumerGetSubject)
	ephemeralConsumerGetSubjectSubscription, err := natsConnection.Subscribe(natsEphemeralConsumerGetSubject,
		consumerEphemeralGetHandler(natsEphemeralConsumerGetSubjectPrefix, natsConnection, ephemeralQueue))
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		log.Infof("Unsubscribing from '%s'", natsEphemeralConsumerGetSubject)

		err = ephemeralConsumerGetSubjectSubscription.Unsubscribe()
		if err != nil {
			log.Error(err)
		}
	}()

	log.Infof("Subscribing to '%s'", natsPersistentConsumerAckSubject)
	persistentConsumerAckSubjectSubscription, err := natsConnection.Subscribe(natsPersistentConsumerAckSubject,
		consumerPersistentAckHandler(natsPersistentConsumerAckSubjectPrefix, natsConnection, queue))
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		log.Infof("Unsubscribing from '%s'", natsPersistentConsumerAckSubject)

		err = persistentConsumerAckSubjectSubscription.Unsubscribe()
		if err != nil {
			log.Error(err)
		}
	}()

	log.Infof("Subscribing to '%s'", natsEphemeralConsumerAckSubject)
	ephemeralConsumerAckSubjectSubscription, err := natsConnection.Subscribe(natsEphemeralConsumerAckSubject,
		consumerEphemeralAckHandler(natsEphemeralConsumerAckSubjectPrefix, natsConnection, ephemeralQueue))
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		log.Infof("Unsubscribing from '%s'", natsEphemeralConsumerAckSubject)

		err = ephemeralConsumerAckSubjectSubscription.Unsubscribe()
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

func publishConsumerGetReplyError(connection *nats.Conn, subject string, err error) {
	log.Infof("Reply Consumer Get [Error: %s]", err.Error())
	common.Publish(connection, subject, &common.ConsumerGetReply{Error: err.Error(), PacketId: "", Data: ""})
}

func publishConsumerAckReplyError(connection *nats.Conn, subject string, err error) {
	log.Infof("Reply Consumer Ack [Error: %s]", err.Error())
	common.Publish(connection, subject, &common.ConsumerAckReply{Error: err.Error()})
}
