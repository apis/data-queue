package main

import (
	"data-queue/cmd/server/ephemeral"
	"data-queue/pkg/common"
	"errors"
	"github.com/beeker1121/goque"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"strings"
)

func main() {
	log.Info("Starting up Data Stream Service")

	opt := parseOptions()
	sOpt := parseSubjectOptions(opt)

	ephemeralPrefixQueue := ephemeral.NewPrefixQueue()

	log.Info("Opening Persisted Queue")
	queue, err := goque.OpenPrefixQueue(opt.storagePath)
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
		queue, err = goque.OpenPrefixQueue(opt.storagePath)
		if err != nil {
			log.Fatal(err)
		}
	}

	log.Infof("Connecting to NATS '%s'", opt.natsUrl)
	natsConnection, err := common.ConnectToNats(opt.natsUrl, opt.natsName)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		log.Info("Closing User NATS connection")
		natsConnection.Close()
	}()

	log.Infof("Subscribing to '%s'", sOpt.natsProducerPutSubject)
	producerPutSubjectSubscription, err := natsConnection.Subscribe(sOpt.natsProducerPutSubject,
		producerPutHandler(sOpt.natsProducerPutSubjectPrefix, sOpt.natsPersistentConsumerAnnSubjectPrefix,
			sOpt.natsEphemeralConsumerAnnSubjectPrefix, natsConnection, queue, ephemeralPrefixQueue))
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		log.Infof("Unsubscribing from '%s'", sOpt.natsProducerPutSubject)

		err = producerPutSubjectSubscription.Unsubscribe()
		if err != nil {
			log.Error(err)
		}
	}()

	log.Infof("Subscribing to '%s'", sOpt.natsPersistentConsumerGetSubject)
	persistentConsumerGetSubjectSubscription, err := natsConnection.Subscribe(sOpt.natsPersistentConsumerGetSubject,
		consumerPersistentGetHandler(sOpt.natsPersistentConsumerGetSubjectPrefix, natsConnection, queue))
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		log.Infof("Unsubscribing from '%s'", sOpt.natsPersistentConsumerGetSubject)

		err = persistentConsumerGetSubjectSubscription.Unsubscribe()
		if err != nil {
			log.Error(err)
		}
	}()

	log.Infof("Subscribing to '%s'", sOpt.natsEphemeralConsumerGetSubject)
	ephemeralConsumerGetSubjectSubscription, err := natsConnection.Subscribe(sOpt.natsEphemeralConsumerGetSubject,
		consumerEphemeralGetHandler(sOpt.natsEphemeralConsumerGetSubjectPrefix, natsConnection, ephemeralPrefixQueue))
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		log.Infof("Unsubscribing from '%s'", sOpt.natsEphemeralConsumerGetSubject)

		err = ephemeralConsumerGetSubjectSubscription.Unsubscribe()
		if err != nil {
			log.Error(err)
		}
	}()

	log.Infof("Subscribing to '%s'", sOpt.natsPersistentConsumerAckSubject)
	persistentConsumerAckSubjectSubscription, err := natsConnection.Subscribe(sOpt.natsPersistentConsumerAckSubject,
		consumerPersistentAckHandler(sOpt.natsPersistentConsumerAckSubjectPrefix, natsConnection, queue))
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		log.Infof("Unsubscribing from '%s'", sOpt.natsPersistentConsumerAckSubject)

		err = persistentConsumerAckSubjectSubscription.Unsubscribe()
		if err != nil {
			log.Error(err)
		}
	}()

	log.Infof("Subscribing to '%s'", sOpt.natsEphemeralConsumerAckSubject)
	ephemeralConsumerAckSubjectSubscription, err := natsConnection.Subscribe(sOpt.natsEphemeralConsumerAckSubject,
		consumerEphemeralAckHandler(sOpt.natsEphemeralConsumerAckSubjectPrefix, natsConnection, ephemeralPrefixQueue))
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		log.Infof("Unsubscribing from '%s'", sOpt.natsEphemeralConsumerAckSubject)

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
