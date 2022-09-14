package main

import (
	"data-queue/pkg/common"
	"encoding/json"
	"errors"
	"github.com/beeker1121/goque"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"strconv"
)

func consumerPersistentAckHandler(natsConsumerAckSubjectPrefix string, natsConnection *nats.Conn, queue *goque.PrefixQueue) func(msg *nats.Msg) {
	return func(msg *nats.Msg) {
		var request common.ConsumerAckRequest

		log.Infof("Request Consumer Ack [%s]", msg.Subject)

		bucketId, err := getBucketId(msg.Subject, natsConsumerAckSubjectPrefix)
		if err != nil {
			log.Error(err)
			publishConsumerAckReplyError(natsConnection, msg.Reply, err)
			return
		}

		err = json.Unmarshal(msg.Data, &request)
		if err != nil {
			log.Error(err)
			publishConsumerAckReplyError(natsConnection, msg.Reply, err)
			return
		}

		queueItem, err := queue.Peek([]byte(bucketId))
		if err != nil {
			log.Error(err)
			publishConsumerAckReplyError(natsConnection, msg.Reply, err)
			return
		}

		id, err := strconv.ParseUint(request.PacketId, 16, 64)
		if err != nil {
			log.Error(err)
			publishConsumerAckReplyError(natsConnection, msg.Reply, err)
			return
		}

		if queueItem.ID != id {
			err = errors.New("parameter packet_id is not matching")
			log.Error(err)
			publishConsumerAckReplyError(natsConnection, msg.Reply, err)
			return
		}

		_, err = queue.Dequeue([]byte(bucketId))
		if err != nil {
			log.Error(err)
			publishConsumerAckReplyError(natsConnection, msg.Reply, err)
			return
		}

		log.Info("Reply Consumer Ack")
		common.Publish(natsConnection, msg.Reply, &common.ConsumerAckReply{Error: ""})
	}
}
