package main

import (
	"data-queue/pkg/common"
	"encoding/json"
	"github.com/beeker1121/goque"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"strconv"
)

func getConsumerGetHandler(natsConsumerGetSubjectPrefix string, natsConnection *nats.Conn, queue *goque.PrefixQueue) func(msg *nats.Msg) {
	return func(msg *nats.Msg) {
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
	}
}

func publishConsumerGetReplyError(connection *nats.Conn, subject string, err error) {
	log.Infof("Reply Consumer Get [Error: %s]", err.Error())
	common.Publish(connection, subject, &common.ConsumerGetReply{Error: err.Error(), PacketId: "", Data: ""})
}
