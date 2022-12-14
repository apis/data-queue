package main

import (
	"data-queue/cmd/server/ephemeral"
	"data-queue/pkg/common"
	"encoding/json"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"strconv"
)

func consumerEphemeralGetHandler(natsConsumerGetSubjectPrefix string, natsConnection *nats.Conn,
	ephemeralPrefixQueue *ephemeral.PrefixQueue) func(msg *nats.Msg) {
	return func(msg *nats.Msg) {
		var request common.ConsumerGetRequest

		log.Infof("Request Consumer Get [%s]", msg.Subject)

		bucketId, err := getBucketId(msg.Subject, natsConsumerGetSubjectPrefix)
		if err != nil {
			log.Error(err)
			publishConsumerGetReplyError(natsConnection, msg.Reply, err)
			return
		}

		err = json.Unmarshal(msg.Data, &request)
		if err != nil {
			log.Error(err)
			publishConsumerGetReplyError(natsConnection, msg.Reply, err)
			return
		}

		ephemeralQueue := ephemeralPrefixQueue.Get(bucketId)

		itemContent, itemId, err := ephemeralQueue.Peek()
		if err != nil {
			log.Info("No data available in a queue")
			common.Publish(natsConnection, msg.Reply, &common.ConsumerGetReply{Error: "", PacketId: "", Data: ""})
			return
		}

		id := strconv.FormatUint(itemId, 16)
		common.Publish(natsConnection, msg.Reply, &common.ConsumerGetReply{Error: "", PacketId: id,
			Data: itemContent})
	}
}
