package main

import (
	"data-queue/cmd/server/ephemeral"
	"data-queue/pkg/common"
	"encoding/json"
	"errors"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"strconv"
)

func consumerEphemeralAckHandler(natsConsumerAckSubjectPrefix string, natsConnection *nats.Conn, queue *ephemeral.Queue) func(msg *nats.Msg) {
	return func(msg *nats.Msg) {
		var request common.ConsumerAckRequest

		log.Infof("Request Consumer Ack [%s]", msg.Subject)

		// TODO use bucket!!!
		_, err := getBucketId(msg.Subject, natsConsumerAckSubjectPrefix)
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

		_, itemId, err := queue.Peek()
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

		if itemId != id {
			err = errors.New("parameter packet_id is not matching")
			log.Error(err)
			publishConsumerAckReplyError(natsConnection, msg.Reply, err)
			return
		}

		_, _, err = queue.Dequeue()
		if err != nil {
			log.Error(err)
			publishConsumerAckReplyError(natsConnection, msg.Reply, err)
			return
		}

		log.Info("Reply Consumer Ack")
		common.Publish(natsConnection, msg.Reply, &common.ConsumerAckReply{Error: ""})
	}
}
