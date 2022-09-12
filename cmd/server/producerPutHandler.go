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

func getProducerPutHandler(natsProducerPutSubjectPrefix string, natsConnection *nats.Conn,
	queue *goque.PrefixQueue) func(msg *nats.Msg) {
	return func(msg *nats.Msg) {
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
	}
}

func publishProducerPutReplyError(connection *nats.Conn, subject string, err error) {
	log.Infof("Reply Producer Put [Error: %s]", err.Error())
	common.Publish(connection, subject, &common.ProducerPutReply{Error: err.Error(), PacketId: ""})
}
