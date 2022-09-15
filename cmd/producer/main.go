package main

import (
	"data-queue/pkg/common"
	"encoding/json"
	"fmt"
	"github.com/dchest/uniuri"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"time"
)

func main() {
	log.Info("Starting up Producer Client")

	opt := parseOptions()

	natsProducerPutSubject := fmt.Sprintf("%s.%s.%s", opt.natsIncomingSubjectPrefix,
		opt.natsPutSubjectPrefix, opt.bucket)

	natsReplySubjectPrefix := opt.natsOutgoingSubjectPrefix

	log.Infof("Connecting to NATS '%s' as '%s'", opt.natsUrl, opt.natsName)
	natsConnection, err := common.ConnectToNats(opt.natsUrl, opt.natsName)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		log.Info("Closing User NATS connection")
		natsConnection.Close()
	}()

	ticker := time.NewTicker(time.Duration(opt.dataRateInMs) * time.Millisecond)
	stopTicker := make(chan bool)

	defer func() {
		stopTicker <- true
	}()

	go func() {
		for {
			select {
			case <-stopTicker:
				return
			case <-ticker.C:
				message := uniuri.NewLen(opt.dataSizeInBytes)
				request := common.ProducerPutRequest{Data: message}

				buffer, err := json.Marshal(request)
				if err != nil {
					log.Fatal(err)
				}

				log.Infof("Request Producer Put [%s]", natsProducerPutSubject)
				msg, err := common.Request(natsConnection, natsProducerPutSubject, natsReplySubjectPrefix, buffer, 3*time.Second)
				if err != nil {
					if err == nats.ErrTimeout {
						log.Error(err)
						continue
					}

					if err == nats.ErrNoResponders {
						log.Error(err)
						continue
					}

					log.Fatal(err)
				}

				var reply common.ProducerPutReply
				err = json.Unmarshal(msg.Data, &reply)
				if err != nil {
					log.Fatal(err)
				}
				log.Infof("Reply Producer Put [PacketId: %s, Error: %s]", reply.PacketId, reply.Error)
			}
		}
	}()

	exitChannel := make(chan os.Signal, 1)
	signal.Notify(exitChannel, os.Interrupt)

	<-exitChannel

	log.Info("Shutting down Producer Client")
}
