package common

import (
	"encoding/json"
	"fmt"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"time"
)

func ConnectToNats(natsUserUrl string, connectionName string) (*nats.Conn, error) {
	options := nats.Options{
		Url:  natsUserUrl,
		Name: connectionName,
	}

	var natsConnection *nats.Conn
	var err error

	for index := 0; index < 5; index++ {
		if index > 0 {
			time.Sleep(time.Second)
		}

		log.Info("Attempting to connect to NATS")
		natsConnection, err = options.Connect()
		if err == nil {
			log.Info("Connected to NATS")
			break
		}

		log.Errorf("NATS connection failed [%v]", err)
	}

	return natsConnection, err
}

func Publish(connection *nats.Conn, subject string, data any) {
	buffer, err := json.Marshal(data)
	if err != nil {
		log.Fatal(err)
	}

	err = connection.Publish(subject, buffer)
	if err != nil {
		log.Fatal(err)
	}
}

func Request(natsConnection *nats.Conn, subject string, requestInboxPrefix string, data []byte, timeout time.Duration) (*nats.Msg, error) {
	if natsConnection == nil {
		return nil, nats.ErrInvalidConnection
	}

	reply := fmt.Sprintf("%s.%s", requestInboxPrefix, nats.NewInbox())
	//reply := nats.NewInbox()
	log.Infof("Inbox: %s", reply)

	subscription, err := natsConnection.SubscribeSync(reply)
	if err != nil {
		return nil, err
	}

	defer func() {
		err = subscription.Unsubscribe()
		if err != nil {
			log.Errorf("Unsubscribe: %s", fmt.Errorf("%w", err))
		}
	}()

	if err := natsConnection.PublishRequest(subject, reply, data); err != nil {
		return nil, err
	}

	msg, err := subscription.NextMsg(timeout)
	if err != nil {
		return nil, err
	}

	//log.Infof("Reply: %s", msg.Data)
	return msg, nil
}
