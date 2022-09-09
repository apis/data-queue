package common

import (
	"encoding/json"
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
			break
		}

		log.Errorf("NATS connection failed [%v]", err)
	}

	return natsConnection, err
}

func Publish(connection *nats.Conn, subject string, data any, logMessageFormat string) {
	buffer, err := json.Marshal(data)
	if err != nil {
		log.Fatal(err)
	}

	log.Infof(logMessageFormat, string(buffer))

	err = connection.Publish(subject, buffer)
	if err != nil {
		log.Fatal(err)
	}
}
