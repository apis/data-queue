package main

import (
	"data-queue/pkg/common"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"os"
	"os/signal"
	"time"
)

func main() {
	log.Info("Starting up Producer Client")

	viper.SetDefault("natsUrl", "nats://leaf_user:leaf_user@127.0.0.1:34111")
	viper.SetDefault("natsProducerPutSubject", "leaf.data-stream.producer.put")
	viper.SetDefault("producerBucket", "bucket1")
	viper.SetConfigName("producer_config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			log.Fatalf("fatal error config file: %s", fmt.Errorf("%w", err))
		}
	}
	natsUrl := viper.GetString("natsUrl")
	producerBucket := viper.GetString("producerBucket")
	natsProducerPutSubject := viper.GetString("natsProducerPutSubject") + "." + producerBucket

	log.Infof("Connecting to NATS '%s'", natsUrl)
	natsConnection, err := common.ConnectToNats(natsUrl, "Data Stream Service Connection")
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		log.Info("Closing User NATS connection")
		natsConnection.Close()
	}()

	ticker := time.NewTicker(3 * time.Second)
	stopTicker := make(chan bool)

	defer func() {
		stopTicker <- true
	}()

	go func() {
		for {
			select {
			case <-stopTicker:
				return
			case t := <-ticker.C:
				message := t.Format(time.RFC3339)
				//data := base64.StdEncoding.EncodeToString([]byte(message))
				request := common.ProducerPutRequest{Data: message}

				buffer, err := json.Marshal(request)
				if err != nil {
					log.Fatal(err)
				}

				log.Infof("Request Producer Put: %s", string(buffer))
				msg, err := natsConnection.Request(natsProducerPutSubject, buffer, 3*time.Second)
				if err != nil {
					log.Fatal(err)
				}

				var reply common.ProducerPutReply
				err = json.Unmarshal(msg.Data, &reply)
				if err != nil {
					log.Fatal(err)
				}
				log.Infof("Reply Producer Put: %s", string(msg.Data))
			}
		}
	}()

	exitChannel := make(chan os.Signal, 1)
	signal.Notify(exitChannel, os.Interrupt)

	<-exitChannel

	log.Info("Shutting down backend service")
}
