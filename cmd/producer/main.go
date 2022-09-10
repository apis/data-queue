package main

import (
	"data-queue/pkg/common"
	"encoding/json"
	"fmt"
	"github.com/dchest/uniuri"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"os"
	"os/signal"
	"time"
)

func main() {
	log.Info("Starting up Producer Client")

	viper.SetDefault("natsUrl", "nats://leaf_user:leaf_user@127.0.0.1:34111")
	viper.SetDefault("natsName", "producer1")
	viper.SetDefault("natsProducerPutSubject", "leaf.data-stream.producer.put")
	viper.SetDefault("bucket", "bucket1")
	viper.SetDefault("dataRateInMs", 250)
	viper.SetDefault("dataSizeInBytes", 1024)
	viper.SetConfigName("producer_config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			log.Fatalf("fatal error config file: %s", fmt.Errorf("%w", err))
		}
	}

	pflag.String("natsName", "producer2", "NATS Connection Name")
	pflag.String("bucket", "bucket2", "Queue bucket name")
	pflag.Parse()
	err := viper.BindPFlags(pflag.CommandLine)
	if err != nil {
		log.Fatal(err)
	}

	natsUrl := viper.GetString("natsUrl")
	natsName := viper.GetString("natsName")
	bucket := viper.GetString("bucket")
	dataRateInMs := viper.GetInt("dataRateInMs")
	dataSizeInBytes := viper.GetInt("dataSizeInBytes")
	natsProducerPutSubject := viper.GetString("natsProducerPutSubject") + "." + bucket

	log.Infof("Connecting to NATS '%s' as '%s'", natsUrl, natsName)
	natsConnection, err := common.ConnectToNats(natsUrl, natsName)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		log.Info("Closing User NATS connection")
		natsConnection.Close()
	}()

	ticker := time.NewTicker(time.Duration(dataRateInMs) * time.Millisecond)
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
				//data := base64.StdEncoding.EncodeToString([]byte(message))
				message := uniuri.NewLen(dataSizeInBytes)
				request := common.ProducerPutRequest{Data: message}

				buffer, err := json.Marshal(request)
				if err != nil {
					log.Fatal(err)
				}

				log.Infof("Request Producer Put [%s]", natsProducerPutSubject)
				msg, err := natsConnection.Request(natsProducerPutSubject, buffer, 3*time.Second)
				if err != nil {
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
