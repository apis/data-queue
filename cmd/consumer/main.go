package main

import (
	"context"
	"data-queue/pkg/common"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"os"
	"os/signal"
	"time"
)

func main() {
	log.Info("Starting up Consumer Client")

	viper.SetDefault("natsUrl", "nats://leaf_user:leaf_user@127.0.0.1:34111")
	viper.SetDefault("natsName", "consumer1")
	viper.SetDefault("natsConsumerGetSubject", "leaf.data-stream.consumer.get")
	viper.SetDefault("natsConsumerAckSubject", "leaf.data-stream.consumer.ack")
	viper.SetDefault("bucket", "bucket1")
	viper.SetConfigName("consumer_config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			log.Fatalf("fatal error config file: %s", fmt.Errorf("%w", err))
		}
	}

	pflag.String("natsName", "consumer2", "NATS Connection Name")
	pflag.String("bucket", "bucket2", "Queue bucket name")
	pflag.Parse()
	err := viper.BindPFlags(pflag.CommandLine)
	if err != nil {
		log.Fatal(err)
	}

	natsUrl := viper.GetString("natsUrl")
	natsName := viper.GetString("natsName")
	bucket := viper.GetString("bucket")
	natsConsumerGetSubject := viper.GetString("natsConsumerGetSubject") + "." + bucket
	natsConsumerAckSubject := viper.GetString("natsConsumerAckSubject") + "." + bucket

	log.Infof("Connecting to NATS '%s' as '%s'", natsUrl, natsName)
	natsConnection, err := common.ConnectToNats(natsUrl, natsName)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		log.Info("Closing User NATS connection")
		natsConnection.Close()
	}()

	jumpToNextItem := make(chan bool, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	jumpToNextItem <- true

	go func() {
		for {
			select {
			case <-jumpToNextItem:
				log.Info("-> Next")
				jumpToNext := processItem(natsConnection, natsConsumerGetSubject, natsConsumerAckSubject)
				if jumpToNext {
					jumpToNextItem <- true
				}
			case <-ctx.Done():
				log.Info("-> Done")
				return
			case <-time.After(time.Second):
				log.Info("-> Time")
				jumpToNext := processItem(natsConnection, natsConsumerGetSubject, natsConsumerAckSubject)
				if jumpToNext {
					jumpToNextItem <- true
				}
			}
		}
	}()

	exitChannel := make(chan os.Signal, 1)
	signal.Notify(exitChannel, os.Interrupt)

	<-exitChannel

	log.Info("Shutting down Consumer Client")
}

func processItem(natsConnection *nats.Conn, natsConsumerGetSubject string, natsConsumerAckSubject string) bool {
	request := common.ConsumerGetRequest{}
	buffer, err := json.Marshal(request)
	if err != nil {
		log.Fatal(err)
	}

	log.Infof("Request Consumer Get [%s] [%s]", natsConsumerGetSubject, string(buffer))
	msg, err := natsConnection.Request(natsConsumerGetSubject, buffer, 3*time.Second)
	if err != nil {
		log.Error(err)
		return false
	}

	var reply common.ConsumerGetReply
	err = json.Unmarshal(msg.Data, &reply)
	if err != nil {
		log.Error(err)
		return false
	}
	log.Infof("Reply Consumer Get [%s]", string(msg.Data))

	if reply.Error != "" {
		err = errors.New(reply.Error)
		log.Error(err)
		return false
	}

	if reply.Data == "" {
		log.Infof("No new data")
		return false
	}

	return true
}
