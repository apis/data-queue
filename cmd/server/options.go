package main

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type options struct {
	natsUrl                     string
	natsName                    string
	storagePath                 string
	natsIncomingSubjectPrefix   string
	natsOutgoingSubjectPrefix   string
	natsProducerSubjectPrefix   string
	natsConsumerSubjectPrefix   string
	natsEphemeralSubjectPrefix  string
	natsPersistentSubjectPrefix string
	natsPutSubjectSuffix        string
	natsGetSubjectSuffix        string
	natsAckSubjectSuffix        string
	natsAnnSubjectSuffix        string
}

func parseOptions() *options {
	opt := &options{}
	opt.getOptions()
	return opt
}

func (opt *options) getOptions() {
	const natsUrlDefault = "nats://user:user@127.0.0.1:34111"
	const natsNameDefault = "NATS Queue Service"
	const storagePathDefault = "./.queue"
	const natsIncomingSubjectPrefixDefault = "leaf.incoming.data-stream"
	const natsOutgoingSubjectPrefixDefault = "leaf.outgoing.data-stream"

	viper.SetDefault("natsUrl", natsUrlDefault)
	viper.SetDefault("natsName", natsNameDefault)
	viper.SetDefault("storagePath", storagePathDefault)
	viper.SetDefault("natsIncomingSubjectPrefix", natsIncomingSubjectPrefixDefault)
	viper.SetDefault("natsOutgoingSubjectPrefix", natsOutgoingSubjectPrefixDefault)
	viper.SetDefault("natsProducerSubjectPrefix", "producer")
	viper.SetDefault("natsConsumerSubjectPrefix", "consumer")
	viper.SetDefault("natsEphemeralSubjectPrefix", "ephemeral")
	viper.SetDefault("natsPersistentSubjectPrefix", "persistent")
	viper.SetDefault("natsPutSubjectSuffix", "put")
	viper.SetDefault("natsGetSubjectSuffix", "get")
	viper.SetDefault("natsAckSubjectSuffix", "ack")
	viper.SetDefault("natsAnnSubjectSuffix", "ann")

	viper.SetConfigName("server_config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("./")

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			log.Fatalf("fatal error config file: %s", fmt.Errorf("%w", err))
		}
	}

	pflag.String("natsUrl", natsNameDefault, "NATS connection URL")
	pflag.String("natsName", natsNameDefault, "NATS connection name")
	pflag.String("storagePath", storagePathDefault, "Storage path directory")
	pflag.String("natsIncomingSubjectPrefix", natsIncomingSubjectPrefixDefault, "NATS incoming subject prefix")
	pflag.String("natsOutgoingSubjectPrefix", natsOutgoingSubjectPrefixDefault, "NATS outgoing subject prefix")
	pflag.Parse()
	err := viper.BindPFlags(pflag.CommandLine)
	if err != nil {
		log.Fatal(err)
	}

	opt.natsUrl = viper.GetString("natsUrl")
	opt.natsName = viper.GetString("natsName")
	opt.storagePath = viper.GetString("storagePath")
	opt.natsIncomingSubjectPrefix = viper.GetString("natsIncomingSubjectPrefix")
	opt.natsOutgoingSubjectPrefix = viper.GetString("natsOutgoingSubjectPrefix")
	opt.natsProducerSubjectPrefix = viper.GetString("natsProducerSubjectPrefix")
	opt.natsConsumerSubjectPrefix = viper.GetString("natsConsumerSubjectPrefix")
	opt.natsEphemeralSubjectPrefix = viper.GetString("natsEphemeralSubjectPrefix")
	opt.natsPersistentSubjectPrefix = viper.GetString("natsPersistentSubjectPrefix")
	opt.natsPutSubjectSuffix = viper.GetString("natsPutSubjectSuffix")
	opt.natsGetSubjectSuffix = viper.GetString("natsGetSubjectSuffix")
	opt.natsAckSubjectSuffix = viper.GetString("natsAckSubjectSuffix")
	opt.natsAnnSubjectSuffix = viper.GetString("natsAnnSubjectSuffix")

	log.Infof("Options: %+v", opt)
}
