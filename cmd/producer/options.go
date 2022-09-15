package main

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type options struct {
	natsUrl                   string
	natsName                  string
	bucket                    string
	natsIncomingSubjectPrefix string
	natsOutgoingSubjectPrefix string
	natsPutSubjectPrefix      string
	dataRateInMs              int
	dataSizeInBytes           int
}

func parseOptions() *options {
	opt := &options{}
	opt.getOptions()
	return opt
}

func (opt *options) getOptions() {
	const natsUrlDefault = "nats://user:user@127.0.0.1:34111"
	const natsNameDefault = "NATS Queue Producer"
	const bucketDefault = "bucket1"
	const natsIncomingSubjectPrefixDefault = "leaf.incoming.data-stream"
	const natsOutgoingSubjectPrefixDefault = "leaf.outgoing.data-stream"

	viper.SetDefault("natsUrl", natsUrlDefault)
	viper.SetDefault("natsName", natsNameDefault)
	viper.SetDefault("bucket", bucketDefault)
	viper.SetDefault("natsIncomingSubjectPrefix", natsIncomingSubjectPrefixDefault)
	viper.SetDefault("natsOutgoingSubjectPrefix", natsOutgoingSubjectPrefixDefault)
	viper.SetDefault("natsPutSubjectPrefix", "producer.put")
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

	pflag.String("natsUrl", natsUrlDefault, "NATS Connection URL")
	pflag.String("natsName", natsNameDefault, "NATS Connection Name")
	pflag.String("bucket", bucketDefault, "Queue bucket name")
	pflag.String("natsIncomingSubjectPrefix", natsIncomingSubjectPrefixDefault, "NATS incoming subject prefix")
	pflag.String("natsOutgoingSubjectPrefix", natsOutgoingSubjectPrefixDefault, "NATS outgoing subject prefix")
	pflag.Parse()
	err := viper.BindPFlags(pflag.CommandLine)
	if err != nil {
		log.Fatal(err)
	}

	opt.natsUrl = viper.GetString("natsUrl")
	opt.natsName = viper.GetString("natsName")
	opt.bucket = viper.GetString("bucket")
	opt.natsIncomingSubjectPrefix = viper.GetString("natsIncomingSubjectPrefix")
	opt.natsOutgoingSubjectPrefix = viper.GetString("natsOutgoingSubjectPrefix")
	opt.natsPutSubjectPrefix = viper.GetString("natsPutSubjectPrefix")
	opt.dataRateInMs = viper.GetInt("dataRateInMs")
	opt.dataSizeInBytes = viper.GetInt("dataSizeInBytes")

	log.Infof("Options: %+v", opt)
}
