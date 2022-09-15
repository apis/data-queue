package main

import "fmt"
import log "github.com/sirupsen/logrus"

type subjectOptions struct {
	natsProducerPutSubjectPrefix           string
	natsProducerPutSubject                 string
	natsPersistentConsumerGetSubjectPrefix string
	natsPersistentConsumerGetSubject       string
	natsPersistentConsumerAckSubjectPrefix string
	natsPersistentConsumerAckSubject       string
	natsPersistentConsumerAnnSubjectPrefix string
	natsEphemeralConsumerGetSubjectPrefix  string
	natsEphemeralConsumerGetSubject        string
	natsEphemeralConsumerAckSubjectPrefix  string
	natsEphemeralConsumerAckSubject        string
	natsEphemeralConsumerAnnSubjectPrefix  string
}

func parseSubjectOptions(opt *options) *subjectOptions {
	sOpt := &subjectOptions{}
	sOpt.getSubjectOptions(opt)
	return sOpt
}

func (sOpt *subjectOptions) getSubjectOptions(opt *options) {
	sOpt.natsProducerPutSubjectPrefix = fmt.Sprintf("%s.%s.%s.", opt.natsIncomingSubjectPrefix,
		opt.natsProducerSubjectPrefix, opt.natsPutSubjectSuffix)
	sOpt.natsProducerPutSubject = fmt.Sprintf("%s*", sOpt.natsProducerPutSubjectPrefix)

	sOpt.natsPersistentConsumerGetSubjectPrefix = fmt.Sprintf("%s.%s.%s.%s.", opt.natsIncomingSubjectPrefix,
		opt.natsConsumerSubjectPrefix, opt.natsPersistentSubjectPrefix, opt.natsGetSubjectSuffix)
	sOpt.natsPersistentConsumerGetSubject = fmt.Sprintf("%s*", sOpt.natsPersistentConsumerGetSubjectPrefix)

	sOpt.natsPersistentConsumerAckSubjectPrefix = fmt.Sprintf("%s.%s.%s.%s.", opt.natsIncomingSubjectPrefix,
		opt.natsConsumerSubjectPrefix, opt.natsPersistentSubjectPrefix, opt.natsAckSubjectSuffix)
	sOpt.natsPersistentConsumerAckSubject = fmt.Sprintf("%s*", sOpt.natsPersistentConsumerAckSubjectPrefix)

	sOpt.natsPersistentConsumerAnnSubjectPrefix = fmt.Sprintf("%s.%s.%s.%s.", opt.natsOutgoingSubjectPrefix,
		opt.natsConsumerSubjectPrefix, opt.natsPersistentSubjectPrefix, opt.natsAnnSubjectSuffix)

	sOpt.natsEphemeralConsumerGetSubjectPrefix = fmt.Sprintf("%s.%s.%s.%s.", opt.natsIncomingSubjectPrefix,
		opt.natsConsumerSubjectPrefix, opt.natsEphemeralSubjectPrefix, opt.natsGetSubjectSuffix)
	sOpt.natsEphemeralConsumerGetSubject = fmt.Sprintf("%s*", sOpt.natsEphemeralConsumerGetSubjectPrefix)

	sOpt.natsEphemeralConsumerAckSubjectPrefix = fmt.Sprintf("%s.%s.%s.%s.", opt.natsIncomingSubjectPrefix,
		opt.natsConsumerSubjectPrefix, opt.natsEphemeralSubjectPrefix, opt.natsAckSubjectSuffix)
	sOpt.natsEphemeralConsumerAckSubject = fmt.Sprintf("%s*", sOpt.natsEphemeralConsumerAckSubjectPrefix)

	sOpt.natsEphemeralConsumerAnnSubjectPrefix = fmt.Sprintf("%s.%s.%s.%s.", opt.natsOutgoingSubjectPrefix,
		opt.natsConsumerSubjectPrefix, opt.natsEphemeralSubjectPrefix, opt.natsAnnSubjectSuffix)

	log.Infof("Configuration: %+v", sOpt)
}
