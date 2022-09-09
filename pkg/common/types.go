package common

type ProducerPutRequest struct {
	Data string `json:"data"`
}

type ProducerPutReply struct {
	PacketId string `json:"packet_id"`
	Error    string `json:"error"`
}

type ConsumerGetRequest struct {
}

type ConsumerGetReply struct {
	PacketId string `json:"packet_id"`
	Data     string `json:"data"`
	Error    string `json:"error"`
}
