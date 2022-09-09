package common

type ProducerPutRequest struct {
	BucketId string `json:"bucket_id"`
	Data     string `json:"data"`
}

type ProducerPutReply struct {
	PacketId string `json:"packet_id"`
	Error    string `json:"error"`
}

type ConsumerGetRequest struct {
	BucketId string `json:"bucket_id"`
}

type ConsumerGetReply struct {
	PacketId string `json:"packet_id"`
	Data     string `json:"data"`
	Error    string `json:"error"`
}
