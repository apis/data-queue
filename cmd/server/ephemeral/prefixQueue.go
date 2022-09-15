package ephemeral

import (
	log "github.com/sirupsen/logrus"
	"sync"
)

type PrefixQueue struct {
	queues map[string]*Queue
	mutex  sync.Mutex
}

func NewPrefixQueue() *PrefixQueue {
	return &PrefixQueue{queues: make(map[string]*Queue)}
}

func (prefixQueue *PrefixQueue) Get(prefix string) *Queue {
	prefixQueue.mutex.Lock()
	defer prefixQueue.mutex.Unlock()

	queue, prefixExists := prefixQueue.queues[prefix]
	if !prefixExists {
		queue = newQueue()
		prefixQueue.queues[prefix] = queue
		log.Infof("PrefixQueue created: %s", prefix)
	}

	return queue
}

func (prefixQueue *PrefixQueue) TryDelete(prefix string, queue *Queue) bool {
	prefixQueue.mutex.Lock()
	defer prefixQueue.mutex.Unlock()

	if queue.Length() == 0 {
		delete(prefixQueue.queues, prefix)
		log.Infof("PrefixQueue deleted: %s", prefix)
		return true
	}

	return false
}
