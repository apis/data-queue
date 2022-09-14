package ephemeral

import (
	"container/list"
	"errors"
	"sync"
)

type queueItem struct {
	Content string
	Id      uint64
}

type Queue struct {
	list   *list.List
	mutex  sync.Mutex
	lastId uint64
}

func New() *Queue {
	return &Queue{list: list.New(), lastId: 0}
}

func (queue *Queue) Enqueue(content string) uint64 {
	queue.mutex.Lock()
	defer queue.mutex.Unlock()

	queue.lastId = queue.lastId + 1
	queue.list.PushBack(&queueItem{Id: queue.lastId, Content: content})
	return queue.lastId
}

func (queue *Queue) Dequeue() (content string, id uint64, err error) {
	queue.mutex.Lock()
	defer queue.mutex.Unlock()

	return queue.peekWithDelete(true)
}

func (queue *Queue) Peek() (content string, id uint64, err error) {
	queue.mutex.Lock()
	defer queue.mutex.Unlock()

	return queue.peekWithDelete(false)
}

func (queue *Queue) peekWithDelete(delete bool) (string, uint64, error) {
	front := queue.list.Front()
	if front == nil {
		return "", 0, errors.New("queue is empty")
	}
	item := front.Value.(*queueItem)

	if delete {
		queue.list.Remove(front)
	}
	return item.Content, item.Id, nil
}
