package engine_util

import (
	"encoding/json"

	"github.com/Connor1996/badger"
)

// 无状态队列
type Queue struct {
	db        *badger.DB
	queueName string
}

// 创建队列
func NewQueue(path string, name string) (*Queue, error) {
	opts := badger.DefaultOptions
	opts.Dir = path
	opts.ValueDir = opts.Dir
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &Queue{db: db, queueName: name}, nil
}

// 获取队列
func (q *Queue) getQueue(txn *badger.Txn) ([]string, error) {
	item, err := txn.Get([]byte(q.queueName))
	if err == badger.ErrKeyNotFound {
		return []string{}, nil // 队列为空
	} else if err != nil {
		return nil, err
	}

	var queue []string
	data, _ := item.ValueCopy(nil)
	err = json.Unmarshal(data, &queue)
	return queue, err
}

// 存储队列
func (q *Queue) setQueue(txn *badger.Txn, queue []string) error {
	data, err := json.Marshal(queue)
	if err != nil {
		return err
	}
	return txn.Set([]byte(q.queueName), data)
}
func (q *Queue) Len() (int, error) {
	var length int
	err := q.db.View(func(txn *badger.Txn) error {
		queue, err := q.getQueue(txn)
		if err != nil {
			return err
		}
		length = len(queue)
		return nil
	})
	return length, err
}
func (q *Queue) Dequeue() (string, error) {
	var result string
	err := q.db.Update(func(txn *badger.Txn) error {
		queue, err := q.getQueue(txn)
		if err != nil {
			return err
		}

		// 队列为空
		if len(queue) == 0 {
			return badger.ErrKeyNotFound
		}

		// 取出队列第一个元素（FIFO）
		result = queue[0]
		queue = queue[1:]

		// 存回数据库
		return q.setQueue(txn, queue)
	})

	if err != nil {
		return "", err
	}
	return result, nil
}
func (q *Queue) Enqueue(value string) error {
	return q.db.Update(func(txn *badger.Txn) error {
		queue, err := q.getQueue(txn)
		if err != nil {
			return err
		}
		queue = append(queue, value) // 追加元素
		return q.setQueue(txn, queue)
	})
}
