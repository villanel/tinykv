package engine_util

import (
	"log"
	"testing"

	"github.com/Connor1996/badger"
	"github.com/stretchr/testify/assert"
)

// 测试入队和出队
func TestQueue(t *testing.T) {
	// 创建一个临时 BadgerDB 队列（避免影响实际数据）
	queue, err := NewQueue("/tmp/test/badger_test", "testQueue")
	log.Println(err)
	assert.NoError(t, err)
	defer queue.db.Close()

	// 清空队列
	_ = queue.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(queue.queueName))
	})

	// 1. 测试入队
	err = queue.Enqueue("Message 1")
	assert.NoError(t, err)

	err = queue.Enqueue("Message 2")
	assert.NoError(t, err)

	err = queue.Enqueue("Message 3")
	assert.NoError(t, err)

	// 2. 测试队列长度
	length, err := queue.Len()
	assert.NoError(t, err)
	assert.Equal(t, 3, length, "Queue length should be 3")

	// 3. 测试出队（FIFO）
	data, err := queue.Dequeue()
	assert.NoError(t, err)
	assert.Equal(t, "Message 1", data)

	data, err = queue.Dequeue()
	assert.NoError(t, err)
	assert.Equal(t, "Message 2", data)

	// 4. 再次检查队列长度
	length, err = queue.Len()
	assert.NoError(t, err)
	assert.Equal(t, 1, length, "Queue length should be 1 after two dequeues")

	// 5. 继续出队
	data, err = queue.Dequeue()
	assert.NoError(t, err)
	assert.Equal(t, "Message 3", data)

	// 6. 测试空队列
	_, err = queue.Dequeue()
	assert.Error(t, err, "Dequeue should return error when queue is empty")

	// 7. 确保队列为空
	length, err = queue.Len()
	assert.NoError(t, err)
	assert.Equal(t, 0, length, "Queue should be empty after all dequeues")
}
