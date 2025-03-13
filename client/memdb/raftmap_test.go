package memdb

import (
	"fmt"
	"log"
	"testing"

	"github.com/pingcap/tidb/kv"

	kvstore "github.com/pingcap-incubator/tinykv/store"
	"github.com/pingcap-incubator/tinykv/store/mockstore"
	"github.com/pingcap-incubator/tinykv/store/tikv"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func registerStores() {
	kvstore.Register("tikv", tikv.Driver{})
	kvstore.Register("mocktikv", mockstore.MockDriver{})
}
func setupTestClient(t *testing.T) *tikv.RawKVClient {
	// 使用 mockstore 作为模拟 TiKV 存储
	registerStores()
	fullPath := fmt.Sprintf("%s://%s", "tikv", "127.0.0.1:2379")
	fmt.Print(fullPath)
	var err error
	var storage kv.Storage
	fmt.Print("Bootstrapping system timezone...")
	storage, err = kvstore.New(fullPath)
	if err != nil {
		log.Fatal("Failed to create storage", zap.Error(err))
	}
	s := tikv.GetTikvStore(storage)
	client, _ := tikv.NewRawKVClient(s)
	return client
}

func TestRaftMap_SetAndGet(t *testing.T) {
	client := setupTestClient(t)
	defer client.Close()

	raftMap := NewRaftMap(client)

	// 测试设置值
	key := "test_key"
	value := "test_value"
	assert.Equal(t, 1, raftMap.Set(key, value))

	// 测试获取值
	result, exists := raftMap.Get(key)
	print(result.(string))
	assert.True(t, exists)
	assert.Equal(t, value, result)
}

func TestRaftMap_SetIfExist(t *testing.T) {
	client := setupTestClient(t)
	defer client.Close()

	raftMap := NewRaftMap(client)

	key := "test_exist_key"
	value := "existing_value"
	assert.Equal(t, 0, raftMap.SetIfExist(key, value)) // 不存在，应该失败

	// 先设置值，再测试 SetIfExist
	assert.Equal(t, 1, raftMap.Set(key, value))
	newValue := "updated_value"
	assert.Equal(t, 1, raftMap.SetIfExist(key, newValue))

	result, exists := raftMap.Get(key)
	assert.True(t, exists)
	assert.Equal(t, newValue, result)
}

func TestRaftMap_SetIfNotExist(t *testing.T) {
	client := setupTestClient(t)
	defer client.Close()

	raftMap := NewRaftMap(client)

	key := "test_not_exist_key"
	value := "first_value"
	assert.Equal(t, 1, raftMap.SetIfNotExist(key, value)) // 第一次插入应该成功

	// 尝试再插入相同 key，不应该被覆盖
	newValue := "new_value"
	assert.Equal(t, 0, raftMap.SetIfNotExist(key, newValue))

	result, exists := raftMap.Get(key)
	assert.True(t, exists)
	assert.Equal(t, value, result) // 应该仍然是原始值
}

func TestRaftMap_Delete(t *testing.T) {
	client := setupTestClient(t)
	defer client.Close()

	raftMap := NewRaftMap(client)

	key := "test_delete_key"
	value := "to_delete"
	assert.Equal(t, 1, raftMap.Set(key, value))

	// 确保值存在
	result, exists := raftMap.Get(key)
	assert.True(t, exists)
	assert.Equal(t, value, result)

	// 测试删除
	assert.Equal(t, 1, raftMap.Delete(key))

	// 确保值已删除
	_, exists = raftMap.Get(key)
	assert.False(t, exists)
}

func TestRaftMap_Keys(t *testing.T) {
	client := setupTestClient(t)
	defer client.Close()

	raftMap := NewRaftMap(client)

	// 插入多个键
	keys := []string{"key1", "key2", "key3"}
	values := []string{"value1", "value2", "value3"}

	for i, key := range keys {
		assert.Equal(t, 1, raftMap.Set(key, values[i]))
	}

	// 获取所有键
	storedKeys := raftMap.Keys()
	assert.ElementsMatch(t, keys, storedKeys)
}
