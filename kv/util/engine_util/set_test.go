package engine_util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// 初始化测试用例
func setupTestStore(t *testing.T) *SetStore {
	store, err := NewSetStore("./badger_test")
	assert.NoError(t, err)
	return store
}

// 清理测试数据
func teardownTestStore(store *SetStore) {
	store.Close()
}
func TestSAddAndSMembers(t *testing.T) {
	store := setupTestStore(t)
	defer teardownTestStore(store)

	// 添加元素
	err := store.SAdd("testSet", "Alice", "Bob", "Charlie")
	assert.NoError(t, err)

	// 获取集合中的所有成员
	members, err := store.SMembers("testSet")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"Alice", "Bob", "Charlie"}, members)
}
func TestSCard(t *testing.T) {
	store := setupTestStore(t)
	defer teardownTestStore(store)

	store.SAdd("testSet", "Alice", "Bob", "Charlie")

	count, err := store.SCard("testSet")
	assert.NoError(t, err)
	assert.Equal(t, 3, count)
}
func TestSIsMember(t *testing.T) {
	store := setupTestStore(t)
	defer teardownTestStore(store)

	store.SAdd("testSet", "Alice")

	// 调试：检查 testSet 的内容
	members, err := store.SMembers("testSet")
	assert.NoError(t, err)
	t.Log("Current set members:", members)
	for _, member := range members {

		print(member)
	}

	// 确保 "Alice" 存在
	exists, err := store.SIsMember("testSet", "Alice")
	assert.NoError(t, err)
	assert.True(t, exists)
	store.SRem("testSet", "Bob")
	// 确保 "Bob" 不存在
	exists, err = store.SIsMember("testSet", "Bob")
	assert.NoError(t, err)
	assert.False(t, exists)
}
func TestSRem(t *testing.T) {
	store := setupTestStore(t)
	defer teardownTestStore(store)

	store.SAdd("testSet", "Alice", "Bob", "Charlie")
	store.SRem("testSet", "Bob")

	members, err := store.SMembers("testSet")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"Alice", "Charlie"}, members)
}
func TestSPop(t *testing.T) {
	store := setupTestStore(t)
	defer teardownTestStore(store)

	store.SAdd("testSet", "Alice", "Bob", "Charlie")

	popped, err := store.SPop("testSet")
	assert.NoError(t, err)

	members, _ := store.SMembers("testSet")
	assert.NotContains(t, members, popped)
}
func TestSRandMember(t *testing.T) {
	store := setupTestStore(t)
	defer teardownTestStore(store)

	store.SAdd("testSet", "Alice", "Bob", "Charlie")

	random, err := store.SRandMember("testSet")
	assert.NoError(t, err)

	members, _ := store.SMembers("testSet")
	assert.Contains(t, members, random)
}
