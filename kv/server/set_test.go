package server

import (
	"context"
	"testing"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage/standalone_storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
)

func TestSetOperations(t *testing.T) {
	conf := config.NewTestConfig()
	s := standalone_storage.NewStandAloneStorage(conf)
	s.Start()
	server := NewServer(s)
	defer cleanUpTestData(conf)
	defer s.Stop()

	// 1. 测试 SAdd（添加元素）
	_, err := server.SAdd(context.Background(), &kvrpcpb.SetAddRequest{
		Key:     []byte("testSet"),
		Members: [][]byte{[]byte("Alice"), []byte("Bob"), []byte("Charlie")},
		Context: &kvrpcpb.Context{},
	})
	assert.NoError(t, err)

	// 2. 测试 SMembers（获取所有成员）
	resp, err := server.SMembers(context.Background(), &kvrpcpb.SetMembersRequest{
		Key:     []byte("testSet"),
		Context: &kvrpcpb.Context{},
	})
	assert.NoError(t, err)
	assert.ElementsMatch(t, [][]byte{[]byte("Alice"), []byte("Bob"), []byte("Charlie")}, resp.Members)

	// 3. 测试 SIsMember（检查元素是否存在）
	isMemberResp, err := server.SIsMember(context.Background(), &kvrpcpb.SetIsMemberRequest{
		Key:     []byte("testSet"),
		Member:  []byte("Alice"),
		Context: &kvrpcpb.Context{},
	})
	assert.NoError(t, err)
	assert.True(t, isMemberResp.Exists)

	isMemberResp, err = server.SIsMember(context.Background(), &kvrpcpb.SetIsMemberRequest{
		Key:     []byte("testSet"),
		Member:  []byte("Dave"),
		Context: &kvrpcpb.Context{},
	})
	assert.NoError(t, err)
	assert.False(t, isMemberResp.Exists)

	// 4. 测试 SCard（获取 Set 长度）
	cardResp, err := server.SCard(context.Background(), &kvrpcpb.SetCardRequest{
		Key:     []byte("testSet"),
		Context: &kvrpcpb.Context{},
	})
	assert.NoError(t, err)
	assert.Equal(t, int32(3), cardResp.Count)

	// 5. 测试 SRem（删除元素）
	_, err = server.SRem(context.Background(), &kvrpcpb.SetRemoveRequest{
		Key:     []byte("testSet"),
		Members: []string{"Alice"},
		Context: &kvrpcpb.Context{},
	})
	assert.NoError(t, err)

	// 确保 "Alice" 被移除
	resp, err = server.SMembers(context.Background(), &kvrpcpb.SetMembersRequest{
		Key:     []byte("testSet"),
		Context: &kvrpcpb.Context{},
	})
	assert.NoError(t, err)
	assert.ElementsMatch(t, [][]byte{[]byte("Bob"), []byte("Charlie")}, resp.Members)

	// 6. 测试 SUnion（计算多个 Set 的并集）
	_, err = server.SAdd(context.Background(), &kvrpcpb.SetAddRequest{
		Key:     []byte("testSet2"),
		Members: [][]byte{[]byte("Charlie"), []byte("Dave")},
		Context: &kvrpcpb.Context{},
	})
	assert.NoError(t, err)

	unionResp, err := server.SUnion(context.Background(), &kvrpcpb.SetUnionRequest{
		Keys:    [][]byte{[]byte("testSet"), []byte("testSet2")},
		Context: &kvrpcpb.Context{},
	})
	assert.NoError(t, err)
	assert.ElementsMatch(t, [][]byte{[]byte("Bob"), []byte("Charlie"), []byte("Dave")}, unionResp.Members)

	// 7. 测试 SRem 删除所有元素后 Set 为空
	_, err = server.SRem(context.Background(), &kvrpcpb.SetRemoveRequest{
		Key:     []byte("testSet"),
		Members: []string{"Bob", "Charlie"},
		Context: &kvrpcpb.Context{},
	})
	assert.NoError(t, err)

	cardResp, err = server.SCard(context.Background(), &kvrpcpb.SetCardRequest{
		Key:     []byte("testSet"),
		Context: &kvrpcpb.Context{},
	})
	assert.NoError(t, err)
	assert.Equal(t, int32(0), cardResp.Count)
}
