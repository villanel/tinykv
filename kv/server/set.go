package server

import (
	"context"
	"encoding/json"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

func (server *Server) getSet(key []byte, cf string, ctx *kvrpcpb.Context) (map[string]struct{}, error) {
	// 获取存储 Reader
	reader, err := server.storage.Reader(ctx)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	// 读取 key 对应的 Set
	value, err := reader.GetCF(cf, key)
	if err != nil {
		return nil, err
	}

	// 如果 key 不存在，返回空集合
	if value == nil {
		return make(map[string]struct{}), nil
	}

	// 解析 JSON
	var set map[string]struct{}
	err = json.Unmarshal(value, &set)
	return set, err
}
func (server *Server) saveSet(key []byte, set map[string]struct{}, context *kvrpcpb.Context) error {
	setData, err := json.Marshal(set)
	if err != nil {
		return err

	}
	put := storage.Put{
		Key:   key,
		Value: setData,
		Cf:    "1",
	}
	modi := storage.Modify{
		Data: put,
	}
	batch := []storage.Modify{modi}

	// 传入 Write
	err = server.storage.Write(context, batch)

	// 返回响应
	if err != nil {
		return err
	}
	return nil
}
func (server *Server) delSet(key []byte, context *kvrpcpb.Context) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	// 构造 Modify 与 batch
	del := storage.Delete{
		Key: key,
		Cf:  "1",
	}
	modi := storage.Modify{
		Data: del,
	}
	batch := []storage.Modify{modi}

	// 传入 Write
	err := server.storage.Write(context, batch)

	// 返回响应
	if err != nil {
		return &kvrpcpb.RawDeleteResponse{}, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// 创建 SetService
// SAdd - 添加元素到集合
func (s *Server) SAdd(ctx context.Context, req *kvrpcpb.SetAddRequest) (*kvrpcpb.SetAddResponse, error) {
	// 获取现有的 Set
	set, err := s.getSet(req.Key, "1", req.Context)
	if err != nil {
		return &kvrpcpb.SetAddResponse{}, err
	}

	// 处理 `[][]byte` 类型的 `Members`
	for _, member := range req.Members {
		set[string(member)] = struct{}{}
	}

	// 存储更新后的 Set
	err = s.saveSet(req.Key, set, req.Context)
	if err != nil {
		return &kvrpcpb.SetAddResponse{}, err
	}

	return &kvrpcpb.SetAddResponse{Success: true}, nil
}
func (s *Server) SMembers(ctx context.Context, req *kvrpcpb.SetMembersRequest) (*kvrpcpb.SetMembersResponse, error) {
	// 获取当前 Set
	set, err := s.getSet(req.Key, "1", req.Context)
	if err != nil {
		return &kvrpcpb.SetMembersResponse{}, err
	}

	// 转换 `map[string]struct{}` 为 `[][]byte`
	var members [][]byte
	for member := range set {
		members = append(members, []byte(member))
	}

	return &kvrpcpb.SetMembersResponse{Members: members}, nil
}
func (s *Server) SRem(ctx context.Context, req *kvrpcpb.SetRemoveRequest) (*kvrpcpb.SetRemoveResponse, error) {
	// 获取当前 Set
	set, err := s.getSet(req.Key, "1", req.Context)
	if err != nil {
		return &kvrpcpb.SetRemoveResponse{}, err
	}

	// 删除指定成员
	for _, member := range req.Members {
		delete(set, member)
	}

	// 如果 Set 为空，则删除 key
	if len(set) == 0 {
		_, err = s.delSet(req.Key, req.Context)
	} else {
		err = s.saveSet(req.Key, set, req.Context)
	}

	if err != nil {
		return &kvrpcpb.SetRemoveResponse{}, err
	}

	return &kvrpcpb.SetRemoveResponse{Success: true}, nil
}
func (s *Server) SIsMember(ctx context.Context, req *kvrpcpb.SetIsMemberRequest) (*kvrpcpb.SetIsMemberResponse, error) {
	// 获取当前 Set
	set, err := s.getSet(req.Key, "1", req.Context)
	if err != nil {
		return &kvrpcpb.SetIsMemberResponse{}, err
	}

	// 检查成员是否存在
	_, exists := set[string(req.Member)]

	return &kvrpcpb.SetIsMemberResponse{Exists: exists}, nil
}
func (s *Server) SCard(ctx context.Context, req *kvrpcpb.SetCardRequest) (*kvrpcpb.SetCardResponse, error) {
	// 获取当前 Set
	set, err := s.getSet(req.Key, "1", req.Context)
	if err != nil {
		return &kvrpcpb.SetCardResponse{}, err
	}

	// 返回 Set 大小
	return &kvrpcpb.SetCardResponse{Count: int32(len(set))}, nil
}
func (s *Server) SUnion(ctx context.Context, req *kvrpcpb.SetUnionRequest) (*kvrpcpb.SetUnionResponse, error) {
	unionSet := make(map[string]struct{})

	// 遍历所有 Sets 并合并
	for _, key := range req.Keys {
		set, err := s.getSet(key, "1", req.Context)
		if err != nil {
			return &kvrpcpb.SetUnionResponse{}, err
		}
		for member := range set {
			unionSet[member] = struct{}{}
		}
	}

	// 转换 `unionSet` 为 `[][]byte`
	var members [][]byte
	for member := range unionSet {
		members = append(members, []byte(member))
	}

	return &kvrpcpb.SetUnionResponse{Members: members}, nil
}
