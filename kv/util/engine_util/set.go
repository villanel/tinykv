package engine_util

import (
	"encoding/json"
	"math/rand"

	"github.com/Connor1996/badger"
)

// SetStore 结构体，用于管理集合
type SetStore struct {
	db *badger.DB
}

// 创建 SetStore
func NewSetStore(path string) (*SetStore, error) {
	opts := badger.DefaultOptions
	opts.Dir = path
	opts.ValueDir = opts.Dir
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &SetStore{db: db}, nil
}

// 关闭数据库
func (s *SetStore) Close() {
	s.db.Close()
}
func (s *SetStore) SAdd(setName string, members ...string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		set, err := s.getSet(txn, setName)
		if err != nil {
			return err
		}

		for _, member := range members {
			set[member] = struct{}{}
		}
		return s.saveSet(txn, setName, set)
	})
}
func (s *SetStore) SCard(setName string) (int, error) {
	var count int
	err := s.db.View(func(txn *badger.Txn) error {
		set, err := s.getSet(txn, setName)
		if err != nil {
			return err
		}
		count = len(set)
		return nil
	})
	return count, err
}
func (s *SetStore) SIsMember(setName, member string) (bool, error) {
	var exists bool
	err := s.db.View(func(txn *badger.Txn) error {
		set, err := s.getSet(txn, setName)
		if err != nil {
			return err
		}
		_, exists = set[member]
		return nil
	})
	return exists, err
}
func (s *SetStore) SMembers(setName string) ([]string, error) {
	var members []string
	err := s.db.View(func(txn *badger.Txn) error {
		set, err := s.getSet(txn, setName)
		if err != nil {
			return err
		}
		for member := range set {
			members = append(members, member)
		}
		return nil
	})
	return members, err
}
func (s *SetStore) SRem(setName string, members ...string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		set, err := s.getSet(txn, setName)
		if err != nil {
			return err
		}

		for _, member := range members {
			delete(set, member)
		}
		return s.saveSet(txn, setName, set)
	})
}
func (s *SetStore) SPop(setName string) (string, error) {
	var popped string
	err := s.db.Update(func(txn *badger.Txn) error {
		set, err := s.getSet(txn, setName)
		if err != nil || len(set) == 0 {
			return err
		}

		for member := range set {
			popped = member
			delete(set, member)
			break
		}

		return s.saveSet(txn, setName, set)
	})
	return popped, err
}
func (s *SetStore) SRandMember(setName string) (string, error) {
	var members []string
	err := s.db.View(func(txn *badger.Txn) error {
		set, err := s.getSet(txn, setName)
		if err != nil || len(set) == 0 {
			return err
		}

		for member := range set {
			members = append(members, member)
		}
		return nil
	})
	if err != nil || len(members) == 0 {
		return "", err
	}
	return members[rand.Intn(len(members))], nil
}

// 获取集合
func (s *SetStore) getSet(txn *badger.Txn, setName string) (map[string]struct{}, error) {
	item, err := txn.Get([]byte("set:" + setName))
	if err == badger.ErrKeyNotFound {
		return make(map[string]struct{}), nil
	} else if err != nil {
		return nil, err
	}

	var set map[string]struct{}
	data, _ := item.ValueCopy(nil)
	err = json.Unmarshal(data, &set)
	return set, err
}

// 存储集合
func (s *SetStore) saveSet(txn *badger.Txn, setName string, set map[string]struct{}) error {
	data, err := json.Marshal(set)
	if err != nil {
		return err
	}
	return txn.Set([]byte("set:"+setName), data)
}
