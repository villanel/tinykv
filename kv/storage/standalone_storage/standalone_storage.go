package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"io/ioutil"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{
		nil,
	}
	return nil
}

func (s *StandAloneStorage) Start() error {
	dir, _ := ioutil.TempDir("", "engine_util")
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir
	db, _ := badger.Open(opts)
	s.db = db
	return nil
}

func (s *StandAloneStorage) Stop() error {
	err := s.db.Close()
	if err != nil {
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return &StandaloneReader{
		s.db,
	}, nil

}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	e := new(engine_util.WriteBatch)
	for _, modify := range batch {
		e.SetCF(modify.Cf(), modify.Key(), modify.Value())
	}
	e.WriteToDB(s.db)
	return nil
}
