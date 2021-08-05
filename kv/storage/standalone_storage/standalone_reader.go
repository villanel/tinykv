package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type StandaloneReader struct {
	db *badger.DB
}

func (r *StandaloneReader  ) GetCF(cf string, key []byte) ([]byte, error) {
	getCF, err := engine_util.GetCF(r.db, cf, key)
	if err != nil {
		return nil, err
	}
	return getCF, nil
}

func (r StandaloneReader ) IterCF(cf string) engine_util.DBIterator {
	panic("implement me")
}

func (r StandaloneReader) Close() {

	panic("implement me")
}

