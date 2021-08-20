package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type StandaloneReader struct {
	txn *badger.Txn
}

func (r *StandaloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	getCF, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err != nil && err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return getCF, err
}

func (r *StandaloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StandaloneReader) Close() {
	r.txn.Discard()
}
