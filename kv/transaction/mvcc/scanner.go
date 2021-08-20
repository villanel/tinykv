package mvcc

import (
	"bytes"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	iter engine_util.DBIterator
	txn *MvccTxn
	key []byte
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	scanner:=&Scanner{
		txn:txn,
		key: startKey,
		iter: txn.Reader.IterCF(engine_util.CfWrite),
	}
	return scanner
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
//从当前item取得value再将item转向下个key
func (scan *Scanner) Next() ([]byte, []byte, error) {
	if scan.key==nil {
		return nil, nil, nil
	}
	key := scan.key
	scan.iter.Seek(EncodeKey(key, scan.txn.StartTS))
	if !scan.iter.Valid() {
		scan.key = nil
		return nil, nil, nil
	}
	item := scan.iter.Item()
	writeVal, err := item.ValueCopy(nil)
	if err != nil {
		return key, nil, err
	}
	write, err := ParseWrite(writeVal)
	if err != nil {
		return key, nil, err
	}

	value, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
	DecrKey := DecodeUserKey(item.Key())
	if !bytes.Equal(key, DecrKey) {
		scan.key = DecrKey
		return scan.Next()
	}
	curKey:=scan.key
	scan.key=nil
		for ;scan.iter.Valid();scan.iter.Next(){
			if decodeTimestamp(scan.iter.Item().Key()) > scan.txn.StartTS {
				continue
			}
			if!bytes.Equal(curKey,DecodeUserKey(scan.iter.Item().Key())){
				scan.key = DecodeUserKey(scan.iter.Item().Key())
				break
			}
		}
	if write.Kind == WriteKindDelete {
		return key, nil, nil
	}
	return key, value, err
}

