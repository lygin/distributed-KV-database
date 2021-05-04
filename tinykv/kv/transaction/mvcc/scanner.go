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
	txn  *MvccTxn
	iter engine_util.DBIterator
	next []byte
	done bool
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	return &Scanner{
		next: startKey,
		txn:  txn,
		iter: txn.Reader.IterCF(engine_util.CfWrite),
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if scan.done {
		return nil, nil, nil
	}
	key := scan.next
	scan.iter.Seek(EncodeKey(key, scan.txn.StartTS))
	if !scan.iter.Valid() {
		scan.done = true
		return nil, nil, nil
	}
	item := scan.iter.Item()
	gotKey := item.KeyCopy(nil)
	userKey := DecodeUserKey(gotKey)
	if !bytes.Equal(key, userKey) {
		scan.next = userKey
		return scan.Next()
	}
	for {
		scan.iter.Next()
		if !scan.iter.Valid() {
			scan.done = true
			break
		}
		item := scan.iter.Item()
		gotKey := item.KeyCopy(nil)
		userKey := DecodeUserKey(gotKey)
		if !bytes.Equal(key, userKey) {
			scan.next = userKey
			break
		}
	}
	writeVal, err := item.ValueCopy(nil)
	if err != nil {
		return key, nil, err
	}
	write, err := ParseWrite(writeVal)
	if err != nil {
		return key, nil, err
	}
	if write.Kind == WriteKindDelete {
		return key, nil, nil
	}
	value, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
	return key, value, err
}
