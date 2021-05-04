package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	// The standalone store is a wrapper for the BadgerDB API, located in util/engine_util
	engine *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	// The creation of standalone storage requires the creation of badgerDB first.
	kvDir := conf.DBPath + "/kv"
	kvDB := engine_util.CreateDB(kvDir, false)
	fakeRaftDir := conf.DBPath + "/fakeRaft" // raft is not required in the standalone version
	raftDB := engine_util.CreateDB(fakeRaftDir, true)
	engine := engine_util.NewEngines(kvDB, raftDB, kvDir, fakeRaftDir)
	return &StandAloneStorage{engine: engine}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.engine.Close()
}

// used in Reader
type storageReader struct {
	txn *badger.Txn
}

// GetCF checks value based on column family and key name. ${cf}_${key}
func (sr *storageReader) GetCF(cf string, key []byte) ([]byte, error) {

	val, err := engine_util.GetCFFromTxn(sr.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return val, nil
}

// Iterator of GetCF
func (sr *storageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, sr.txn)
}

func (sr *storageReader) Close() {
	sr.txn.Discard()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	// Reader Get snapshot data using BadgerDB's transaction feature
	txn := s.engine.Kv.NewTransaction(false)
	return &storageReader{txn: txn}, nil
}

// Batch write
func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	writeBatch := &engine_util.WriteBatch{}
	for _, modify := range batch {
		switch v := modify.Data.(type) {
		case storage.Put:
			writeBatch.SetCF(v.Cf, v.Key, v.Value)
		case storage.Delete:
			writeBatch.DeleteCF(v.Cf, v.Key)
		}
	}
	return s.engine.WriteKV(writeBatch)
}
