package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raw API.
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return &kvrpcpb.RawGetResponse{Error: err.Error()}, err
	}
	defer reader.Close()
	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return &kvrpcpb.RawGetResponse{Error: err.Error()}, err
	} else if val == nil {
		return &kvrpcpb.RawGetResponse{NotFound: true}, nil
	}
	return &kvrpcpb.RawGetResponse{Value: val}, nil
}

func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	batch := []storage.Modify{
		{
			Data: storage.Put{
				Cf:    req.Cf,
				Key:   req.Key,
				Value: req.Value,
			},
		}}
	err := server.storage.Write(req.Context, batch)
	res := &kvrpcpb.RawPutResponse{}
	if err != nil {
		res.Error = err.Error()
		return res, err
	}
	return res, nil
}

func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	modify := storage.Modify{Data: storage.Delete{
		Key: req.Key,
		Cf:  req.Cf,
	}}
	err := server.storage.Write(req.Context, []storage.Modify{modify})
	if err != nil {
		return &kvrpcpb.RawDeleteResponse{Error: err.Error()}, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	response := &kvrpcpb.RawScanResponse{}
	if req.Limit == 0 {
		return response, nil
	}
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		return &kvrpcpb.RawScanResponse{Error: err.Error()}, err
	}
	iter := reader.IterCF(req.Cf)
	iter.Seek(req.StartKey)
	defer iter.Close()
	for i := uint32(0); i < req.Limit && iter.Valid(); i++ {
		key := iter.Item().KeyCopy(nil)
		val, err := iter.Item().ValueCopy(nil)
		if err != nil {
			return &kvrpcpb.RawScanResponse{Error: err.Error()}, err
		}
		response.Kvs = append(response.Kvs, &kvrpcpb.KvPair{Key: key, Value: val})
		iter.Next()
	}
	return response, nil
}

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

func isRegionError(err error) *raft_storage.RegionError {
	regionErr, r := err.(*raft_storage.RegionError)
	if !r {
		return nil
	}
	return regionErr
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		rr := isRegionError(err)
		if rr != nil {
			return &kvrpcpb.GetResponse{RegionError: rr.RequestErr}, nil
		}
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.Version)
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		rr := isRegionError(err)
		if rr != nil {
			return &kvrpcpb.GetResponse{RegionError: rr.RequestErr}, nil
		}
		return nil, err
	}
	if lock != nil && req.Version >= lock.Ts {
		res := &kvrpcpb.GetResponse{}
		res.Error = &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				Key:         req.Key,
				PrimaryLock: lock.Primary,
				LockVersion: lock.Ts,
				LockTtl:     lock.Ttl,
			}}
		return res, nil
	}
	val, err := txn.GetValue(req.Key)
	if err != nil {
		rr := isRegionError(err)
		if rr != nil {
			return &kvrpcpb.GetResponse{RegionError: rr.RequestErr}, nil
		}
		return nil, err
	}
	if val == nil {
		return &kvrpcpb.GetResponse{NotFound: true}, nil
	}
	return &kvrpcpb.GetResponse{Value: val}, nil
}

func getKeyErrors(req *kvrpcpb.PrewriteRequest, txn *mvcc.MvccTxn) []*kvrpcpb.KeyError {
	var keyErrors []*kvrpcpb.KeyError
	for _, m := range req.Mutations {
		write, ts, _ := txn.MostRecentWrite(m.Key)
		if write != nil && req.StartVersion <= ts {
			keyErrors = append(keyErrors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					Key:        m.Key,
					StartTs:    req.StartVersion,
					ConflictTs: ts,
					Primary:    req.PrimaryLock,
				}})
			continue
		}
		lock, _ := txn.GetLock(m.Key)
		if lock != nil && lock.Ts != req.StartVersion {
			keyErrors = append(keyErrors, &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					Key:         m.Key,
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					LockTtl:     lock.Ttl,
				}})
		}
		var kind mvcc.WriteKind
		switch m.Op {
		case kvrpcpb.Op_Put:
			kind = mvcc.WriteKindPut
			txn.PutValue(m.Key, m.Value)
		case kvrpcpb.Op_Del:
			kind = mvcc.WriteKindDelete
			txn.DeleteValue(m.Key)
		}
		txn.PutLock(m.Key, &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    kind,
		})
	}
	return keyErrors
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	if len(req.Mutations) == 0 {
		return &kvrpcpb.PrewriteResponse{}, nil
	}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		rr := isRegionError(err)
		if rr != nil {
			return &kvrpcpb.PrewriteResponse{RegionError: rr.RequestErr}, nil
		}
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	keyErrors := getKeyErrors(req, txn)
	if len(keyErrors) > 0 {
		return &kvrpcpb.PrewriteResponse{Errors: keyErrors}, nil
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		rr := isRegionError(err)
		if rr != nil {
			return &kvrpcpb.PrewriteResponse{RegionError: rr.RequestErr}, nil
		}
		return nil, err
	}
	return &kvrpcpb.PrewriteResponse{}, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	if len(req.Keys) == 0 {
		return &kvrpcpb.CommitResponse{}, nil
	}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		rr := isRegionError(err)
		if rr != nil {
			return &kvrpcpb.CommitResponse{RegionError: rr.RequestErr}, nil
		}
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			rr := isRegionError(err)
			if rr != nil {
				return &kvrpcpb.CommitResponse{RegionError: rr.RequestErr}, nil
			}
			return nil, err
		}
		if lock == nil {
			return &kvrpcpb.CommitResponse{}, nil
		}
		if lock.Ts != req.StartVersion {
			return &kvrpcpb.CommitResponse{Error: &kvrpcpb.KeyError{Retryable: "true"}}, nil
		}
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    lock.Kind,
		})
		txn.DeleteLock(key)
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		rr := isRegionError(err)
		if rr != nil {
			return &kvrpcpb.CommitResponse{RegionError: rr.RequestErr}, nil
		}
		return nil, err
	}
	return &kvrpcpb.CommitResponse{}, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		rr := isRegionError(err)
		if rr != nil {
			resp.RegionError = rr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer scanner.Close()
	var pairs []*kvrpcpb.KvPair
	limit := int(req.Limit)
	for i := 0; i < limit; {
		key, value, err := scanner.Next()
		if err != nil {
			rr := isRegionError(err)
			if rr != nil {
				resp.RegionError = rr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		if key == nil {
			break
		}
		lock, err := txn.GetLock(key)
		if err != nil {
			rr := isRegionError(err)
			if rr != nil {
				resp.RegionError = rr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		if lock != nil && req.Version >= lock.Ts {
			pairs = append(pairs, &kvrpcpb.KvPair{
				Error: &kvrpcpb.KeyError{
					Locked: &kvrpcpb.LockInfo{
						PrimaryLock: lock.Primary,
						LockVersion: lock.Ts,
						Key:         key,
						LockTtl:     lock.Ttl,
					}},
				Key: key,
			})
			i++
			continue
		}
		if value != nil {
			pairs = append(pairs, &kvrpcpb.KvPair{Key: key, Value: value})
			i++
		}
	}
	resp.Pairs = pairs
	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.CheckTxnStatusResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		rr := isRegionError(err)
		if rr != nil {
			resp.RegionError = rr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.LockTs)
	write, ts, err := txn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		rr := isRegionError(err)
		if rr != nil {
			resp.RegionError = rr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	if write != nil {
		if write.Kind != mvcc.WriteKindRollback {
			resp.CommitVersion = ts
		}
		return resp, nil
	}
	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		rr := isRegionError(err)
		if rr != nil {
			resp.RegionError = rr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	if lock == nil {
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			rr := isRegionError(err)
			if rr != nil {
				resp.RegionError = rr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		resp.Action = kvrpcpb.Action_LockNotExistRollback
		return resp, nil
	}
	if mvcc.PhysicalTime(lock.Ts)+lock.Ttl <= mvcc.PhysicalTime(req.CurrentTs) {
		txn.DeleteLock(req.PrimaryKey)
		txn.DeleteValue(req.PrimaryKey)
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			rr := isRegionError(err)
			if rr != nil {
				resp.RegionError = rr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		resp.Action = kvrpcpb.Action_TTLExpireRollback
	}
	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.BatchRollbackResponse{}
	if len(req.Keys) == 0 {
		return resp, nil
	}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		rr := isRegionError(err)
		if rr != nil {
			resp.RegionError = rr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	for _, key := range req.Keys {
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			rr := isRegionError(err)
			if rr != nil {
				resp.RegionError = rr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		if write != nil {
			if write.Kind == mvcc.WriteKindRollback {
				continue
			} else {
				resp.Error = &kvrpcpb.KeyError{Abort: "true"}
				return resp, nil
			}
		}
		lock, err := txn.GetLock(key)
		if err != nil {
			rr := isRegionError(err)
			if rr != nil {
				resp.RegionError = rr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		if lock == nil || lock.Ts != req.StartVersion {
			txn.PutWrite(key, req.StartVersion, &mvcc.Write{
				StartTS: req.StartVersion,
				Kind:    mvcc.WriteKindRollback,
			})
			continue
		}
		txn.DeleteLock(key)
		txn.DeleteValue(key)
		txn.PutWrite(key, req.StartVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    mvcc.WriteKindRollback,
		})
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		rr := isRegionError(err)
		if rr != nil {
			resp.RegionError = rr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ResolveLockResponse{}
	if req.StartVersion == 0 {
		return resp, nil
	}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		rr := isRegionError(err)
		if rr != nil {
			resp.RegionError = rr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()
	iter := reader.IterCF(engine_util.CfLock)
	if err != nil {
		rr := isRegionError(err)
		if rr != nil {
			resp.RegionError = rr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer iter.Close()
	var keys [][]byte
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		value, err := item.ValueCopy(nil)
		if err != nil {
			return resp, err
		}
		lock, err := mvcc.ParseLock(value)
		if err != nil {
			return resp, err
		}
		if lock.Ts == req.StartVersion {
			key := item.KeyCopy(nil)
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return resp, nil
	}
	if req.CommitVersion == 0 {
		resp1, err := server.KvBatchRollback(nil, &kvrpcpb.BatchRollbackRequest{
			Context:      req.Context,
			StartVersion: req.StartVersion,
			Keys:         keys,
		})
		resp.Error = resp1.Error
		resp.RegionError = resp1.RegionError
		return resp, err
	} else {
		resp1, err := server.KvCommit(nil, &kvrpcpb.CommitRequest{
			Context:       req.Context,
			StartVersion:  req.StartVersion,
			Keys:          keys,
			CommitVersion: req.CommitVersion,
		})
		resp.Error = resp1.Error
		resp.RegionError = resp1.RegionError
		return resp, err
	}
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
