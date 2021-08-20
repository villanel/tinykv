package server

import (
	"context"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
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
	resp := new(kvrpcpb.RawGetResponse)
	reader, _ := server.storage.Reader(nil)
	cf, err := reader.GetCF(req.Cf, req.Key)
	if err != nil &&err!=badger.ErrKeyNotFound{
		resp.Error = err.Error()
		return resp, err
	}
	var not bool
	not =cf==nil
	return &kvrpcpb.RawGetResponse{Value: cf,NotFound:not}, nil
}

func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	resp := &kvrpcpb.RawPutResponse{}
	put := []storage.Modify{
		{
			Data: storage.Put{
				Cf:    req.Cf,
				Key:   req.Key,
				Value: req.Value,
			},
		}}
	err := server.storage.Write(req.Context, put)
	if err!=nil{
		resp.RegionError = util.RaftstoreErrToPbError(err)
		resp.Error=err.Error()
		return resp,nil
	}

	return resp, nil
}

func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	resp := new(kvrpcpb.RawDeleteResponse)
	put := []storage.Modify{
		{
			Data: storage.Put{
				Cf:    req.Cf,
				Key:   req.Key,
			},
		}}

	err := server.storage.Write(req.GetContext(), put)
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		resp.Error = err.Error()
		return resp, err
	}
	return resp, err

}

func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	resp := new(kvrpcpb.RawScanResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		resp.Error = err.Error()
		return resp, nil
	}
	defer reader.Close()
	iter := reader.IterCF(req.Cf)
	defer iter.Close()
	iter.Seek(req.GetStartKey())
	KV := make([]*kvrpcpb.KvPair, 0)
	n:=req.Limit
	for iter.Valid()&&n>0{
		item := iter.Item()
		val, err := item.ValueCopy(nil)
		if err != nil {
			resp.Error = err.Error()
			return resp, nil

		}
		KV = append(KV, &kvrpcpb.KvPair{
			Key:   item.KeyCopy(nil),
			Value: val,
		})
		n--
		iter.Next()
	}
	resp.Kvs=KV
	return resp,nil
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

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	resp := new(kvrpcpb.GetResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, nil
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.Version)
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, nil
	}
 if lock.IsLockedFor(req.Key,req.Version,resp){
 	return resp,nil
 }
	value, err := txn.GetValue(req.GetKey())
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, nil
	}
	if value == nil {
		resp.NotFound = true
	} else {
		resp.Value = value
	}
	return resp, nil
}
type Lear struct {
	Error *kvrpcpb.KeyError
}
func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	resp := new(kvrpcpb.PrewriteResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, nil
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, mutation := range req.Mutations {
		//1.check lock
		lock, err := txn.GetLock(mutation.Key)
		if err != nil {
			resp.RegionError = util.RaftstoreErrToPbError(err)
			return resp, nil
		}
		//var rsp kvrpcpb.PrewriteResponse
		var rsp Lear
		//kvrpcpb.PrewriteResponse 中error不一致，无法使用IsLockedFor
		if lock.IsLockedFor(mutation.Key,req.StartVersion,&rsp){
			resp.Errors = append(resp.Errors, rsp.Error)
			continue
		}
		//2.ckeck most recent write
		write, ts, err := txn.MostRecentWrite(mutation.Key)
		if err != nil {
			resp.RegionError = util.RaftstoreErrToPbError(err)
			return resp, nil
		}
		if write!=nil && txn.StartTS<=ts{
			resp.Errors = append(resp.Errors,&kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs: txn.StartTS,
					ConflictTs: ts,
					Key: mutation.Key,
					Primary: req.PrimaryLock,
				},
			} )
			continue
		}
		//put value
		switch mutation.Op {
		case kvrpcpb.Op_Put:
			txn.PutValue(mutation.Key, mutation.Value)
		case kvrpcpb.Op_Del:
			txn.DeleteValue(mutation.Key)
		}
		//put lock
		nlock := &mvcc.Lock{
			Primary: req.GetPrimaryLock(),
			Ts:      req.GetStartVersion(),
			Ttl:     req.GetLockTtl(),
		}
		Kind := mvcc.WriteKindFromProto(mutation.Op)
		nlock.Kind=Kind
		txn.PutLock(mutation.Key,nlock)
	}
//flush into storage
	 err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, nil
	}
	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	resp := new(kvrpcpb.CommitResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, nil
	}
	defer reader.Close()
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _,key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			resp.RegionError = util.RaftstoreErrToPbError(err)
			return resp, nil
		}
		if lock ==nil{
			continue
		}
		if lock.Ts!=txn.StartTS{
			 resp.Error=&kvrpcpb.KeyError{
				Retryable: "true",
				Locked: lock.Info(key),
			}
			return resp,nil
		}
		txn.PutWrite(key,req.CommitVersion,&mvcc.Write{txn.StartTS,lock.Kind})
		txn.DeleteLock(key)
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, nil
	}
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	resp := new(kvrpcpb.ScanResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, nil
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, txn)
   defer scanner.Close()
	limit := req.Limit
	for i := 0; i < int(limit); i++ {
		key, value, err := scanner.Next()
		if key==nil {
			break
		}
		var pair *kvrpcpb.KvPair
		if err==nil{
			pair=&kvrpcpb.KvPair{
				Key: key,
				Value: value,
			}
		}else{
			pair=&kvrpcpb.KvPair{
				Error: &kvrpcpb.KeyError{},
			}

		}
		if value!=nil{
		resp.Pairs = append(resp.Pairs, pair )}
	}
	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C)
	resp := new(kvrpcpb.CheckTxnStatusResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, nil
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.LockTs)
	write, TS, err := txn.MostRecentWrite(req.PrimaryKey)
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, nil
	}
	if write!=nil{
		if write.Kind!=mvcc.WriteKindRollback{
			resp.CommitVersion=TS
		}
		return resp,nil
	}
	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, nil
	}
      if lock!=nil {
		  //expired
		  if mvcc.PhysicalTime(req.CurrentTs) >= mvcc.PhysicalTime(lock.Ts)+lock.Ttl {
			  resp.Action = kvrpcpb.Action_TTLExpireRollback
			  locks, err := mvcc.AllLocksForTxn(txn)
			  if err != nil {
				  //panic(err.Error())
			  }
			  for _, lock := range locks {
				  txn.DeleteLock(lock.Key)
				  txn.DeleteValue(lock.Key)
			  }
			  write := &mvcc.Write{
				  StartTS: req.LockTs,
				  Kind:    mvcc.WriteKindRollback,
			  }
			  txn.PutWrite(req.PrimaryKey, req.LockTs, write)
			  err = server.storage.Write(req.Context, txn.Writes())
			  if err != nil {
				  resp.RegionError = util.RaftstoreErrToPbError(err)
				  return resp, nil
			  }
			  return resp, nil
		  }
		  resp.Action=kvrpcpb.Action_NoAction
		  return resp,nil
	  }
	if lock == nil {
		write := &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		}
		txn.PutWrite(req.PrimaryKey, req.LockTs,write)
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			resp.RegionError = util.RaftstoreErrToPbError(err)
			return resp, nil
		}
		resp.Action = kvrpcpb.Action_LockNotExistRollback
		return resp, nil
	}
	return resp,nil
	}


func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	resp := new(kvrpcpb.BatchRollbackResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, nil
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	for _, key := range req.Keys {
		write, _, err := txn.MostRecentWrite(key)
		if err != nil {
			resp.RegionError = util.RaftstoreErrToPbError(err)
			return resp, nil
		}
		if write != nil {
			//committed can't do roll back
			if write.Kind != mvcc.WriteKindRollback {
				resp.Error = &kvrpcpb.KeyError{Abort: "sur"}
				return resp, nil
			} else {
				continue
			}
		}
		lock, err := txn.GetLock(key)
		if err != nil {
			resp.RegionError = util.RaftstoreErrToPbError(err)
			return resp, nil
		}//can rollback
		if lock!=nil && lock.Ts==req.StartVersion{
			txn.DeleteValue(key)
			txn.DeleteLock(key)
		}
		put := &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    mvcc.WriteKindRollback,
		}
		txn.PutWrite(key, req.StartVersion,put)
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, nil
	}
	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	resp := new(kvrpcpb.ResolveLockResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, nil
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	locks, err := mvcc.AllLocksForTxn(txn)
	if err != nil {
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return resp, nil
	}
	var keys [][]byte
	for _, lock := range locks {
		keys = append(keys, lock.Key)
	}
	//if req.CommitVersion==0{
	//
	//}else{
	//
	//}
	if req.CommitVersion == 0 {
		newRollbackReq:=&kvrpcpb.BatchRollbackRequest{
			Context: req.Context,
			StartVersion: req.StartVersion,
			Keys:         keys,
		}
		temResp, err := server.KvBatchRollback(nil,newRollbackReq)
		resp.Error,resp.RegionError = temResp.Error,temResp.RegionError
		return resp, err
	} else {
		newcommitReq:=&kvrpcpb.CommitRequest{
			Context: req.Context,
			StartVersion: req.StartVersion,
			Keys:         keys,
			CommitVersion: req.CommitVersion,
		}
		temResp, err := server.KvCommit(nil,newcommitReq)
		resp.Error,resp.RegionError = temResp.Error,temResp.RegionError
		return resp, err
	}
	//err = server.storage.Write(req.Context, txn.Writes())
	//if err != nil {
	//	resp.RegionError = util.RaftstoreErrToPbError(err)
	//	return resp, nil
	//}
	//return resp, nil
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
