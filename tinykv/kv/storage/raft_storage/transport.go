package raft_storage

import (
	"sync"

	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/util/worker"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
)

type ServerTransport struct {
	raftClient        *RaftClient
	raftRouter        message.RaftRouter
	resolverScheduler chan<- worker.Task
	snapScheduler     chan<- worker.Task
	resolving         sync.Map
}

func NewServerTransport(raftClient *RaftClient, snapScheduler chan<- worker.Task, raftRouter message.RaftRouter, resolverScheduler chan<- worker.Task) *ServerTransport {
	return &ServerTransport{
		raftClient:        raftClient,
		raftRouter:        raftRouter,
		resolverScheduler: resolverScheduler,
		snapScheduler:     snapScheduler,
	}
}

func (t *ServerTransport) Send(msg *raft_serverpb.RaftMessage) error {
	storeID := msg.GetToPeer().GetStoreId()
	t.SendStore(storeID, msg)
	return nil
}

func (t *ServerTransport) SendStore(storeID uint64, msg *raft_serverpb.RaftMessage) {
	addr := t.raftClient.GetAddr(storeID)
	if addr != "" {
		t.WriteData(storeID, addr, msg)
		return
	}
	if _, ok := t.resolving.Load(storeID); ok {
		log.Debugf("store address is being resolved, msg dropped. storeID: %v, msg: %s", storeID, msg)
		return
	}
	log.Debug("begin to resolve store address. storeID: %v", storeID)
	t.resolving.Store(storeID, struct{}{})
	t.Resolve(storeID, msg)
}

func (t *ServerTransport) Resolve(storeID uint64, msg *raft_serverpb.RaftMessage) {
	callback := func(addr string, err error) {
		// clear resolving
		t.resolving.Delete(storeID)
		if err != nil {
			log.Errorf("resolve store address failed. storeID: %v, err: %v", storeID, err)
			return
		}
		t.raftClient.InsertAddr(storeID, addr)
		t.WriteData(storeID, addr, msg)
		t.raftClient.Flush()
	}
	t.resolverScheduler <- &resolveAddrTask{
		storeID:  storeID,
		callback: callback,
	}
}

func (t *ServerTransport) WriteData(storeID uint64, addr string, msg *raft_serverpb.RaftMessage) {
	if msg.GetMessage().GetSnapshot() != nil {
		t.SendSnapshotSock(addr, msg)
		return
	}
	if err := t.raftClient.Send(storeID, addr, msg); err != nil {
		log.Errorf("send raft msg err. err: %v", err)
	}
}

func (t *ServerTransport) SendSnapshotSock(addr string, msg *raft_serverpb.RaftMessage) {
	callback := func(err error) {
		regionID := msg.GetRegionId()
		toPeerID := msg.GetToPeer().GetId()
		toStoreID := msg.GetToPeer().GetStoreId()
		log.Debugf("send snapshot. toPeerID: %v, toStoreID: %v, regionID: %v, status: %v", toPeerID, toStoreID, regionID, err)
	}

	t.snapScheduler <- &sendSnapTask{
		addr:     addr,
		msg:      msg,
		callback: callback,
	}
}

func (t *ServerTransport) Flush() {
	t.raftClient.Flush()
}
