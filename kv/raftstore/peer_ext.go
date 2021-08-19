package raftstore

import (
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
)


func (d *peerMsgHandler) HandleEntryNormal(curProposal *proposal, ent eraftpb.Entry) {
	resp := newCmdResp()
	BindRespTerm(resp, d.Term())
	req := new(raft_cmdpb.RaftCmdRequest)
	if err := req.Unmarshal(ent.Data); err != nil {
		panic(err.Error())
	}
	// Check whether the Epoch is legal before apply
	region := d.Region()
	if err := util.CheckRegionEpoch(req, region, true); err != nil {
		if curProposal != nil {
			curProposal.cb.Done(ErrResp(err))
		}
		return
	}
	needTxn := false
	if req.AdminRequest != nil {
		// Execute AdminRequest
		resp.AdminResponse = &raft_cmdpb.AdminResponse{
			CmdType: req.AdminRequest.CmdType,
		}
		switch req.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_CompactLog:
			// Modify metadata: update RaftTruncatedState
			// May solve "no need to gc"
			if d.peerStorage.applyState.AppliedIndex < req.AdminRequest.CompactLog.CompactIndex {
				if term, err := d.RaftGroup.Raft.RaftLog.Term(d.peerStorage.applyState.AppliedIndex); err != nil {
					panic(err.Error())
				} else {
					req.AdminRequest.CompactLog.CompactIndex = d.peerStorage.applyState.AppliedIndex
					req.AdminRequest.CompactLog.CompactTerm = term
				}
			}
			resp.AdminResponse.CompactLog = &raft_cmdpb.CompactLogResponse{}
			if d.peerStorage.applyState.TruncatedState.Index < req.AdminRequest.CompactLog.CompactIndex {
				d.peerStorage.applyState.TruncatedState = &rspb.RaftTruncatedState{
					Index: req.AdminRequest.CompactLog.CompactIndex,
					Term:  req.AdminRequest.CompactLog.CompactTerm,
				}
				// log.Infof("Compact Idx=%v, Term=%v", d.peerStorage.applyState.TruncatedState.Index, d.peerStorage.applyState.TruncatedState.Term)
				// Schedule a task to raftlog-gc worker
				d.ScheduleCompactLog(req.AdminRequest.CompactLog.CompactIndex)
			}
		case raft_cmdpb.AdminCmdType_Split:
			if err := util.CheckKeyInRegion(req.AdminRequest.Split.SplitKey, region); err != nil {
				// Split has applied
				if curProposal != nil {
					curProposal.cb.Done(ErrResp(err))
					return
				}
			} else {
				// Update version, the version of original and new region
				// equals to the version of original version + new region count
				// Update Region State
				firstRegion, secondRegion := &metapb.Region{}, &metapb.Region{}
				if err := util.CloneMsg(region, firstRegion); err != nil {
					panic(err.Error())
				}
				util.CloneMsg(region, secondRegion)
				firstRegion.EndKey = req.AdminRequest.Split.SplitKey
				firstRegion.RegionEpoch.Version++
				secondRegion.StartKey = req.AdminRequest.Split.SplitKey
				secondRegion.Id = req.AdminRequest.Split.NewRegionId
				secondRegion.RegionEpoch.Version++
				for i, v := range req.AdminRequest.Split.NewPeerIds {
					secondRegion.Peers[i].Id = v
				}
				// Set region in PeerStorage, GlobalCtx, Response, RegionLocalState
				d.SetRegion(firstRegion)
				d.ctx.storeMeta.Lock()
				d.ctx.storeMeta.regionRanges.Delete(&regionItem{region})
				d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{firstRegion})
				d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{secondRegion})
				d.ctx.storeMeta.regions[firstRegion.Id] = firstRegion
				d.ctx.storeMeta.regions[secondRegion.Id] = secondRegion
				d.ctx.storeMeta.Unlock()
				resp.AdminResponse.Split = &raft_cmdpb.SplitResponse{
					Regions: []*metapb.Region{firstRegion, secondRegion},
				}
				kvRegionWB := engine_util.WriteBatch{}
				d.peerStorage.SaveRegionLocalState(&rspb.RegionLocalState{
					State:  rspb.PeerState_Normal,
					Region: firstRegion,
				}, &kvRegionWB)
				kvRegionWB.Reset()
				d.peerStorage.SaveRegionLocalState(&rspb.RegionLocalState{
					State:  rspb.PeerState_Normal,
					Region: secondRegion,
				}, &kvRegionWB)
				// Update Region Size
				d.SizeDiffHint = 0
				d.ApproximateSize = new(uint64)
				// Initial local state and apply state will created by NewPeerStorage()
				// Apply Region Split
				d.onReadySplitRegion(secondRegion)
			}
		}
	}
	kvApplyWB := engine_util.WriteBatch{}
	// Set WriteBatch
	for _, v := range req.Requests {
		switch v.CmdType {
		case raft_cmdpb.CmdType_Put:
			if err := util.CheckKeyInRegion(v.Put.Key, d.Region()); err != nil {
				if curProposal != nil {
					curProposal.cb.Done(ErrResp(err))
				}
				return
			}
			kvApplyWB.SetCF(v.Put.Cf, v.Put.Key, v.Put.Value)
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{CmdType: raft_cmdpb.CmdType_Put, Put: &raft_cmdpb.PutResponse{}})
		case raft_cmdpb.CmdType_Delete:
			if err := util.CheckKeyInRegion(v.Delete.Key, d.Region()); err != nil {
				if curProposal != nil {
					curProposal.cb.Done(ErrResp(err))
				}
				return
			}
			kvApplyWB.DeleteCF(v.Delete.Cf, v.Delete.Key)
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{CmdType: raft_cmdpb.CmdType_Delete, Delete: &raft_cmdpb.DeleteResponse{}})
		case raft_cmdpb.CmdType_Snap:
			// Check region epoch before get Txn
			if req.Header.RegionEpoch.Version != d.Region().RegionEpoch.Version {
				if curProposal != nil {
					curProposal.cb.Done(ErrResp(&util.ErrEpochNotMatch{}))
				}
				return
			}
			needTxn = true
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{CmdType: raft_cmdpb.CmdType_Snap, Snap: &raft_cmdpb.SnapResponse{Region: d.Region()}})
		case raft_cmdpb.CmdType_Get:
			if err := util.CheckKeyInRegion(v.Get.Key, d.Region()); err != nil {
				if curProposal != nil {
					curProposal.cb.Done(ErrResp(err))
				}
				return
			}
			val, err := engine_util.GetCF(d.peerStorage.Engines.Kv, v.Get.Cf, v.Get.Key)
			if err != nil {
				if curProposal != nil {
					curProposal.cb.Done(ErrResp(err))
				}
				return
			}
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{CmdType: raft_cmdpb.CmdType_Get, Get: &raft_cmdpb.GetResponse{Value: val}})
		}
	}
	// Apply to KvDB, get Txn handler
	if err := kvApplyWB.WriteToDB(d.peerStorage.Engines.Kv); err != nil {
		if curProposal != nil {
			curProposal.cb.Done(ErrResp(err))
		}
		return
	}
	if needTxn {
		if curProposal != nil {
			curProposal.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
		}
	}
	if curProposal != nil {
		curProposal.cb.Done(resp)
	}
}

func (d *peerMsgHandler) HandleEntryConf(curProposal *proposal, ent eraftpb.Entry) bool {
	resp := newCmdResp()
	BindRespTerm(resp, d.Term())
	kvApplyWB := engine_util.WriteBatch{}
	// Unmarshal ConfChange and RaftCmdRequest
	cc := &eraftpb.ConfChange{}
	if err := cc.Unmarshal(ent.Data); err != nil {
		panic(err.Error())
	}
	msg := &raft_cmdpb.RaftCmdRequest{}
	if err := msg.Unmarshal(cc.Context); err != nil {
		panic(err.Error())
	}
	// log.Infof("%v Apply ConfChange %v, ID %v", d.PeerId(), cc.ChangeType, cc.NodeId)
	// Update RegionLocalState, Region
	regionLocal := &rspb.RegionLocalState{
		State: rspb.PeerState_Normal,
	}
	if cc.ChangeType == eraftpb.ConfChangeType_RemoveNode && cc.NodeId == d.PeerId() {
		regionLocal.State = rspb.PeerState_Tombstone
	}
	newRegion := &metapb.Region{}
	if err := util.CloneMsg(d.Region(), newRegion); err != nil {
		panic(err.Error())
	}
	newRegion.RegionEpoch.ConfVer++
	switch cc.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		newRegion.Peers = append(newRegion.Peers, msg.AdminRequest.ChangePeer.Peer)
	case eraftpb.ConfChangeType_RemoveNode:
		if res := util.RemovePeer(newRegion, msg.AdminRequest.ChangePeer.Peer.StoreId); res == nil {
			panic("Can't find the store to remove")
		}
		d.removePeerCache(cc.NodeId)
	}
	// Set region in PeerStorage, GlobalCtx, Response, RegionLocalState
	d.SetRegion(newRegion)
	d.ctx.storeMeta.Lock()
	d.ctx.storeMeta.regions[d.regionId] = newRegion
	d.ctx.storeMeta.Unlock()
	regionLocal.Region = newRegion
	resp.AdminResponse = &raft_cmdpb.AdminResponse{
		CmdType: raft_cmdpb.AdminCmdType_ChangePeer,
		ChangePeer: &raft_cmdpb.ChangePeerResponse{
			Region: newRegion,
		},
	}
	// Apply ConfChange in Raft
	d.RaftGroup.ApplyConfChange(*cc)
	// Write RegionLocalState to KvDB
	d.peerStorage.SaveRegionLocalState(regionLocal, &kvApplyWB)
	if curProposal != nil {
		curProposal.cb.Done(resp)
	}
	// Destroy peer in RemoveNode
	if cc.ChangeType == eraftpb.ConfChangeType_RemoveNode && cc.NodeId == d.PeerId() {
		d.destroyPeer()
		return true
	}
	return false
}

func (d *peerMsgHandler) HandleRaftReady1() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	// log.Infof("Into HandleRaftReady %v", d.peer.RaftGroup.Raft.GetId())
	if !d.RaftGroup.HasReady() {
		return
	}
	rd := d.RaftGroup.Ready()
	// Save Log Entries, RaftLocalState, RaftApplyState, RegionLocalState
	if applyRes, err := d.peerStorage.SaveReadyState(&rd); err != nil {
		panic(err.Error())
	} else {
		if applyRes != nil {
			region := d.Region()
			d.ctx.storeMeta.Lock()
			d.ctx.storeMeta.regions[d.regionId] = region
			d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
			d.ctx.storeMeta.Unlock()
			d.peerCache = make(map[uint64]*metapb.Peer)
			for _, pr := range region.Peers {
				d.insertPeerCache(pr)
			}
		}
	}
	// Send Raft messages
	d.Send(d.ctx.trans, rd.Messages)
	// Apply log entries
	for _, ent := range rd.CommittedEntries {
		d.peerStorage.applyState.AppliedIndex = ent.Index
		if ent.Data == nil {
			continue
		}
		// Find callback, execute request and send response
		var curProposal *proposal = nil
		// In ManyPartitions, the index in proposals may not be consecutive
		for i, prop := range d.proposals {
			if ent.Index == prop.index {
				// Reject stale callbacks
				if prop.term < d.Term() {
					prop.cb.Done(ErrRespStaleCommand(d.Term()))
				} else {
					curProposal = prop
				}
				d.proposals = d.proposals[i+1:]
				break
			}
		}

		switch ent.EntryType {
		case eraftpb.EntryType_EntryNormal:
			d.HandleEntryNormal(curProposal, ent)
		case eraftpb.EntryType_EntryConfChange:
			if destroy := d.HandleEntryConf(curProposal, ent); destroy {
				return
			}
		}
	}
	// log.Infof("Apply Ok %v", d.peer.RaftGroup.Raft.GetId())
	// Update ApplyState
	d.peerStorage.SaveApplied()
	// Advance raftFsm
	d.RaftGroup.Advance(rd)
	// log.Infof("Send Ok %v", d.peer.RaftGroup.Raft.GetId())
}


// Return false if ConfChange has applied
func (d *peerMsgHandler) validateChangePeer(req *raft_cmdpb.RaftCmdRequest) bool {
	if req.AdminRequest != nil && req.AdminRequest.CmdType == raft_cmdpb.AdminCmdType_ChangePeer {
		switch req.AdminRequest.ChangePeer.ChangeType {
		case eraftpb.ConfChangeType_AddNode:
			// Store to add has added
			if util.FindPeer(d.Region(), req.AdminRequest.ChangePeer.Peer.StoreId) != nil {
				return false
			}
		case eraftpb.ConfChangeType_RemoveNode:
			// Store to remove has removed
			if util.FindPeer(d.Region(), req.AdminRequest.ChangePeer.Peer.StoreId) == nil {
				return false
			}
		}
	}
	return true
}

// Create peers of new region, register on router
func (d *peerMsgHandler) onReadySplitRegion(second *metapb.Region) {
	// Create peer
	peer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, second)
	if err != nil {
		panic(err.Error())
	}
	// Register on router
	d.ctx.router.register(peer)
	d.ctx.router.send(second.Id, message.Msg{RegionID: second.Id, Type: message.MsgTypeStart})
	// Report to scheduler
	if d.IsLeader() {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
}
func (ps *PeerStorage) SaveRegionLocalState(state *rspb.RegionLocalState, kvWB *engine_util.WriteBatch) {
	kvWB.SetMeta(meta.RegionStateKey(state.Region.Id), state)
	kvWB.MustWriteToDB(ps.Engines.Kv)
}


func (ps *PeerStorage) SaveApplied() {
	kvWB := engine_util.WriteBatch{}
	kvWB.SetMeta(meta.ApplyStateKey(ps.region.Id), ps.applyState)
	if err := kvWB.WriteToDB(ps.Engines.Kv); err != nil {
		panic(err.Error())
	}
}


