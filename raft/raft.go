// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"fmt"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"
)

type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0
const maxSize = 18446744073709551615

type VoteResult uint8

const (
	// VotePending indicates that the decision of the vote depends on future
	// votes, i.e. neither "yes" or "no" has reached quorum yet.
	VotePending VoteResult = 1 + iota
	// VoteLost indicates that the quorum has voted "no".
	VoteLost
	// VoteWon indicates that the quorum has voted "yes".
	VoteWon
)

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id                        uint64
	randomizedElectionTimeout int

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

func reset(r *Raft) {
	//for i, _ := range r.votes {
	//	r.votes[i]=false
	//}
	//r.PendingConfIndex = 0
	r.votes = make(map[uint64]bool)
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
	for i, _ := range r.Prs {
		r.Prs[i].Match = 0
		r.Prs[i].Next = r.RaftLog.LastIndex() + 1
	}
	if r.Prs[r.id] != nil {
		r.Prs[r.id].Match = r.RaftLog.LastIndex()
	}
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	log := newLog(c.Storage)
	//state, _, _ := c.Storage.InitialState()
	hardState, confState, _ := c.Storage.InitialState()
	//confState.Nodes
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	m := make(map[uint64]*Progress)
	vote := make(map[uint64]bool)
	if len(c.peers) <= 0 {
		c.peers = confState.Nodes
	}
	for _, j := range c.peers {
		m[j] = &Progress{Next: 0, Match: None}
	}
	r := &Raft{
		id:               c.ID,
		State:            StateFollower,
		Prs:              m,
		electionTimeout:  c.ElectionTick,
		votes:            vote,
		heartbeatTimeout: c.HeartbeatTick,
		RaftLog:          log,
	}
	reset(r)
	r.Vote = hardState.Vote
	r.Term = hardState.Term
	r.RaftLog.committed = hardState.Commit
	//r.RaftLog.committed = state.Commit
	hi, _ := c.Storage.LastIndex()
	r.RaftLog.stabled = hi
	if c.Applied!=0{
	r.RaftLog.applied = c.Applied
	}
	//r.RaftLog.committed =hi
	//lo, _ := c.Storage.firstIdx()
	//entries, err := c.Storage.Entries(lo, hi+1)
	//if err != nil {
	//	panic(err)
	//}
	//r.RaftLog.entries = append(r.RaftLog.entries, entries...)

	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	//log.Infof("%d send append to %d",r.id,to)
	progress := r.Prs[to]
	m := pb.Message{}
	m.To = to
	m.From = r.id
	term, err := r.RaftLog.Term(progress.Next - 1)
	if err != nil {
		if err == ErrCompacted {
			r.sendSnapshot(to)
			return false
		}
		panic(err)
	}
	//var entries []*pb.Entry
	//n := len(r.RaftLog.entries)
	//for i := r.RaftLog.toSliceIndex(r.Prs[to].Next); i < n; i++ {
	//	entries = append(entries, &r.RaftLog.entries[i])
	//}
	ents, _ := r.RaftLog.entry(progress.Next)
	m.MsgType = pb.MessageType_MsgAppend
	m.Index = progress.Next - 1
	m.LogTerm = term
	m.Entries = ents
	m.Commit = r.RaftLog.committed
	m.Term = r.Term
	r.msgs = append(r.msgs, m)
	if n := len(m.Entries); n != 0 {
		r.Prs[to].Next = m.Entries[n-1].Index + 1
	}
	return false
}

func (r *Raft) sendSnapshot(to uint64) {
	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		//shapshot still readying
		return
	}
	msg := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		From:     r.id,
		To:       to,
		Term:     r.Term,
		Snapshot: &snapshot,
	}
	log.Infof("%d sendSnapshot to %d", r.id, to)
	r.msgs = append(r.msgs, msg)
	r.Prs[to].Match = r.RaftLog.pendingSnapshot.GetMetadata().GetIndex()
	r.Prs[to].Next = snapshot.Metadata.Index + 1
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	if r.id == to {
		return
	}
	commit := min(r.Prs[to].Match, r.RaftLog.committed)
	r.msgs = append(r.msgs, pb.Message{From: r.id, To: to, Term: r.Term, Commit: commit, MsgType: pb.MessageType_MsgHeartbeat})
	// Your Code Here (2A).
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	switch r.State {
	case StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.randomizedElectionTimeout {
			r.electionElapsed = 0
			err := r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup})
			r.votes[r.id] = true
			if err != nil {
				return
			}
		}
	case StateFollower:
		r.electionElapsed++
		if r.electionElapsed >= r.randomizedElectionTimeout {
			r.electionElapsed = 0
			err := r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup})
			r.votes[r.id] = true
			if err != nil {
				return
			}
		}
	case StateLeader:
		r.heartbeatElapsed++
		r.electionElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.electionElapsed = 0
			err2 := r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgBeat})
			if err2 != nil {
				return
			}
		}

	}
	// Your Code Here (2A).
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.votes = make(map[uint64]bool)
	r.State = StateFollower
	r.Term = term
	r.Vote = None
	r.Lead = lead
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	log.Infof("%d become to follower(%d) vote(%d) lastIndex(%d)", r.id, r.Term, lead, r.RaftLog.LastIndex())

}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.State = StateCandidate
	r.Term += 1
	r.Vote = r.id
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	// Your Code Here (2A).
	log.Infof("%d goto election(%d)last=%d; %s", r.id, r.Term, r.RaftLog.LastIndex(), r.prs2string())

}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	reset(r)
	r.Lead = r.id
	r.State = StateLeader
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	emptyEnt := pb.Entry{Data: nil}
	r.appendEntry(emptyEnt)
	log.Infof("%d become to leader: term(%d)index(%d)", r.id, r.Term, r.RaftLog.LastIndex())

	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch {
	case m.Term > r.Term:
		r.leadTransferee = None
		if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgSnapshot {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None)
		}
	}

	switch m.MsgType {
	case pb.MessageType_MsgHup:
		if r.State == StateLeader {
			break
		}
		r.becomeCandidate()
		r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
		if len(r.Prs) == 0 {
			return nil
		}
		if len(r.Prs) == 1 {
			r.becomeLeader()
			for id := range r.Prs {
				if id == r.id {
					continue
				}
				r.sendAppend(id)
			}
			return nil
		}
		//res := r.poll(m)
		//switch res {
		//case VoteWon:
		//	r.becomeLeader()
		//	for id := range r.Prs {
		//		if id == r.id {
		//			continue
		//		}
		//		r.sendAppend(id)
		//	}
		//	return nil
		//}
		for u, _ := range r.Prs {
			if r.id == u {
				continue
			}
			term, err := r.RaftLog.Term(r.RaftLog.LastIndex())
			if err != nil {
				panic(err)
			}
			r.msgs = append(r.msgs, pb.Message{From: r.id, To: u, Term: r.Term, MsgType: pb.MessageType_MsgRequestVote, Index: r.RaftLog.LastIndex(), LogTerm: term})
		}
	case pb.MessageType_MsgRequestVote:

		//r.handleRequestVote(m)

		canvote := r.Vote == m.From ||
			(r.Vote == None && r.Lead == None)
		if canvote && r.RaftLog.isUpToDate(m.Index, m.LogTerm) {
			r.msgs = append(r.msgs, pb.Message{From: r.id, To: m.From, Term: m.Term, MsgType: pb.MessageType_MsgRequestVoteResponse})
			log.Debugf("%d vote %d-> %d for term(%d->%d)", r.id, r.Vote, m.From, r.Term, m.Term)
			r.electionElapsed = 0
			r.Vote = m.From
		} else {
			r.msgs = append(r.msgs, pb.Message{From: r.id, To: m.From, Term: m.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})
		}
	}
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgTimeoutNow:
			//judge if id was still in the raft group
			if _, ok := r.Prs[r.id]; ok {
				err := r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup})
				if err != nil {
					panic(err)
				}
			}
		case pb.MessageType_MsgSnapshot:
			r.electionElapsed = 0
			r.Lead = m.From
			r.handleSnapshot(m)
		case pb.MessageType_MsgAppend:
			r.electionElapsed = 0
			r.Lead = m.From
			r.handleAppendEntries(m)
		case pb.MessageType_MsgHeartbeat:
			r.electionElapsed = 0
			r.Lead = m.From
			r.handleHeartbeat(m)
		case pb.MessageType_MsgTransferLeader:
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHeartbeat:
			if m.Term == r.Term {
				r.becomeFollower(m.Term, m.From)
			}
			r.handleHeartbeat(m)
		case pb.MessageType_MsgSnapshot:
			r.becomeFollower(m.Term, m.From) // always m.Term == r.Term
			r.handleSnapshot(m)
		case pb.MessageType_MsgRequestVoteResponse:
			res := r.poll(m)
			switch res {
			case VoteWon:
				r.becomeLeader()
				for id := range r.Prs {
					if id == r.id {
						continue
					}
					r.sendAppend(id)
				}
			case VoteLost:
				r.becomeFollower(r.Term, None)
			}
		case pb.MessageType_MsgAppend:
			if m.Term == r.Term {
				r.becomeFollower(m.Term, m.From)
			}
			r.handleAppendEntries(m)
		case pb.MessageType_MsgTransferLeader:
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgTransferLeader:
			r.handleTransferLeader(m)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgBeat:
			for u, _ := range r.Prs {
				r.sendHeartbeat(u)
			}
			return nil
		case pb.MessageType_MsgHeartbeatResponse:
			//if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
			//	r.sendAppend(m.From)
			//}
			if m.Index < r.RaftLog.LastIndex() {
				r.sendAppend(m.From)
			}
		case pb.MessageType_MsgPropose:

			if r.leadTransferee != None {
				return nil
			}
			for _, entry := range m.Entries {
				entry.Term = r.Term
				entry.Index = r.RaftLog.LastIndex() + 1
				//get the first index of ConfChange
				if entry.EntryType == pb.EntryType_EntryConfChange {
					if r.PendingConfIndex >r.RaftLog.applied {
						entry.EntryType, entry.Data = pb.EntryType_EntryNormal, nil
					}else{
					r.PendingConfIndex = r.RaftLog.LastIndex()+1}
				}
				r.appendEntry(*entry)
			}
			for id := range r.Prs {
				if id == r.id {
					continue
				}
				//log.Infof("leader %s send append",r.id)
				r.sendAppend(id)
			}
		case pb.MessageType_MsgAppendResponse:
			//r.handleAppendEntriesResponse(m)
           if m.Term<r.Term{
           	return  nil
		   }
			if m.Reject {
				progress := r.Prs[m.From]
				//println(progress.Next)
				progress.Next = m.Index
				r.sendAppend(m.From)
			} else {

				if progress := r.Prs[m.From]; progress != nil {
					if progress.MaybeUpdate(m.Index) {
						if r.maybeCommit() {
							for id := range r.Prs {
								if id == r.id {
									continue
								}
								r.sendAppend(id)
							}
						}
						if progress.Match == r.RaftLog.LastIndex() && r.leadTransferee == m.GetFrom() {
							r.sendTimeoutNow(m.GetFrom())
						}
					}
				}
			}
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{To: m.From, From: m.To, Term: m.Term, MsgType: pb.MessageType_MsgAppendResponse, Index: 0})
		return
	}
	if m.Index < r.RaftLog.committed {
		r.msgs = append(r.msgs, pb.Message{To: m.From, From: m.To, Term: m.Term, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.committed})
		return
	}
	if m.Index > r.RaftLog.LastIndex(){
		r.msgs = append(r.msgs, pb.Message{To: m.From, From: m.To, Term: m.Term,Reject: true, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.committed+1})
		return
	}

	r.Lead=m.From
	//lastIndex := r.RaftLog.LastIndex()
	//if m.Index > lastIndex {
	//	r.sendAppendResponse(m.From, true, None, lastIndex+1)
	//	return
	//}
	//if m.Index >= firstIndex {
	//	logTerm, err := r.RaftLog.Term(m.Index)
	//	if err != nil {
	//		panic(err)
	//	}
	//	if logTerm != m.LogTerm {
	//		index := r.RaftLog.toEntryIndex(sort.Search(r.RaftLog.toSliceIndex(m.Index+1),
	//			func(i int) bool { return r.RaftLog.entries[i].Term == logTerm }))
	//		r.sendAppendResponse(m.From, true, logTerm, index)
	//		return
	//	}
	//}
	term, _ := r.RaftLog.Term(m.Index)
	if term == m.LogTerm {
		//var conIndex uint64 = 0
		var ent []pb.Entry
		for _, entry := range m.Entries {
			ent = append(ent, *entry)
		}
		lctIndex := m.Index + uint64(len(m.Entries))
		for pos, entry := range m.Entries {
			if entry.Index < r.RaftLog.firstIdx {
				continue
			}
			if entry.Index <= r.RaftLog.LastIndex() {
				u, _ := r.RaftLog.Term(entry.Index)
				if u != entry.Term {
					//conIndex = entry.GetIndex()
					i := entry.Index - r.RaftLog.firstIdx
					r.RaftLog.entries[i] = *entry
					r.RaftLog.entries = r.RaftLog.entries[:i+1]
					r.RaftLog.stabled = min(r.RaftLog.stabled, entry.Index-1)
				}
			} else {
				r.RaftLog.entries = append(r.RaftLog.entries, ent[pos:]...)
				break
			}
		}
		//switch {
		//case conIndex == 0:
		//default:
		//	var ent []pb.Entry
		//	for _, entry := range m.Entries {
		//		ent = append(ent, *entry)
		//	}
		//	offset := m.Index + 1
		//	//r.RaftLog.entries = append(r.RaftLog.entries, ent[conIndex-offset:]...)
		//	r.RaftLog.truncateAndAppend(ent[conIndex-offset:])
		//}
		if r.RaftLog.committed < min(m.Commit, lctIndex) {
			r.RaftLog.committed = min(m.Commit, lctIndex)
		}
		r.msgs = append(r.msgs, pb.Message{To: m.From, From: m.To, Term: m.Term, MsgType: pb.MessageType_MsgAppendResponse, Index: lctIndex})
	} else {
		//fmt.println("reject msgapp")
		hintIndex := min(m.Index, r.RaftLog.LastIndex())
		hintIndex = r.RaftLog.findConflictByTerm(hintIndex, m.LogTerm)
		hintTerm, _ := r.RaftLog.Term(hintIndex)
		r.msgs = append(r.msgs, pb.Message{
			From:    m.To,
			Term:    m.Term,
			To:      m.From,
			MsgType: pb.MessageType_MsgAppendResponse,
			Index:   m.Index,
			Reject:  true,
			LogTerm: hintTerm,
		})
	}
	//
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	//if r.RaftLog.committed < m.Commit {
	//	r.RaftLog.committed = m.Commit
	//}
	if m.Term < r.Term {

		r.msgs = append(r.msgs, pb.Message{From: r.id, Reject: true, To: m.From, MsgType: pb.MessageType_MsgHeartbeatResponse})
	}
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{From: r.id, To: m.From, Index: m.Index, MsgType: pb.MessageType_MsgHeartbeatResponse})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	metadata := m.Snapshot.Metadata
	if m.Term<r.Term  {
		r.msgs = append(r.msgs, pb.Message{
			From:    r.id,
			Term:    r.Term,
			To:      m.From,
			MsgType: pb.MessageType_MsgAppendResponse,
			Index:   r.RaftLog.committed,
			Reject:  false,
			LogTerm: None,
		})
		return
	}
	r.becomeFollower(max(r.Term, m.Term), m.From)
	if m.Snapshot.Metadata.Index > r.RaftLog.committed {
		if r.RaftLog.pendingSnapshot == nil || m.Snapshot.Metadata.Index > r.RaftLog.pendingSnapshot.Metadata.Index {
			r.RaftLog.pendingSnapshot = m.Snapshot
		}

	log := r.RaftLog
	r.RaftLog.firstIdx=metadata.GetIndex()+1
	log.entries = log.entries[:0]
	log.applied = metadata.GetIndex()
	log.committed = log.applied
	log.stabled = log.applied}
	r.Prs = make(map[uint64]*Progress)
	for _, peer := range metadata.ConfState.Nodes {
		r.Prs[peer] = &Progress{}
	}
	// Your Code Here (2C).
	//r.msgs = append(r.msgs, pb.Message{
	//	From:    r.id,
	//	Term:    r.Term,
	//	To:      m.From,
	//	MsgType: pb.MessageType_MsgAppendResponse,
	//	Index:   r.RaftLog.LastIndex(),
	//	Reject:  false,
	//	LogTerm: None,
	//})
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	log.Infof("add new node %d", id)
	if _, ok := r.Prs[id]; !ok {
		r.Prs[id] = &Progress{
			Match: 0,
			Next:  1,
		}
	}
	//r.PendingConfIndex = None
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; ok {
		delete(r.Prs, id)
		log.Infof("remove  node %d", id)
		if r.State == StateLeader {
			r.maybeCommit()
			for id := range r.Prs {
				if id == r.id {
					continue
				}
				//log.Infof("leader %s send append",r.id)
				r.sendAppend(id)
			}
		}
	}
	//r.PendingConfIndex = None
}
func (r *Raft) poll(m pb.Message) VoteResult {
	if m.Term != None && m.Term < r.Term {
		return VotePending
	}
	if !m.Reject {
		//log.Infof("%d get vote from %d", r.id, m.From)

	}
	r.votes[m.From] = !m.Reject
	//if len(r.votes) >= len(r.Prs)/2+1 {
	agg := 0
	for _, b := range r.votes {
		if b == true {
			agg++
		}
	}
	if agg > len(r.Prs)/2 {
		return VoteWon
	} else if len(r.votes)-agg > (len(r.Prs) / 2) {
		return VoteLost
	} else {
		return VotePending
	}
	//}
	return VotePending
}
func (r *Raft) reset() {
	r.votes = make(map[uint64]bool)
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}
func (r *Raft) appendEntry(es ...pb.Entry) {
	li := r.RaftLog.LastIndex()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = li + 1 + uint64(i)
		r.RaftLog.entries = append(r.RaftLog.entries, es[i])

	}
	if progress := r.Prs[r.id]; progress != nil {
		progress.MaybeUpdate(r.RaftLog.LastIndex())
	}
	r.maybeCommit()
}
func (pr *Progress) MaybeUpdate(n uint64) bool {
	var updated bool
	if pr.Match < n {
		pr.Match = n
		updated = true
	}
	pr.Next = max(pr.Next, n+1)
	return updated
}
func (r *Raft) maybeCommit() bool {
	n := len(r.Prs)
	if n == 0 {
		return true
	}
	srt := make([]uint64, n)
	i := n - 1
	for _, progress := range r.Prs {
		srt[i] = progress.Match
		i--
	}
	insertionSort(srt)
	commitind := srt[n-(n/2+1)]
	term, err := r.RaftLog.Term(commitind)
	if err != nil {
		//log.Error(err)
	}
	if commitind > r.RaftLog.committed && term == r.Term {
		r.RaftLog.committed = commitind
		return true
	}
	return false
}
func insertionSort(sl []uint64) {
	a, b := 0, len(sl)
	for i := a + 1; i < b; i++ {
		for j := i; j > a && sl[j] < sl[j-1]; j-- {
			sl[j], sl[j-1] = sl[j-1], sl[j]
		}
	}
}
func (r *Raft) softState() *SoftState {
	return &SoftState{Lead: r.Lead, RaftState: r.State}
}
func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

//func (r *Raft) advance(rd Ready) {
//}
//
//func (l *RaftLog) hasNextEnts() bool {
//	off := max(l.applied+1, l.firstIndex())
//	return l.committed+1 > off
//}
//
func (r *Raft) prs2string() string {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("Prs(%d){", len(r.Prs)))
	for id, pr := range r.Prs {
		builder.WriteString(fmt.Sprintf("%d:{%d,%d},", id, pr.Match, pr.Next))
	}
	builder.WriteByte('}')
	return builder.String()
}

//
func (r *Raft) sendAppendResponse(to uint64, reject bool, term, index uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
		LogTerm: term,
		Index:   index,
	}
	r.msgs = append(r.msgs, msg)
}

//func (r *Raft) handleRequestVote(m pb.Message) {
//	if m.Term != None && m.Term < r.Term {
//		r.sendRequestVoteResponse(m.From, true)
//		return
//	}
//	if r.Vote != None && r.Vote != m.From {
//		r.sendRequestVoteResponse(m.From, true)
//		return
//	}
//	lastIndex := r.RaftLog.LastIndex()
//	lastLogTerm, _ := r.RaftLog.Term(lastIndex)
//	if lastLogTerm > m.LogTerm ||
//		lastLogTerm == m.LogTerm && lastIndex > m.Index {
//		r.sendRequestVoteResponse(m.From, true)
//		return
//	}
//	r.Vote = m.From
//	r.electionElapsed = 0
//	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
//	r.sendRequestVoteResponse(m.From, false)
//}
//func (r *Raft) sendRequestVoteResponse(to uint64, reject bool) {
//	msg := pb.Message{
//		MsgType: pb.MessageType_MsgRequestVoteResponse,
//		From:    r.id,
//		To:      to,
//		Term:    r.Term,
//		Reject:  reject,
//	}
//	r.msgs = append(r.msgs, msg)
//}
func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if m.Term != None && m.Term < r.Term {
		return
	}
	if m.Reject {
		index := m.Index
		if index == None {
			return
		}
		if m.LogTerm != None {
			logTerm := m.LogTerm
			l := r.RaftLog
			sliceIndex := sort.Search(len(l.entries),
				func(i int) bool { return l.entries[i].Term > logTerm })
			if sliceIndex > 0 && l.entries[sliceIndex-1].Term == logTerm {
				index = l.toEntryIndex(sliceIndex)
			}
		}
		r.Prs[m.From].Next = index
		r.sendAppend(m.From)
		return
	}
	if m.Index > r.Prs[m.From].Match {
		r.Prs[m.From].Match = m.Index
		r.Prs[m.From].Next = m.Index + 1
		r.leaderCommit()
	}
}

func (r *Raft) leaderCommit() {
	match := make(uint64Slice, len(r.Prs))
	i := 0
	for _, prs := range r.Prs {
		match[i] = prs.Match
		i++
	}
	sort.Sort(match)
	n := match[(len(r.Prs)-1)/2]

	if n > r.RaftLog.committed {
		logTerm, err := r.RaftLog.Term(n)
		if err != nil {
			panic(err)
		}
		if logTerm == r.Term {
			r.RaftLog.committed = n
			r.bcastAppend()
		}
	}
}
func (r *Raft) bcastAppend() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendAppend(peer)
	}
}

func (r *Raft) handleTransferLeader(m pb.Message) {
	log.Infof("handleTransferLeader")
	if r.id == m.From {
		return
	}
	if r.leadTransferee != None {
		if m.From == r.leadTransferee {
			return
		}
	}
	if _, ok := r.Prs[m.From]; !ok {
		log.Infof("no such perr %d", m.From)
		return
	}
	r.leadTransferee = m.From
	if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	} else {
		r.sendTimeoutNow(m.From)
	}
}

func (r *Raft) sendTimeoutNow(to uint64) {
	r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgTimeoutNow, From: r.id, To: to})
	r.leadTransferee = None
}
