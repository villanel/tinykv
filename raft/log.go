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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)
import "log"

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	offset uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	if storage == nil {
		log.Panic("storage must not be nil")
	}
	log := &RaftLog{
		storage: storage,
	}
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	log.offset = lastIndex + 1
	log.committed = firstIndex - 1
	log.applied = firstIndex - 1

	return log
	return nil
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	//if l.stabled>=l.offset{
	//	l.offset = l.stabled + 1
	//	return  l.entries[l.stabled+1-l.offset:]
	//}
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return nil
	}

	entries := l.entries[l.offset-1:]

	return entries
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	off := max(l.applied+1, l.FirstIndex())
	if l.committed+1 > off {
		hi := l.committed + 1
		lo := off
		var ents []pb.Entry
		//if lo < l.offset {
		//	storedEnts, _ := l.storage.Entries(lo, min(hi, l.offset))
		//	for _, ent := range storedEnts {
		//		ents = append(ents, ent)
		//	}
		//}
		//if hi >l.offset{
		//	entries := l.entries[max(lo, l.offset)-l.offset:hi-l.offset]
		//	for _, entry := range entries {
		//		ents = append(ents, entry)
		//	}
		for _, entry := range l.entries[lo-1 : hi-1] {
			ents = append(ents, entry)

		}
		return ents
	}
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	//lastIndex, _ := l.storage.LastIndex()
	//if lastIndex >=l.offset{
	//	l.offset=lastIndex+1
	//}

	// Your Code Here (2A).
	if len := len(l.entries); len != 0 {
		//return l.offset + uint64(len) - 1
		return uint64(len)
	}
	index, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	// Your Code Here (2A).
	return index
}
func (l *RaftLog) FirstIndex() uint64 {
	index, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	// Your Code Here (2A).
	return index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	dummyIndex := l.FirstIndex() - 1
	if i < dummyIndex || i > l.LastIndex() {
		return 0, nil
	}
	if t, ok := l.unstableTerm(i); ok {
		return t, nil
	}

	t, err := l.storage.Term(i)
	if err == nil {
		return t, nil
	}
	if err == ErrCompacted || err == ErrUnavailable {
		return 0, err
	}
	panic(err)
}

func (l *RaftLog) entry(lo uint64) ([]*pb.Entry, error) {
	if lo > l.LastIndex() {
		return nil, nil
	}
	hi := l.LastIndex()
	var ents []*pb.Entry
	//if lo < l.offset {
	//	storedEnts, _ := l.storage.Entries(lo, min(hi, l.offset))
	//	for _, ent := range storedEnts {
	//		ents = append(ents, &ent)
	//	}
	//}
	//if hi >l.offset{
	//	entries := l.entries[max(lo, l.offset)-l.offset:hi-l.offset]
	//		for _, entry := range entries {
	//			ents = append(ents, &entry)
	//		}
	//}
	entries := l.entries[lo-1 : hi]
	for i := 0; i < len(entries); i++ {
		ents = append(ents, &entries[i])
	}
	return ents, nil
}

func (l *RaftLog) unstableTerm(i uint64) (uint64, bool) {
	if i < l.offset {
		return 0, false
	}

	last := l.LastIndex()
	if i > last {
		return 0, false
	}

	return l.entries[i-1].Term, true
}
func (l *RaftLog) findConflictByTerm(index uint64, term uint64) uint64 {
	if li := l.LastIndex(); index > li {
		// NB: such calls should not exist, but since there is a straightfoward
		// way to recover, do it.
		//
		// It is tempting to also check something about the first index, but
		// there is odd behavior with peers that have no log, in which case
		// lastIndex will return zero and firstIndex will return one, which
		// leads to calls with an index of zero into this method.
		return index
	}
	for {
		logTerm, err := l.Term(index)
		if logTerm <= term || err != nil {
			break
		}
		index--
	}
	return index
}
func (l *RaftLog) truncateAndAppend(ents []pb.Entry) {
	after := ents[0].Index
	switch {
	case uint64(len(l.entries))+1 == after:
		// after is the next index in the u.entries
		// directly append
		l.entries = append(l.entries, ents...)
	case after <= l.offset:
		// The log is being truncated to before our current offset
		// portion, so set the offset and replace the entries
		l.offset = after
		l.entries = l.entries[:l.offset-1]
		l.entries = append(l.entries, ents...)
	default:
		if after > l.LastIndex() {

			l.entries = append(l.entries, ents...)
		} else {
			// truncate to after and copy to u.entries
			// then append
			l.entries = append([]pb.Entry{}, l.entries[0:after-l.offset]...)
			l.entries = append(l.entries, ents...)
		}
	}
}
func (l *RaftLog) isUpToDate(lasti, term uint64) bool {
	lastTerm, err := l.Term(l.LastIndex())
	if err != nil {
		panic(err)
	}

	return term > lastTerm || (term == lastTerm && lasti >= l.LastIndex())
}
