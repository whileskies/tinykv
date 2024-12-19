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

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
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
	offset uint64 // entries[i] index: offset + i
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}

	return &RaftLog{
		storage:   storage,
		stabled:   lastIndex,
		offset:    lastIndex + 1,
		committed: firstIndex - 1,
		applied:   firstIndex - 1,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

func (l *RaftLog) storageFirstIndex() uint64 {
	firstIndex, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	return firstIndex
}

func (l *RaftLog) storageLastIndex() uint64 {
	lastIndex, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}

	return lastIndex
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).

	allEntries, err := l.storage.Entries(l.storageFirstIndex(), l.storageLastIndex()+1)
	if err != nil {
		panic(err)
	}

	allEntries = append(allEntries, l.entries...)

	filtered := make([]pb.Entry, 0)

	for _, e := range allEntries {
		if e.Term != 0 {
			filtered = append(filtered, e)
		}
	}

	return filtered
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if ln := len(l.entries); ln != 0 {
		return l.offset + uint64(ln) - 1
	}

	return l.storageLastIndex()
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i > l.LastIndex() {
		return 0, ErrUnavailable
	} else if i >= l.offset {
		return l.entries[i-l.offset].Term, nil
	} else {
		return l.storage.Term(i)
	}
}

func (l *RaftLog) slice(lo uint64, hi uint64) ([]pb.Entry, error) {
	if lo > hi {
		panic("lo > hi")
	}

	if lo == hi {
		return make([]pb.Entry, 0), nil
	}

	if lo >= l.offset {
		return l.unstableSlice(lo, hi), nil
	}

	cut := min(hi, l.offset)
	ents, err := l.storage.Entries(lo, cut)
	if err != nil {
		return nil, err
	}

	if hi <= l.offset {
		return ents, nil
	}

	return append(ents, l.unstableSlice(l.offset, hi)...), nil
}

func (l *RaftLog) unstableSlice(lo uint64, hi uint64) []pb.Entry {
	if lo > hi {
		panic("lo > hi")
	}
	if lo == hi {
		return make([]pb.Entry, 0)
	}

	return l.entries[lo-l.offset : hi-l.offset : hi-l.offset]
}

func (l *RaftLog) append(ents ...pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.LastIndex()
	}

	l.truncatedAndAppend(ents)
	return l.LastIndex()
}

func (l *RaftLog) truncatedAndAppend(ents []pb.Entry) {
	startIndex := ents[0].Index

	if startIndex == l.offset+uint64(len(l.entries)) {
		l.entries = append(l.entries, ents...)
	} else if startIndex <= l.offset {
		l.entries = ents
		l.offset = startIndex
	} else {
		keep := l.unstableSlice(l.offset, startIndex)
		l.entries = append(keep, ents...)
	}
}

func (l *RaftLog) findConflict(entries []pb.Entry) uint64 {
	for _, ent := range entries {
		index := ent.Index
		term, err := l.Term(index)
		if err != nil || ent.Term != term {
			return index
		}
	}

	return 0
}

func (l *RaftLog) maybeAppend(prevIndex, prevTerm, leaderCommit uint64, entries []pb.Entry) (uint64, bool) {
	term, err := l.Term(prevIndex)
	if err != nil || term != prevTerm {
		return 0, false
	}

	ci := l.findConflict(entries)
	start := prevIndex + 1
	l.truncatedAndAppend(entries[ci-start:])

	lastIndex := prevIndex + uint64(len(entries))

	l.committed = min(leaderCommit, lastIndex)
	return lastIndex, true
}
