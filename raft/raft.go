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
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

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
	id uint64

	peers []uint64

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

	randomElectionTimeout int // 随机选举超时

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

	Logger Logger
}

func (r *Raft) raftStat() string {
	return fmt.Sprintf("[id:%d term:%d, %s]", r.id, r.Term, r.State)
}

func assert(prec bool) {
	if !prec {
		panic("assert error")
	}
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	raft := Raft{
		id:               c.ID,
		Vote:             None,
		peers:            c.peers,
		Lead:             None,
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		RaftLog: &RaftLog{
			storage: c.Storage,
			applied: c.Applied,
		},
	}

	raft.Logger = defaultLogger
	// raft.Logger = discardLogger

	hardStat, _, err := raft.RaftLog.storage.InitialState()
	if err != nil {
		panic(err.Error())
	}

	raft.Term = hardStat.Term
	raft.Vote = hardStat.Vote
	raft.RaftLog.committed = hardStat.Commit

	// if len(raft.peers) == 1 {
	// 	raft.becomeLeader()
	// } else {
	// 	raft.becomeFollower(raft.Term, raft.Lead)
	// }
	raft.becomeFollower(raft.Term, raft.Lead)
	raft.Logger.Debug("new Raft, become Follower")

	return &raft
}

func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}

	r.resetVotes()

	r.Lead = None
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.resetRandomElectionTimeout()
}

func (r *Raft) resetVotes() {
	r.votes = make(map[uint64]bool)
}

func (r *Raft) resetRandomElectionTimeout() {
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	})
}

func (r *Raft) sendRequestVote(to uint64) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	})
}

func (r *Raft) sendRequestVoteResponse(vote bool, to uint64) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  !vote,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.doHeartbeatTick()
	r.doElectionTick()
}

func (r *Raft) sendHeartbeatBroadcast() {
	for _, p := range r.peers {
		if p == r.id {
			continue
		}
		r.sendHeartbeat(p)
	}
}

func (r *Raft) doHeartbeatTick() {
	if r.State != StateLeader {
		return
	}
	r.heartbeatElapsed += 1
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.sendHeartbeatBroadcast()
		r.heartbeatElapsed = 0
	}
}

func (r *Raft) doElectionTick() {
	if r.State == StateLeader {
		return
	}
	r.electionElapsed += 1
	if r.electionElapsed >= r.randomElectionTimeout {
		r.startRequestVote()
		r.electionElapsed = 0
	}
}

func (r *Raft) startRequestVote() {
	r.becomeCandidate()
	r.votes[r.id] = true

	if r.candidateCanBeLeader() {
		r.becomeLeader()
		return
	}

	for _, p := range r.peers {
		if p == r.id {
			continue
		}
		r.sendRequestVote(p)
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.reset(term)
	r.Lead = lead
	r.State = StateFollower

	r.Logger.Infof("%x became follower at term %d", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.reset(r.Term + 1)
	r.State = StateCandidate

	r.Logger.Infof("%x became candidate at term %d", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.reset(r.Term)
	r.State = StateLeader

	r.Logger.Infof("%x became leader at term %d", r.id, r.Term)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).

	if m.Term != 0 && m.Term < r.Term { // term == 0, local message
		return nil
	} else if m.Term > r.Term {
		r.Logger.Infof("%x [term: %d] received a %s message with higher term from %d [term: %d]", r.id, r.Term, m.From, m.Term)

		if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat {
			// from leader
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None)
		}
	}

	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleMsgHup(m)

	default:
		switch r.State {
		case StateFollower:
			return r.followerStep(m)
		case StateCandidate:
			return r.candidateStep(m)
		case StateLeader:
			return r.leaderStep(m)
		}
	}

	return nil
}

// Follower handle messages
func (r *Raft) followerStep(m pb.Message) error {

	switch m.MsgType {
	case pb.MessageType_MsgAppend:
		// append
		// r.RaftLog.storage.

	case pb.MessageType_MsgRequestVote:
		return r.followerHandleRequestVote(m)

	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	}
	return nil
}

func (r *Raft) canVoteFor(candiateId uint64) bool {
	if r.Vote == None || r.Vote == candiateId {
		return true
	}
	return false
}

func (r *Raft) followerHandleRequestVote(m pb.Message) error {
	if r.canVoteFor(m.From) {
		r.Vote = m.From
		r.sendRequestVoteResponse(true, m.From)
	} else {
		r.sendRequestVoteResponse(false, m.From)
	}
	return nil
}

// Candidate handle messages
func (r *Raft) candidateStep(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)

	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	}

	return nil
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	r.votes[m.From] = !m.Reject

	if r.candidateCanBeLeader() {
		r.becomeLeader()
	}

}

func (r *Raft) candidateCanBeLeader() bool {
	voteCnt := 0
	for _, v := range r.votes {
		if v {
			voteCnt += 1
		}
	}

	return voteCnt > len(r.peers)/2
}

// Leader handle messages
func (r *Raft) leaderStep(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgAppend:
		assert(m.Term != r.Term)
		// append

	case pb.MessageType_MsgBeat:
		return r.leaderHandleBeat(m)
	}

	return nil
}

func (r *Raft) leaderHandleBeat(m pb.Message) error {
	r.sendHeartbeatBroadcast()
	return nil
}

func (r *Raft) handleMsgHup(m pb.Message) {
	if r.State == StateLeader {
		r.Logger.Info("%s ignore MsgHup", r.raftStat())
		return
	}
	if r.id == m.From && r.id == m.To {
		r.startRequestVote()
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// r.resetRandomElectionTimeout()
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.electionElapsed = 0
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
	})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
