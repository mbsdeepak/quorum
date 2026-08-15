// Package raft implements the Raft consensus algorithm (Ongaro & Ousterhout,
// "In Search of an Understandable Consensus Algorithm"): leader election, log
// replication, and crash-safe persistence.
//
// A cluster of nodes elects a leader; the leader accepts commands, replicates
// them to followers, and marks an entry committed once a majority — a quorum —
// has stored it. Committed entries are delivered in order on applyCh, where the
// application feeds them to its state machine. In quorum, that state machine is
// the LSM key-value store, making the whole thing a replicated database.
//
// This package covers elections + replication + persistence. Snapshots and
// dynamic membership changes are on the roadmap.
package raft

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// Timing. The election timeout must be comfortably larger than the heartbeat
// interval so a live leader is never spuriously replaced (Raft §5.6).
const (
	heartbeatInterval  = 50 * time.Millisecond
	electionTimeoutMin = 250 * time.Millisecond
	electionTimeoutMax = 450 * time.Millisecond
	tickInterval       = 15 * time.Millisecond
)

// Raft is one node in the cluster. It is safe for concurrent use; almost all
// state is guarded by mu.
type Raft struct {
	mu        sync.Mutex
	id        int
	peers     []int // all node ids, including this one
	trans     Transport
	persister Persister
	applyCh   chan ApplyMsg
	applyCond *sync.Cond
	rnd       *rand.Rand

	// Persistent state (saved before responding to RPCs).
	currentTerm int
	votedFor    int // -1 when none
	log         []LogEntry

	// Volatile state on all nodes.
	role        Role
	commitIndex int
	lastApplied int

	// Volatile state on leaders (reinitialized after election).
	nextIndex  map[int]int
	matchIndex map[int]int

	// Election timing.
	lastHeard      time.Time
	electionTimeout time.Duration

	dead int32
}

// Make creates and starts a node. peers lists every node id (including id).
// If the persister holds prior state, the node recovers it before starting.
func Make(id int, peers []int, trans Transport, persister Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		id:         id,
		peers:      append([]int(nil), peers...),
		trans:      trans,
		persister:  persister,
		applyCh:    applyCh,
		votedFor:   -1,
		log:        []LogEntry{{Term: 0}}, // index 0 is a sentinel; real entries start at 1
		role:       Follower,
		nextIndex:  make(map[int]int),
		matchIndex: make(map[int]int),
		// Decorrelate each node's RNG: seeds that differ by only 1 (nodes built
		// in a tight loop) yield correlated timeout streams and permanent split
		// votes. XOR-ing a large per-id constant gives independent streams.
		rnd: rand.New(rand.NewSource(time.Now().UnixNano() ^ (int64(id+1) * 0x2545F4914F6CDD1D))),
	}
	rf.applyCond = sync.NewCond(&rf.mu)

	if b, err := persister.Load(); err == nil {
		if s, err := decodeState(b); err == nil && len(s.Log) > 0 {
			rf.currentTerm = s.CurrentTerm
			rf.votedFor = s.VotedFor
			rf.log = s.Log
		}
	}
	rf.resetElectionTimerLocked()

	go rf.ticker()
	go rf.applier()
	return rf
}

// ---- public API -----------------------------------------------------------

// Start proposes a command. If this node is the leader it appends the command
// to its log, kicks off replication, and returns the index it will occupy once
// committed. Otherwise isLeader is false and the caller should retry elsewhere.
func (rf *Raft) Start(command []byte) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		return -1, rf.currentTerm, false
	}
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: append([]byte(nil), command...)})
	rf.persistLocked()
	index = rf.lastIndexLocked()
	rf.matchIndex[rf.id] = index
	go rf.broadcastAppendEntries()
	return index, rf.currentTerm, true
}

// Status reports the node's term, whether it believes it is leader, and how far
// its state machine has been applied. Handy for tests and for a future admin API.
func (rf *Raft) Status() (term int, isLeader bool, lastApplied int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader, rf.lastApplied
}

// Kill stops the node's goroutines. It does not erase persisted state.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.mu.Lock()
	rf.applyCond.Broadcast()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool { return atomic.LoadInt32(&rf.dead) == 1 }

// ---- election -------------------------------------------------------------

// ticker triggers an election whenever this node hasn't heard from a leader (or
// granted a vote) within its randomized election timeout.
func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(tickInterval)
		rf.mu.Lock()
		if rf.role != Leader && time.Since(rf.lastHeard) >= rf.electionTimeout {
			rf.startElectionLocked()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) startElectionLocked() {
	rf.role = Candidate
	rf.currentTerm++
	rf.votedFor = rf.id
	rf.persistLocked()
	rf.resetElectionTimerLocked()

	term := rf.currentTerm
	lastLogIndex := rf.lastIndexLocked()
	lastLogTerm := rf.log[lastLogIndex].Term
	votes := 1 // vote for self

	for _, peer := range rf.peers {
		if peer == rf.id {
			continue
		}
		go func(peer int) {
			args := &RequestVoteArgs{Term: term, CandidateID: rf.id, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
			reply := &RequestVoteReply{}
			if !rf.trans.SendRequestVote(peer, args, reply) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.currentTerm != term || rf.role != Candidate {
				return // stale reply
			}
			if reply.Term > rf.currentTerm {
				rf.becomeFollowerLocked(reply.Term)
				rf.persistLocked()
				return
			}
			if reply.VoteGranted {
				votes++
				if votes > len(rf.peers)/2 {
					rf.becomeLeaderLocked()
				}
			}
		}(peer)
	}
}

// HandleRequestVote decides whether to grant a candidate our vote for its term.
func (rf *Raft) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persistLocked()

	if args.Term < rf.currentTerm {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}
	reply.Term = rf.currentTerm

	// Grant only if we haven't voted for someone else this term AND the
	// candidate's log is at least as up to date as ours (§5.4.1).
	upToDate := rf.candidateUpToDateLocked(args.LastLogIndex, args.LastLogTerm)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && upToDate {
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
		rf.resetElectionTimerLocked()
	} else {
		reply.VoteGranted = false
	}
}

// candidateUpToDateLocked implements Raft's log-comparison rule: a longer last
// term wins; on equal terms, the longer log wins.
func (rf *Raft) candidateUpToDateLocked(candLastIndex, candLastTerm int) bool {
	myLastIndex := rf.lastIndexLocked()
	myLastTerm := rf.log[myLastIndex].Term
	if candLastTerm != myLastTerm {
		return candLastTerm > myLastTerm
	}
	return candLastIndex >= myLastIndex
}

func (rf *Raft) becomeLeaderLocked() {
	rf.role = Leader
	last := rf.lastIndexLocked()
	for _, peer := range rf.peers {
		rf.nextIndex[peer] = last + 1
		rf.matchIndex[peer] = 0
	}
	rf.matchIndex[rf.id] = last
	go rf.leaderLoop(rf.currentTerm)
}

func (rf *Raft) becomeFollowerLocked(term int) {
	rf.currentTerm = term
	rf.role = Follower
	rf.votedFor = -1
}

func (rf *Raft) resetElectionTimerLocked() {
	rf.lastHeard = time.Now()
	d := electionTimeoutMax - electionTimeoutMin
	rf.electionTimeout = electionTimeoutMin + time.Duration(rf.rnd.Int63n(int64(d)))
}

// ---- replication ----------------------------------------------------------

// leaderLoop sends heartbeats/entries on a fixed cadence for as long as this
// node remains leader in the term it was elected.
func (rf *Raft) leaderLoop(term int) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		rf.broadcastAppendEntries()
		time.Sleep(heartbeatInterval)
	}
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	term := rf.currentTerm
	for _, peer := range rf.peers {
		if peer == rf.id {
			continue
		}
		prevIndex := rf.nextIndex[peer] - 1
		if prevIndex < 0 {
			prevIndex = 0
		}
		args := &AppendEntriesArgs{
			Term:         term,
			LeaderID:     rf.id,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  rf.log[prevIndex].Term,
			Entries:      append([]LogEntry(nil), rf.log[prevIndex+1:]...),
			LeaderCommit: rf.commitIndex,
		}
		go rf.sendAppendEntriesTo(peer, args, term)
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntriesTo(peer int, args *AppendEntriesArgs, term int) {
	reply := &AppendEntriesReply{}
	if !rf.trans.SendAppendEntries(peer, args, reply) {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm != term || rf.role != Leader {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.becomeFollowerLocked(reply.Term)
		rf.persistLocked()
		return
	}
	if reply.Success {
		match := args.PrevLogIndex + len(args.Entries)
		if match > rf.matchIndex[peer] {
			rf.matchIndex[peer] = match
		}
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		rf.advanceCommitLocked()
		return
	}
	// Rejected: use the conflict hint to back up nextIndex quickly.
	rf.nextIndex[peer] = rf.backupIndexLocked(reply)
	if rf.nextIndex[peer] < 1 {
		rf.nextIndex[peer] = 1
	}
}

// backupIndexLocked turns a follower's conflict hint into the next index to try.
func (rf *Raft) backupIndexLocked(reply *AppendEntriesReply) int {
	if reply.ConflictTerm == -1 {
		// Follower's log is too short; jump straight to its end.
		return reply.ConflictIndex
	}
	// If the leader has the conflicting term, resume just past its last entry
	// in that term; otherwise fall back to the follower's first index for it.
	for i := rf.lastIndexLocked(); i > 0; i-- {
		if rf.log[i].Term == reply.ConflictTerm {
			return i + 1
		}
	}
	return reply.ConflictIndex
}

// HandleAppendEntries is the follower side of replication and heartbeats.
func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persistLocked()

	reply.Success = false
	reply.ConflictTerm, reply.ConflictIndex = -1, -1

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}
	// Valid current-term leader: (re)affirm follower status and stay alive.
	rf.role = Follower
	rf.resetElectionTimerLocked()
	reply.Term = rf.currentTerm

	// Consistency check: our log must contain PrevLogIndex with PrevLogTerm.
	last := rf.lastIndexLocked()
	if args.PrevLogIndex > last {
		reply.ConflictIndex = last + 1 // we're missing entries; ConflictTerm stays -1
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		i := args.PrevLogIndex
		for i > 0 && rf.log[i-1].Term == reply.ConflictTerm {
			i--
		}
		reply.ConflictIndex = i
		return
	}

	// Merge entries: skip the matching prefix, truncate on the first conflict,
	// then append the remainder. Never truncate on a mere prefix match, or a
	// stale/duplicated AppendEntries could erase committed entries.
	for i, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx <= rf.lastIndexLocked() {
			if rf.log[idx].Term == e.Term {
				continue
			}
			rf.log = rf.log[:idx]
		}
		rf.log = append(rf.log, args.Entries[i:]...)
		break
	}
	reply.Success = true

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastIndexLocked())
		rf.applyCond.Signal()
	}
}

// advanceCommitLocked advances commitIndex to the highest N replicated on a
// majority — but only for an entry from the current term. Committing a
// prior-term entry by vote count alone is unsafe (Raft §5.4.2); it becomes
// committed indirectly once a current-term entry above it commits.
func (rf *Raft) advanceCommitLocked() {
	for N := rf.lastIndexLocked(); N > rf.commitIndex; N-- {
		if rf.log[N].Term != rf.currentTerm {
			continue
		}
		count := 1 // self
		for _, peer := range rf.peers {
			if peer != rf.id && rf.matchIndex[peer] >= N {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = N
			rf.applyCond.Signal()
			return
		}
	}
}

// ---- apply loop -----------------------------------------------------------

// applier delivers committed entries to the application in index order. It
// sends on applyCh without holding the lock so a slow consumer can't stall Raft.
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex && !rf.killed() {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		var batch []ApplyMsg
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			batch = append(batch, ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			})
		}
		rf.mu.Unlock()

		for _, m := range batch {
			rf.applyCh <- m
		}
	}
}

// ---- helpers --------------------------------------------------------------

func (rf *Raft) lastIndexLocked() int { return len(rf.log) - 1 }

func (rf *Raft) persistLocked() {
	b, err := encodeState(persistentState{
		CurrentTerm: rf.currentTerm,
		VotedFor:    rf.votedFor,
		Log:         rf.log,
	})
	if err != nil {
		return
	}
	_ = rf.persister.Save(b)
}
