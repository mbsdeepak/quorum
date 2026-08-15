// Package raft implements the Raft consensus algorithm (Ongaro & Ousterhout,
// "In Search of an Understandable Consensus Algorithm"): leader election, log
// replication, crash-safe persistence, and log compaction via snapshots.
//
// A cluster of nodes elects a leader; the leader accepts commands, replicates
// them to followers, and marks an entry committed once a majority — a quorum —
// has stored it. Committed entries are delivered in order on applyCh, where the
// application feeds them to its state machine. In quorum, that state machine is
// the LSM key-value store, making the whole thing a replicated database.
//
// Snapshots keep the log from growing without bound: the application periodically
// hands Raft a snapshot of its state up to some index (Snapshot), and Raft
// discards the covered prefix. A follower that has fallen behind the leader's
// compacted prefix is caught up with InstallSnapshot instead of the log.
//
// Log indexing with snapshots: absolute index i maps to slice offset
// i-lastIncludedIndex. log[0] is a sentinel standing in for the snapshot
// boundary (its Term is lastIncludedTerm), and real entries follow it. All index
// arithmetic goes through the *Locked helpers to keep this in one place.
//
// Dynamic membership changes are the remaining Raft-completeness item (roadmap).
package raft

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	heartbeatInterval  = 50 * time.Millisecond
	electionTimeoutMin = 250 * time.Millisecond
	electionTimeoutMax = 450 * time.Millisecond
	tickInterval       = 15 * time.Millisecond
)

// Raft is one node in the cluster. It is safe for concurrent use; almost all
// state is guarded by mu.
type Raft struct {
	mu    sync.Mutex
	id    int
	peers []int // CURRENT cluster config (voting members); derived from the log
	// baseConfig is the config as of the snapshot boundary — the fallback when
	// the live log holds no configuration entry.
	baseConfig []int
	trans      Transport
	persister Persister
	applyCh   chan ApplyMsg
	applyCond *sync.Cond
	rnd       *rand.Rand

	// Persistent state.
	currentTerm       int
	votedFor          int // -1 when none
	log               []LogEntry
	lastIncludedIndex int // last index covered by the snapshot (0 = none)
	lastIncludedTerm  int
	snapshot          []byte // latest snapshot bytes, for serving InstallSnapshot

	// Volatile state on all nodes.
	role        Role
	commitIndex int
	lastApplied int

	// Volatile state on leaders (reinitialized after election).
	nextIndex  map[int]int
	matchIndex map[int]int

	// Election timing.
	lastHeard       time.Time
	electionTimeout time.Duration

	// snapPending holds a snapshot received via InstallSnapshot that the applier
	// must deliver to the application (ahead of any further commands).
	snapPending *ApplyMsg

	dead int32
}

// Make creates and starts a node, recovering any persisted state and snapshot.
func Make(id int, peers []int, trans Transport, persister Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		id:         id,
		peers:      append([]int(nil), peers...),
		baseConfig: append([]int(nil), peers...),
		trans:      trans,
		persister:  persister,
		applyCh:    applyCh,
		votedFor:   -1,
		log:        []LogEntry{{Term: 0}}, // sentinel at absolute index 0
		role:       Follower,
		nextIndex:  make(map[int]int),
		matchIndex: make(map[int]int),
		rnd:        rand.New(rand.NewSource(time.Now().UnixNano() ^ (int64(id+1) * 0x2545F4914F6CDD1D))),
	}
	rf.applyCond = sync.NewCond(&rf.mu)

	if b, err := persister.LoadState(); err == nil {
		if s, err := decodeState(b); err == nil && len(s.Log) > 0 {
			rf.currentTerm = s.CurrentTerm
			rf.votedFor = s.VotedFor
			rf.log = s.Log
			rf.lastIncludedIndex = s.LastIncludedIndex
			rf.lastIncludedTerm = s.LastIncludedTerm
			if s.BaseConfig != nil {
				rf.baseConfig = s.BaseConfig
			}
		}
	}
	rf.refreshConfigLocked() // derive the live config from the restored log
	if snap, err := persister.LoadSnapshot(); err == nil && len(snap) > 0 {
		rf.snapshot = snap
	}
	// Entries up to the snapshot are, by definition, committed and applied.
	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex
	// If we booted from a snapshot, hand it to the application so a volatile
	// state machine can rebuild before commands replay on top.
	if len(rf.snapshot) > 0 {
		rf.snapPending = &ApplyMsg{
			SnapshotValid: true,
			Snapshot:      append([]byte(nil), rf.snapshot...),
			SnapshotTerm:  rf.lastIncludedTerm,
			SnapshotIndex: rf.lastIncludedIndex,
		}
	}
	rf.resetElectionTimerLocked()

	go rf.ticker()
	go rf.applier()
	return rf
}

// ---- public API -----------------------------------------------------------

// Start proposes a command on the leader, returning the index it will occupy.
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

// Snapshot is called by the application once it has applied every entry through
// index and captured its state in snapshot. Raft discards the covered log prefix.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastIncludedIndex || index > rf.lastIndexLocked() {
		return // stale, or ahead of our log
	}
	off := index - rf.lastIncludedIndex
	rf.lastIncludedTerm = rf.log[off].Term
	newBase := rf.configAsOfLocked(index) // fold the config at the boundary into baseConfig
	// Rebuild the log as: sentinel(index) + entries after index.
	newLog := make([]LogEntry, 1, len(rf.log)-off)
	newLog[0] = LogEntry{Term: rf.lastIncludedTerm}
	newLog = append(newLog, rf.log[off+1:]...)
	rf.log = newLog
	rf.lastIncludedIndex = index
	rf.baseConfig = append([]int(nil), newBase...)
	rf.snapshot = append([]byte(nil), snapshot...)
	rf.refreshConfigLocked()
	rf.persistStateAndSnapshotLocked()
}

// ChangeConfig proposes a new cluster configuration (single-server add or remove
// — change membership by at most one node at a time). Leader-only, and rejected
// while a previous configuration change is still uncommitted, so at most one
// change is ever in flight (Raft dissertation §4.1). The new config takes effect
// on every node the moment it appends the entry, not when it commits.
func (rf *Raft) ChangeConfig(newConfig []int) (index int, ok bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader || rf.pendingConfigLocked() {
		return -1, false
	}
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Config: append([]int(nil), newConfig...)})
	rf.refreshConfigLocked()
	rf.persistLocked()
	index = rf.lastIndexLocked()
	rf.matchIndex[rf.id] = index
	go rf.broadcastAppendEntries()
	return index, true
}

// Config returns the node's current view of the cluster membership.
func (rf *Raft) Config() []int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return append([]int(nil), rf.peers...)
}

// Status reports the node's term, whether it is leader, and its applied index.
func (rf *Raft) Status() (term int, isLeader bool, lastApplied int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader, rf.lastApplied
}

// LogLength returns the number of in-memory log entries (post-compaction) — used
// by tests/metrics to observe that snapshots actually shrink the log.
func (rf *Raft) LogLength() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return len(rf.log) - 1
}

// SnapshotIndex returns the last index covered by the snapshot (0 = none).
func (rf *Raft) SnapshotIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastIncludedIndex
}

// Kill stops the node's goroutines. Persisted state is left intact.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.mu.Lock()
	rf.applyCond.Broadcast()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool { return atomic.LoadInt32(&rf.dead) == 1 }

// ---- election -------------------------------------------------------------

func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(tickInterval)
		rf.mu.Lock()
		// A node not in the current config (e.g. a fresh learner catching up, or
		// one that has been removed) must never start an election — it would only
		// disrupt the cluster and can never win a majority.
		if rf.role != Leader && rf.isMemberLocked(rf.id) && time.Since(rf.lastHeard) >= rf.electionTimeout {
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
	lastLogTerm := rf.termAtLocked(lastLogIndex)
	votes := 1

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
				return
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

	upToDate := rf.candidateUpToDateLocked(args.LastLogIndex, args.LastLogTerm)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && upToDate {
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
		rf.resetElectionTimerLocked()
	} else {
		reply.VoteGranted = false
	}
}

func (rf *Raft) candidateUpToDateLocked(candLastIndex, candLastTerm int) bool {
	myLastIndex := rf.lastIndexLocked()
	myLastTerm := rf.termAtLocked(myLastIndex)
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
	// Replicate to every tracked server, not just current voters: a server being
	// removed stays in matchIndex until the removal commits, so it keeps getting
	// the entries that tell it it has been removed (and thus stops campaigning).
	for peer := range rf.matchIndex {
		if peer == rf.id {
			continue
		}
		prevIndex := rf.nextIndex[peer] - 1
		if prevIndex < rf.lastIncludedIndex {
			// The entries this follower needs are already compacted away; ship
			// the snapshot instead of log entries.
			args := &InstallSnapshotArgs{
				Term:               term,
				LeaderID:           rf.id,
				LastIncludedIndex:  rf.lastIncludedIndex,
				LastIncludedTerm:   rf.lastIncludedTerm,
				LastIncludedConfig: append([]int(nil), rf.baseConfig...),
				Data:               append([]byte(nil), rf.snapshot...),
			}
			go rf.sendInstallSnapshotTo(peer, args, term)
			continue
		}
		args := &AppendEntriesArgs{
			Term:         term,
			LeaderID:     rf.id,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  rf.termAtLocked(prevIndex),
			Entries:      rf.entriesFromLocked(prevIndex + 1),
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
	rf.nextIndex[peer] = rf.backupIndexLocked(reply)
	if rf.nextIndex[peer] < 1 {
		rf.nextIndex[peer] = 1
	}
}

func (rf *Raft) sendInstallSnapshotTo(peer int, args *InstallSnapshotArgs, term int) {
	reply := &InstallSnapshotReply{}
	if !rf.trans.SendInstallSnapshot(peer, args, reply) {
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
	// The follower now holds everything through LastIncludedIndex.
	if args.LastIncludedIndex > rf.matchIndex[peer] {
		rf.matchIndex[peer] = args.LastIncludedIndex
	}
	rf.nextIndex[peer] = rf.matchIndex[peer] + 1
	rf.advanceCommitLocked()
}

func (rf *Raft) backupIndexLocked(reply *AppendEntriesReply) int {
	if reply.ConflictTerm == -1 {
		return reply.ConflictIndex
	}
	for i := rf.lastIndexLocked(); i > rf.lastIncludedIndex; i-- {
		if rf.termAtLocked(i) == reply.ConflictTerm {
			return i + 1
		}
	}
	return reply.ConflictIndex
}

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
	rf.role = Follower
	rf.resetElectionTimerLocked()
	reply.Term = rf.currentTerm

	// Entries at or below our snapshot boundary are already durable; treat the
	// boundary as the effective PrevLogIndex and skip the covered prefix.
	if args.PrevLogIndex < rf.lastIncludedIndex {
		skip := rf.lastIncludedIndex - args.PrevLogIndex
		if skip >= len(args.Entries) {
			// Everything the leader sent is already in our snapshot.
			reply.Success = true
			return
		}
		args.PrevLogIndex = rf.lastIncludedIndex
		args.PrevLogTerm = rf.lastIncludedTerm
		args.Entries = args.Entries[skip:]
	}

	last := rf.lastIndexLocked()
	if args.PrevLogIndex > last {
		reply.ConflictIndex = last + 1 // missing entries; ConflictTerm stays -1
		return
	}
	if rf.termAtLocked(args.PrevLogIndex) != args.PrevLogTerm {
		reply.ConflictTerm = rf.termAtLocked(args.PrevLogIndex)
		i := args.PrevLogIndex
		for i > rf.lastIncludedIndex+1 && rf.termAtLocked(i-1) == reply.ConflictTerm {
			i--
		}
		reply.ConflictIndex = i
		return
	}

	// Merge: skip the matching prefix, truncate at the first conflict, append
	// the rest. Never truncate on a mere prefix match (a stale duplicate could
	// otherwise erase committed entries).
	for i, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx <= rf.lastIndexLocked() {
			if rf.termAtLocked(idx) == e.Term {
				continue
			}
			rf.log = rf.log[:idx-rf.lastIncludedIndex]
		}
		rf.log = append(rf.log, args.Entries[i:]...)
		break
	}
	rf.refreshConfigLocked() // appended/truncated entries may change membership
	reply.Success = true

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastIndexLocked())
		rf.applyCond.Signal()
	}
}

// HandleInstallSnapshot installs a leader's snapshot when our needed log prefix
// has been compacted away on the leader.
func (rf *Raft) HandleInstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}
	rf.role = Follower
	rf.resetElectionTimerLocked()
	reply.Term = rf.currentTerm

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return // stale snapshot
	}

	// Keep any log entries we have beyond the snapshot (and that match at the
	// boundary); otherwise discard the whole log.
	if args.LastIncludedIndex <= rf.lastIndexLocked() &&
		rf.termAtLocked(args.LastIncludedIndex) == args.LastIncludedTerm {
		off := args.LastIncludedIndex - rf.lastIncludedIndex
		newLog := make([]LogEntry, 1, len(rf.log)-off)
		newLog[0] = LogEntry{Term: args.LastIncludedTerm}
		newLog = append(newLog, rf.log[off+1:]...)
		rf.log = newLog
	} else {
		rf.log = []LogEntry{{Term: args.LastIncludedTerm}}
	}
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	if args.LastIncludedConfig != nil {
		rf.baseConfig = append([]int(nil), args.LastIncludedConfig...)
	}
	rf.snapshot = append([]byte(nil), args.Data...)
	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	rf.refreshConfigLocked()
	rf.persistStateAndSnapshotLocked()

	// Queue the snapshot for the applier to deliver to the state machine.
	rf.snapPending = &ApplyMsg{
		SnapshotValid: true,
		Snapshot:      append([]byte(nil), args.Data...),
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.applyCond.Signal()
}

func (rf *Raft) advanceCommitLocked() {
	for N := rf.lastIndexLocked(); N > rf.commitIndex; N-- {
		if rf.termAtLocked(N) != rf.currentTerm {
			continue
		}
		count := 1
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

// applier delivers snapshots and committed commands to the application in order,
// sending on applyCh without holding the lock so a slow consumer can't stall Raft.
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for !rf.killed() && rf.snapPending == nil && rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}

		// A pending snapshot resets the state machine; deliver it first. Use >=
		// so a boot-from-snapshot (lastApplied already == snapshot index) still
		// rebuilds the state machine; a genuinely stale snapshot has a strictly
		// smaller index and is dropped.
		if rf.snapPending != nil {
			msg := *rf.snapPending
			rf.snapPending = nil
			if msg.SnapshotIndex >= rf.lastApplied {
				rf.lastApplied = msg.SnapshotIndex
				rf.mu.Unlock()
				rf.applyCh <- msg
				continue
			}
			rf.mu.Unlock()
			continue
		}

		var batch []ApplyMsg
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			if rf.lastApplied <= rf.lastIncludedIndex {
				continue // covered by a snapshot; skip
			}
			e := rf.log[rf.lastApplied-rf.lastIncludedIndex]
			if e.Config != nil {
				rf.applyConfigCommitLocked(e.Config)
				continue // membership entries aren't client commands
			}
			batch = append(batch, ApplyMsg{
				CommandValid: true,
				Command:      e.Command,
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

// ---- membership helpers ---------------------------------------------------

// currentConfigLocked derives the live cluster config: the newest config entry
// in the log, or baseConfig if the log holds none.
func (rf *Raft) currentConfigLocked() []int { return rf.configAsOfLocked(rf.lastIndexLocked()) }

// configAsOfLocked returns the config in effect at absolute index upto.
func (rf *Raft) configAsOfLocked(upto int) []int {
	for i := upto; i > rf.lastIncludedIndex; i-- {
		if e := rf.log[i-rf.lastIncludedIndex]; e.Config != nil {
			return e.Config
		}
	}
	return rf.baseConfig
}

// refreshConfigLocked recomputes peers from the log and makes sure leader
// bookkeeping has an entry for every current member. Call it after any change to
// the log or the snapshot boundary.
func (rf *Raft) refreshConfigLocked() {
	rf.peers = rf.currentConfigLocked()
	for _, p := range rf.peers {
		if _, ok := rf.nextIndex[p]; !ok {
			rf.nextIndex[p] = rf.lastIndexLocked() + 1
			rf.matchIndex[p] = 0
		}
	}
}

// applyConfigCommitLocked runs when a membership entry commits: prune bookkeeping
// for departed servers, and step down if this leader has removed itself (Raft
// dissertation §4.2.2 — the leader serves Cnew until it commits, then leaves).
func (rf *Raft) applyConfigCommitLocked(config []int) {
	for p := range rf.matchIndex {
		if !containsInt(config, p) {
			delete(rf.matchIndex, p)
			delete(rf.nextIndex, p)
		}
	}
	if rf.role == Leader && !containsInt(config, rf.id) {
		rf.becomeFollowerLocked(rf.currentTerm)
		rf.persistLocked()
	}
}

// pendingConfigLocked reports whether an uncommitted membership entry exists.
func (rf *Raft) pendingConfigLocked() bool {
	for i := rf.lastIndexLocked(); i > rf.commitIndex && i > rf.lastIncludedIndex; i-- {
		if rf.log[i-rf.lastIncludedIndex].Config != nil {
			return true
		}
	}
	return false
}

func (rf *Raft) isMemberLocked(id int) bool { return containsInt(rf.peers, id) }

func containsInt(s []int, x int) bool {
	for _, v := range s {
		if v == x {
			return true
		}
	}
	return false
}

// lastIndexLocked is the highest absolute index present (in log or snapshot).
func (rf *Raft) lastIndexLocked() int { return rf.lastIncludedIndex + len(rf.log) - 1 }

// termAtLocked returns the term of absolute index i, which must be in
// [lastIncludedIndex, lastIndex].
func (rf *Raft) termAtLocked(i int) int {
	if i == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	return rf.log[i-rf.lastIncludedIndex].Term
}

// entriesFromLocked returns a copy of the log entries from absolute index i on.
func (rf *Raft) entriesFromLocked(i int) []LogEntry {
	return append([]LogEntry(nil), rf.log[i-rf.lastIncludedIndex:]...)
}

func (rf *Raft) persistLocked() {
	b, err := encodeState(rf.stateLocked())
	if err != nil {
		return
	}
	_ = rf.persister.SaveState(b)
}

func (rf *Raft) persistStateAndSnapshotLocked() {
	b, err := encodeState(rf.stateLocked())
	if err != nil {
		return
	}
	_ = rf.persister.SaveStateAndSnapshot(b, rf.snapshot)
}

func (rf *Raft) stateLocked() persistentState {
	return persistentState{
		CurrentTerm:       rf.currentTerm,
		VotedFor:          rf.votedFor,
		Log:               rf.log,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		BaseConfig:        rf.baseConfig,
	}
}
