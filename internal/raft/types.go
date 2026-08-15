package raft

// Role is a node's current position in the Raft state machine.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// LogEntry is one replicated entry tagged with the term it was created in (the
// term is what lets nodes detect and resolve divergent histories). Most entries
// carry a client Command; a membership-change entry instead carries Config, the
// new set of node ids. A node adopts a Config entry as soon as it appends it —
// not when it commits — which is what makes single-server changes safe.
type LogEntry struct {
	Term    int
	Command []byte
	Config  []int // non-nil ⇒ this is a configuration-change entry
}

// ApplyMsg carries either a committed command or an installed snapshot up to the
// application. Exactly one of CommandValid / SnapshotValid is true. Commands
// arrive in CommandIndex order; a snapshot means "discard your state and reset
// to this point" (used when a follower was too far behind to catch up via the
// log, because the leader had already compacted the entries it needed).
type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// RequestVoteArgs / RequestVoteReply — the RPC a candidate uses to gather votes
// (Raft paper §5.2, §5.4.1).
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// AppendEntriesArgs / AppendEntriesReply — the RPC a leader uses to replicate
// entries and to send heartbeats (empty Entries). ConflictTerm/ConflictIndex
// implement the fast-backup optimization so a lagging follower is caught up in
// O(terms) round-trips instead of O(entries) (Raft paper §5.3).
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

// InstallSnapshotArgs / InstallSnapshotReply — a leader ships its snapshot to a
// follower whose needed log prefix has already been compacted away (Raft §7).
// This implementation sends the snapshot in a single message rather than in
// chunks; chunking is a straightforward extension.
type InstallSnapshotArgs struct {
	Term               int
	LeaderID           int
	LastIncludedIndex  int
	LastIncludedTerm   int
	LastIncludedConfig []int // cluster config as of the snapshot boundary
	Data               []byte
}

type InstallSnapshotReply struct {
	Term int
}
