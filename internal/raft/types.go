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

// LogEntry is one replicated command tagged with the term it was created in.
// The term is what lets nodes detect and resolve divergent histories.
type LogEntry struct {
	Term    int
	Command []byte
}

// ApplyMsg is handed to the application each time an entry is committed, i.e.
// safely replicated on a majority. The application applies Command to its state
// machine (here, the LSM store) in CommandIndex order.
type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex int
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
