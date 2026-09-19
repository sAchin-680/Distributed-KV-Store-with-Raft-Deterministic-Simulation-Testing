package raft

import (
	"errors"
	"fmt"
)

// ErrNotLeader is returned by Propose on a node that is not the leader. The
// caller should redirect to Lead(), or retry once a leader is known.
var ErrNotLeader = errors.New("raft: not the leader")

// defaultMaxEntriesPerMessage bounds how many entries ride in one AppendEntries.
// Without a cap, a follower that is far behind provokes a single message the
// size of the entire log.
const defaultMaxEntriesPerMessage = 64

// Propose appends a command to the log and starts replicating it.
//
// It returns the index the command was assigned. That index is a promise about
// *ordering*, not about durability: the entry is committed only once a quorum
// has it and the commit rule allows it. Callers that need to know the command
// took effect must wait for that index to be applied.
func (r *RawNode) Propose(cmd []byte) (Index, error) {
	return r.propose(LogEntry{Type: EntryNormal, Command: cmd})
}

func (r *RawNode) propose(e LogEntry) (Index, error) {
	if r.state != Leader {
		return 0, ErrNotLeader
	}

	e.Term = r.term
	e.Index = r.log.lastIndex() + 1
	if err := r.log.append([]LogEntry{e}); err != nil {
		return 0, err
	}

	// The leader's own log counts toward the quorum, so record it immediately.
	// In a single-node cluster this is already a majority and the entry commits
	// without a single message being sent.
	self := r.progress[r.id]
	self.Match = e.Index
	self.Next = e.Index + 1

	if _, err := r.maybeAdvanceCommit(); err != nil {
		return 0, err
	}
	r.broadcastAppend()
	return e.Index, nil
}

// ---------------------------------------------------------------------------
// Sending
// ---------------------------------------------------------------------------

func (r *RawNode) broadcastAppend() {
	for _, id := range r.conf.Peers(r.id) {
		r.sendAppend(id)
	}
}

// sendAppend sends whatever this peer is missing, starting at its Next.
//
// With no entries to send this is a heartbeat, which is the same message with an
// empty payload — the paper deliberately gives them one form, so that every
// heartbeat also carries a consistency check and the current commit index.
func (r *RawNode) sendAppend(to NodeID) {
	pr, ok := r.progress[to]
	if !ok {
		return
	}

	prevIndex := pr.Next - 1
	prevTerm, err := r.log.term(prevIndex)
	if err != nil {
		// The entries this peer needs have been compacted away; it needs a
		// snapshot rather than entries. Until snapshot transfer exists, leave
		// it behind rather than send something it cannot check.
		return
	}

	var entries []LogEntry
	if last := r.log.lastIndex(); pr.Next <= last {
		hi := min(last+1, pr.Next+Index(r.maxEntriesPerMessage()))
		entries, err = r.log.entries(pr.Next, hi)
		if err != nil {
			return
		}
	}

	// Next is deliberately not advanced here.
	//
	// Advancing optimistically would avoid resending entries that are already in
	// flight, and a throughput-oriented implementation does exactly that. It also
	// means Next no longer reflects what the follower has confirmed, so every
	// rejection path has to unwind it correctly. Resending is wasteful; getting
	// the unwind wrong is a correctness bug. Until there is a measurement saying
	// otherwise, this takes the waste.
	r.send(Message{
		Type:         MsgAppendReq,
		To:           to,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: r.log.committed,
	})
}

func (r *RawNode) maxEntriesPerMessage() int {
	if r.cfg.MaxEntriesPerMessage > 0 {
		return r.cfg.MaxEntriesPerMessage
	}
	return defaultMaxEntriesPerMessage
}

// ---------------------------------------------------------------------------
// Receiving
// ---------------------------------------------------------------------------

func (r *RawNode) handleAppendRequest(m Message) error {
	// Hearing from the leader of our term is what keeps us from campaigning.
	r.electionElapsed = 0

	if r.state != Follower {
		// A leader has been elected in our term, so our campaign is over.
		r.becomeFollower(m.Term, m.From)
	}
	r.lead = m.From

	lastNew, ok, err := r.log.maybeAppend(m.PrevLogIndex, m.PrevLogTerm, m.LeaderCommit, m.Entries)
	if err != nil {
		return err
	}

	if !ok {
		ci, ct := r.log.conflictHint(m.PrevLogIndex)
		r.send(Message{
			Type:          MsgAppendResp,
			To:            m.From,
			Success:       false,
			ConflictIndex: ci,
			ConflictTerm:  ct,
			ReadID:        m.ReadID,
		})
		return nil
	}

	// Persist before the acknowledgement leaves: it is a claim that these
	// entries are durable here, and the leader will count it toward a commit.
	if err := r.persistHardState(); err != nil {
		return err
	}
	r.send(Message{
		Type:       MsgAppendResp,
		To:         m.From,
		Success:    true,
		MatchIndex: lastNew,
		ReadID:     m.ReadID,
	})
	return nil
}

func (r *RawNode) handleAppendResponse(m Message) error {
	if r.state != Leader {
		return fmt.Errorf("raft: append response to non-leader: %w", ErrIgnoredMessage)
	}
	pr, ok := r.progress[m.From]
	if !ok {
		return fmt.Errorf("raft: append response from untracked peer %d: %w", m.From, ErrIgnoredMessage)
	}

	if m.Success {
		// Take the follower's word for how far it matches rather than assuming
		// what we sent arrived. Responses can be delayed, reordered and
		// duplicated, and Match must never move backwards.
		if m.MatchIndex > pr.Match {
			pr.Match = m.MatchIndex
			pr.Next = pr.Match + 1

			advanced, err := r.maybeAdvanceCommit()
			if err != nil {
				return err
			}
			if advanced {
				// Tell everyone promptly rather than making them wait for the
				// next heartbeat to learn what is now committed.
				r.broadcastAppend()
				return nil
			}
		}
		// Still behind: keep feeding it.
		if pr.Next <= r.log.lastIndex() {
			r.sendAppend(m.From)
		}
		return nil
	}

	// Rejected: back up and retry. This loop is what enforces the log matching
	// property — a follower only ever appends at a point where it already agrees
	// with us, so agreement extends forward and never has a hole.
	next := r.nextAfterRejection(pr, m)
	if next < 1 {
		next = 1
	}
	if next < pr.Next {
		pr.Next = next
		r.sendAppend(m.From)
	}
	return nil
}

// nextAfterRejection decides where to resume for a follower that refused.
//
// The conflict hint (§5.3) is what makes this cheap. Without it the leader
// decrements by one per round trip, so a follower that diverged for a thousand
// entries costs a thousand round trips. With it the leader skips a whole term
// at a time: O(terms) rather than O(entries).
func (r *RawNode) nextAfterRejection(pr *Progress, m Message) Index {
	switch {
	case m.ConflictTerm > 0:
		// The follower has entries from ConflictTerm that we disagree with. If
		// we also have that term, resume just past our own last entry in it —
		// everything before that we already agree on. If we have never seen the
		// term, none of it can be right, so skip the whole run.
		if idx, found := r.lastIndexOfTerm(m.ConflictTerm); found {
			return idx + 1
		}
		return m.ConflictIndex

	case m.ConflictIndex > 0:
		// The follower's log simply ends before prevLogIndex. Resume from where
		// it actually ends rather than probing down one index at a time.
		return m.ConflictIndex

	default:
		return pr.Next - 1
	}
}

// lastIndexOfTerm finds our own last entry in the given term, walking back from
// the end of the log.
func (r *RawNode) lastIndexOfTerm(term Term) (Index, bool) {
	for i := r.log.lastIndex(); i >= r.log.firstIndex() && i > 0; i-- {
		t, err := r.log.term(i)
		if err != nil {
			return 0, false
		}
		if t == term {
			return i, true
		}
		if t < term {
			// Terms never decrease along the log, so once we are below the term
			// we are looking for, it is not there.
			return 0, false
		}
	}
	return 0, false
}

// ---------------------------------------------------------------------------
// The commit rule
// ---------------------------------------------------------------------------

// maybeAdvanceCommit advances commitIndex as far as the quorum and the commit
// rule allow, and reports whether it moved.
//
// The quorum arithmetic is the easy half: find the highest index a majority has
// matched. The rule below it is the half that is easy to get wrong, and getting
// it wrong produces a system that passes casual testing and loses committed
// data under leader churn.
//
// # The rule
//
// A leader may only advance commitIndex to cover an entry from its OWN current
// term. Entries from earlier terms are committed indirectly: once a same-term
// entry above them commits, everything below it commits with it.
//
// # Why (Figure 8 of the paper)
//
// Five nodes. S1 is leader in term 2 and replicates an entry at index 2 to S2,
// then crashes. S5 is elected for term 3 with votes from S3, S4 and itself, and
// writes its own entry at index 2. S5 crashes. S1 comes back, is elected for
// term 4, and resumes replicating its old term-2 entry at index 2 — now to S3 as
// well. At this moment index 2 is on S1, S2 and S3: a majority.
//
// A leader using only quorum arithmetic commits it here. That is the bug. S1
// then crashes, and S5 can still be elected for term 5 — its log ends at term 3,
// which beats the term-2 entry that S2 and S3 hold, so the election restriction
// does not stop it. S5 replicates its own index 2 everywhere, overwriting an
// entry that was reported committed. A client was told a write succeeded, and it
// has been silently erased.
//
// The rule closes this. In term 4, S1 refuses to commit the term-2 entry on
// majority replication alone. If it instead replicates an entry of term 4 to a
// majority, that entry commits — and now S5 cannot win an election at all,
// because a majority holds a term-4 entry that S5's log lacks. The entry at
// index 2 commits with it, safely, and only then.
//
// The no-op entry a leader appends on election exists precisely so that this
// can happen promptly instead of waiting for a client write.
func (r *RawNode) maybeAdvanceCommit() (bool, error) {
	if r.state != Leader {
		return false, nil
	}

	candidate := r.conf.CommittedIndex(func(id NodeID) Index {
		if p, ok := r.progress[id]; ok {
			return p.Match
		}
		return 0
	})

	if candidate <= r.log.committed {
		return false, nil
	}

	term, err := r.log.term(candidate)
	if err != nil {
		return false, fmt.Errorf("raft: term of commit candidate %d: %w", candidate, err)
	}
	if term != r.term {
		// Replicated to a majority, but from an earlier term. Not ours to
		// commit. See the reasoning above — this is the whole point.
		return false, nil
	}

	r.log.commitTo(candidate)
	return true, r.persistHardState()
}

// ---------------------------------------------------------------------------
// Applying
// ---------------------------------------------------------------------------

// CommittedEntries returns committed entries the state machine has not yet
// consumed, in order, bounded by MaxApplyEntries.
//
// The caller applies them and then calls ApplyTo. Splitting it in two matters:
// an entry must not be marked applied until the state machine has actually
// taken it, or a crash in between loses the command with no way to notice.
func (r *RawNode) CommittedEntries() ([]LogEntry, error) {
	return r.log.entriesToApply(r.cfg.MaxApplyEntries)
}

// HasCommittedEntries reports whether anything is waiting to be applied.
func (r *RawNode) HasCommittedEntries() bool { return r.log.hasEntriesToApply() }

// ApplyTo records that the state machine has consumed entries through index i.
func (r *RawNode) ApplyTo(i Index) { r.log.appliedTo(i) }

// AppliedIndex returns the highest index handed to the state machine.
func (r *RawNode) AppliedIndex() Index { return r.log.applied }
