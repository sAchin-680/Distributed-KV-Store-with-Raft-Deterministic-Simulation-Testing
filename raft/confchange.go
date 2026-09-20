package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Changing a cluster's membership is the part of Raft that looks easy and is
// not.
//
// The naive version — every node switches from the old configuration to the new
// one as soon as it hears about the change — is broken, because nodes switch at
// different moments. Moving from {1,2,3} to {3,4,5}, there is a window where
// nodes 1 and 2 still believe the cluster is {1,2,3}, so between them they are a
// majority and can elect a leader, while nodes 4 and 5 already believe it is
// {3,4,5} and between them are also a majority and can elect a different one.
// Two leaders in one term, with neither having done anything wrong.
//
// Joint consensus closes the window by passing through an intermediate
// configuration in which every decision needs a majority of BOTH the old and the
// new membership:
//
//	C_old ──append EnterJoint──▶ C_old,new ──append LeaveJoint──▶ C_new
//
// In C_old,new, nodes 1 and 2 are a majority of C_old but not of C_new, and
// nodes 4 and 5 the reverse. Neither can decide anything alone. The overlap is
// forced, and the two configurations can never elect separate leaders.

var (
	// ErrConfChangeInProgress means a membership change is already underway.
	// Raft permits only one uncommitted configuration change at a time; two in
	// flight would reintroduce exactly the ambiguity joint consensus removes.
	ErrConfChangeInProgress = errors.New("raft: a configuration change is already in progress")

	// ErrConfChangeInvalid means the requested configuration could not function.
	ErrConfChangeInvalid = errors.New("raft: invalid configuration change")
)

// ProposeConfChange begins a membership change toward target.
//
// It appends the entry that enters the joint configuration. Leaving it happens
// on its own, once that entry commits — the caller does not drive the second
// half, because doing so would mean a client crash could strand the cluster in
// a joint configuration for ever.
func (r *RawNode) ProposeConfChange(target Configuration) (Index, error) {
	if r.state != Leader {
		return 0, ErrNotLeader
	}
	if r.conf.IsJoint() {
		return 0, ErrConfChangeInProgress
	}
	// A configuration change that has been appended but not yet committed is
	// still in progress, even though the configuration does not look joint
	// yet — the entry could still be truncated by a different leader.
	if r.confIndex > r.log.committed {
		return 0, ErrConfChangeInProgress
	}
	// The commit rule requires an entry of this leader's own term before
	// anything can commit. Proposing a membership change before that means the
	// change cannot complete, and the cluster sits joint until it does.
	if !r.committedInCurrentTerm() {
		return 0, ErrReadIndexUnavailable
	}

	cc, err := r.conf.EnterJoint(target)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrConfChangeInvalid, err)
	}
	return r.propose(LogEntry{Type: EntryConfChange, Command: cc.Encode()})
}

// maybeLeaveJoint appends the second half of a membership change once the first
// half has committed.
//
// Automatic, and on the leader only. A joint configuration needs majorities of
// both halves for everything, so a cluster left in one is less available than
// either configuration alone — it must not be able to get stuck there because a
// client went away.
func (r *RawNode) maybeLeaveJoint() error {
	if r.state != Leader || !r.conf.IsJoint() {
		return nil
	}
	// Only once the entry that established the joint configuration is itself
	// committed. Leaving earlier would abandon C_old while a future leader
	// could still truncate the entry that brought us here.
	if r.confIndex > r.log.committed {
		return nil
	}

	cc, err := r.conf.LeaveJoint()
	if err != nil {
		return fmt.Errorf("raft: leaving joint configuration: %w", err)
	}
	if _, err := r.propose(LogEntry{Type: EntryConfChange, Command: cc.Encode()}); err != nil {
		if errors.Is(err, ErrNotLeader) {
			return nil
		}
		return err
	}
	return nil
}

// refreshConfiguration recomputes the configuration in force from the log.
//
// # Applied on append, not on commit
//
// A configuration change takes effect the moment it is appended, before it is
// known to have committed. That is deliberate and it is not an optimization:
// committing the change requires votes counted under the new configuration, so a
// node that waited for the commit would be voting under a configuration it is
// simultaneously replacing, and the change could never commit itself.
//
// The price is that an uncommitted change can be truncated by a different
// leader, and the configuration has to be able to go backwards. Recomputing from
// the log rather than tracking incremental changes is what makes that work: the
// log is the only authority on what the configuration is, so whatever is in it
// after a truncation is the answer.
func (r *RawNode) refreshConfiguration() error {
	for i := r.log.lastIndex(); i >= r.log.firstIndex() && i > 0; i-- {
		entry, err := r.log.storage.GetEntry(i)
		if err != nil {
			break // compacted; fall through to the snapshot
		}
		if entry.Type != EntryConfChange {
			continue
		}
		cc, err := DecodeConfChange(entry.Command)
		if err != nil {
			return fmt.Errorf("raft: decoding configuration at index %d: %w", i, err)
		}
		r.setConfiguration(cc.Config, i)
		return nil
	}

	// Nothing in the log. The snapshot carries the configuration in force as of
	// its boundary, which is exactly why it has to carry one.
	snap, err := r.cfg.Storage.LoadSnapshot()
	if err == nil && !snap.Config.IsEmpty() {
		r.setConfiguration(snap.Config, snap.LastIncludedIndex)
		return nil
	}

	r.setConfiguration(r.bootstrap, 0)
	return nil
}

// setConfiguration adopts a configuration and keeps leader bookkeeping in step.
func (r *RawNode) setConfiguration(conf Configuration, at Index) {
	if r.conf.Equal(conf) && r.confIndex == at {
		return
	}
	r.conf = conf.Clone()
	r.confIndex = at

	if r.state == Leader {
		r.refreshProgress()
	}
}

// refreshProgress adds tracking for new members and drops it for departed ones.
//
// A member that leaves and later returns starts from zero rather than from
// whatever the leader remembered: its log may have been rebuilt in the
// meantime, and trusting a stale match index would let the leader commit on the
// strength of entries that node no longer has.
func (r *RawNode) refreshProgress() {
	if r.progress == nil {
		return
	}
	members := r.conf.Members()
	next := r.log.lastIndex() + 1

	seen := make(map[NodeID]bool, len(members))
	for _, id := range members {
		seen[id] = true
		if _, ok := r.progress[id]; ok {
			continue
		}
		pr := &Progress{Next: next}
		if id == r.id {
			pr.Match = r.log.lastIndex()
		}
		r.progress[id] = pr
	}
	for id := range r.progress {
		// Never drop our own entry while we are still leading.
		//
		// A leader removing itself is still the node appending entries, and it
		// needs somewhere to record how far its own log has got — including for
		// the entry that completes its own removal. Dropping it here meant the
		// next proposal dereferenced nothing and the process died.
		//
		// Keeping it is safe: the quorum arithmetic only consults the voters in
		// the configuration, so a leader that is no longer one of them does not
		// count toward any majority. stepDownIfRemoved retires it a moment
		// later, once the change has left the joint configuration.
		if id == r.id && r.state == Leader {
			continue
		}
		if !seen[id] {
			delete(r.progress, id)
		}
	}
}

// stepDownIfRemoved makes a leader that is no longer a voter stop leading.
//
// Only once the change has left the joint configuration. While joint, the
// departing leader is still a member of C_old and its votes still count toward
// one of the two majorities the transition needs — standing down early would
// remove a voter the change depends on.
func (r *RawNode) stepDownIfRemoved() {
	if r.state != Leader || r.conf.IsJoint() || r.conf.IsVoter(r.id) {
		return
	}
	if r.confIndex > r.log.committed {
		return // the removal could still be truncated
	}
	r.becomeFollower(r.term, None)
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

// The core has to be able to read a configuration change, because it applies one
// on append rather than handing it to the application. So the encoding lives
// here rather than in the protobuf conversion layer — one definition, in the
// package that cannot do without it.

// Encode renders a configuration change for the log entry that carries it.
func (cc ConfChange) Encode() []byte {
	buf := make([]byte, 0, 32)
	buf = append(buf, byte(cc.Kind))
	buf = appendIDs(buf, cc.Config.Voters)
	buf = appendIDs(buf, cc.Config.OldVoters)
	buf = appendIDs(buf, cc.Config.Learners)
	return buf
}

// DecodeConfChange parses one back.
func DecodeConfChange(b []byte) (ConfChange, error) {
	if len(b) == 0 {
		return ConfChange{}, errors.New("raft: empty configuration change")
	}
	cc := ConfChange{Kind: ConfChangeKind(b[0])}
	rest := b[1:]

	var err error
	if cc.Config.Voters, rest, err = readIDs(rest, "voters"); err != nil {
		return ConfChange{}, err
	}
	if cc.Config.OldVoters, rest, err = readIDs(rest, "old voters"); err != nil {
		return ConfChange{}, err
	}
	if cc.Config.Learners, _, err = readIDs(rest, "learners"); err != nil {
		return ConfChange{}, err
	}
	// Normalize on the way in: the core derives ordered decisions from these
	// slices, and an unsorted one would make behaviour depend on what a peer
	// happened to encode.
	cc.Config.normalize()
	return cc, nil
}

func appendIDs(dst []byte, ids []NodeID) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(ids)))
	for _, id := range ids {
		dst = binary.AppendUvarint(dst, uint64(id))
	}
	return dst
}

func readIDs(b []byte, what string) (ids []NodeID, rest []byte, err error) {
	count, n := binary.Uvarint(b)
	if n <= 0 {
		return nil, nil, fmt.Errorf("raft: truncated %s count", what)
	}
	b = b[n:]

	if count == 0 {
		return nil, b, nil
	}
	ids = make([]NodeID, 0, count)
	for range count {
		id, n := binary.Uvarint(b)
		if n <= 0 {
			return nil, nil, fmt.Errorf("raft: truncated %s list", what)
		}
		ids = append(ids, NodeID(id))
		b = b[n:]
	}
	return ids, b, nil
}
