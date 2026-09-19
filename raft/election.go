package raft

import "fmt"

// campaign stands for election.
//
// With pre-vote enabled this happens in two rounds. The first is a straw poll
// at term+1 that commits to nothing; only if it succeeds does the node advance
// its term and campaign for real. The extra round trip buys protection against
// a specific, common failure: a node partitioned away from the cluster times out
// repeatedly, incrementing its term each time, and on rejoining forces a healthy
// leader — which has done nothing wrong — to step down and the cluster to hold
// an election it did not need.
func (r *RawNode) campaign(preVote bool) error {
	if !r.promotable() {
		// A learner, or a node removed from the configuration. It may still
		// receive entries; it may not stand for election.
		return nil
	}

	// The term this campaign concerns. For a pre-vote it is hypothetical — the
	// term we would use if the straw poll succeeds — and our own term is
	// deliberately left alone.
	var campaignTerm Term
	if preVote {
		r.becomePreCandidate()
		campaignTerm = r.term + 1
	} else {
		r.becomeCandidate()
		campaignTerm = r.term
		if err := r.persistHardState(); err != nil {
			return err
		}
	}

	// A candidate always votes for itself. In a single-node cluster that is
	// already a quorum.
	r.recordVote(r.id, true)
	if done, err := r.tallyVotes(); err != nil || done {
		return err
	}

	lastIndex := r.log.lastIndex()
	lastTerm := r.log.lastTerm()

	for _, peer := range r.conf.Peers(r.id) {
		if !r.conf.IsVoter(peer) {
			continue // learners have no vote to give
		}
		r.send(Message{
			Type:         MsgVoteReq,
			To:           peer,
			Term:         campaignTerm,
			PreVote:      preVote,
			LastLogIndex: lastIndex,
			LastLogTerm:  lastTerm,
		})
	}
	return nil
}

func (r *RawNode) recordVote(from NodeID, granted bool) {
	if _, seen := r.votes[from]; seen {
		// First answer wins. A duplicated response must not be able to change a
		// tally, and the network is allowed to duplicate.
		return
	}
	r.votes[from] = granted
}

// tallyVotes checks whether the campaign has been decided, and acts on it.
// It reports whether the campaign is over, either way.
func (r *RawNode) tallyVotes() (bool, error) {
	granted := func(id NodeID) bool { g, ok := r.votes[id]; return ok && g }
	rejected := func(id NodeID) bool { g, ok := r.votes[id]; return ok && !g }

	switch {
	case r.conf.HasQuorum(granted):
		if r.state == PreCandidate {
			// The straw poll says an election would succeed. Now hold one.
			return true, r.campaign(false)
		}
		return true, r.becomeLeader()

	case r.conf.QuorumImpossible(rejected):
		// Enough nodes have refused that we cannot win. Stand down rather than
		// waiting out the timeout — someone with a better log is probably
		// campaigning, and blocking on a lost election only delays them.
		//
		// Note this is not "not a quorum yet": votes may still be outstanding,
		// and treating that as defeat would abandon winnable elections.
		r.becomeFollower(r.term, None)
		return true, nil

	default:
		return false, nil // still waiting
	}
}

// handleVoteRequest decides whether to grant a vote.
func (r *RawNode) handleVoteRequest(m Message) error {
	granted := r.canVote(m) && r.log.isUpToDate(m.LastLogIndex, m.LastLogTerm)

	if granted && !m.PreVote {
		// Record the vote and make it durable before the grant leaves this
		// node. A node that forgets it voted can vote again in the same term,
		// and two votes in one term can elect two leaders.
		r.vote = m.From
		r.electionElapsed = 0
		if err := r.persistHardState(); err != nil {
			return err
		}
	}

	// Which term the reply carries matters more than it looks.
	//
	// A *granted* pre-vote echoes the hypothetical term that was asked about,
	// so the pre-candidate can match the reply to the poll it is running.
	//
	// A *rejected* one must carry our own term instead. Echoing the
	// hypothetical term would hand the pre-candidate a term one greater than
	// its own; it would adopt it, and go on to force a healthy leader to step
	// down — which is exactly the disruption pre-vote exists to prevent. The
	// straw poll would end up causing the harm it was added to avoid. Sending
	// our real term leaves a rejected pre-candidate exactly where it started,
	// while still teaching it the true term if it is genuinely behind.
	replyTerm := r.term
	if m.PreVote && granted {
		replyTerm = m.Term
	}

	r.send(Message{
		Type:    MsgVoteResp,
		To:      m.From,
		Term:    replyTerm,
		PreVote: m.PreVote,
		Granted: granted,
	})
	return nil
}

// canVote applies everything except the log comparison.
func (r *RawNode) canVote(m Message) bool {
	switch {
	case m.PreVote:
		// A straw poll commits to nothing, so the only question is whether this
		// node would be willing to hold an election at all.
		//
		// The candidate must be proposing a term strictly later than ours — a
		// real election at an earlier term could not succeed, so there is
		// nothing to encourage.
		if m.Term <= r.term {
			return false
		}
		// And we must not currently be hearing from a leader. This second
		// clause is the whole of the disruption protection: a node returning
		// from a partition finds that everyone still has a leader, is told no,
		// and nothing about the cluster has changed. Dropping this check makes
		// pre-vote decorative, because the term in a pre-vote request is always
		// one greater than the sender's and so almost always greater than ours.
		return r.lead == None || r.electionElapsed >= r.randomizedElectionTimeout

	case r.vote == m.From:
		// Already voted for this candidate this term. Re-granting is safe and
		// keeps a lost response from stalling an election.
		return true

	case r.vote == None && r.lead == None:
		// Not yet voted this term, and no leader known for it.
		return true

	default:
		// Either we voted for someone else this term, or we already know this
		// term's leader. Either way there is nothing left to give.
		return false
	}
}

// handleVoteResponse counts a reply to our own campaign.
func (r *RawNode) handleVoteResponse(m Message) error {
	// A pre-vote reply is only meaningful to a pre-candidate, and a real vote
	// only to a candidate. Mixing them would let a reply from a previous,
	// abandoned campaign decide the current one — and delayed replies are
	// routine, not exotic.
	switch {
	case m.PreVote && r.state != PreCandidate:
		return fmt.Errorf("raft: pre-vote response in state %s: %w", r.state, ErrIgnoredMessage)
	case !m.PreVote && r.state != Candidate:
		return fmt.Errorf("raft: vote response in state %s: %w", r.state, ErrIgnoredMessage)
	}

	r.recordVote(m.From, m.Granted)
	_, err := r.tallyVotes()
	return err
}

// Campaign forces an immediate election, bypassing the timeout.
//
// Exists for tests and for an explicit leadership transfer; the algorithm itself
// never needs it.
func (r *RawNode) Campaign() error {
	return r.campaign(r.cfg.PreVote)
}
