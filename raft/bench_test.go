package raft

import (
	"errors"
	"fmt"
	"testing"
)

// Benchmarks for the consensus core in isolation — no network, no disk, no
// goroutines. What they measure is the algorithm's own cost: log appends,
// message construction, quorum arithmetic, commit checks.
//
// These are not end-to-end system numbers and must never be quoted as such. A
// real cluster is bounded by fsync and network round trips, both of which are
// orders of magnitude slower than anything here. The value of measuring the core
// alone is that it tells you how much headroom the algorithm leaves, and it
// turns a regression in the hot path into a number instead of a feeling.

// benchCluster elects a leader and hands back a network ready to be driven.
func benchCluster(b *testing.B, nodes int) (*network, *RawNode) {
	b.Helper()

	voters := make([]NodeID, nodes)
	jitter := map[NodeID]int{}
	for i := range voters {
		voters[i] = NodeID(i + 1)
		jitter[NodeID(i+1)] = i * 2 // node 1 always campaigns first
	}

	n := newNetwork(b, voters, withJitter(jitter))
	n.tickAll(11)

	leaders := n.leaders()
	if len(leaders) != 1 {
		b.Fatalf("setup: want one leader, got %v", leaders)
	}
	return n, n.nodes[leaders[0]]
}

// BenchmarkProposeAndCommit measures one full replication round: append on the
// leader, AppendEntries to every follower, responses back, commit advanced.
func BenchmarkProposeAndCommit(b *testing.B) {
	for _, nodes := range []int{1, 3, 5} {
		for _, size := range []int{16, 256, 4096} {
			b.Run(fmt.Sprintf("nodes=%d/cmd=%dB", nodes, size), func(b *testing.B) {
				n, leader := benchCluster(b, nodes)
				cmd := make([]byte, size)

				b.SetBytes(int64(size))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := leader.Propose(cmd); err != nil {
						b.Fatalf("Propose: %v", err)
					}
					n.deliver()
				}
				b.StopTimer()

				if leader.CommitIndex() < Index(b.N) {
					b.Fatalf("only committed %d of %d proposals", leader.CommitIndex(), b.N)
				}
			})
		}
	}
}

// BenchmarkAppendEntriesToFollower measures the receiving side alone: the
// consistency check, the conflict scan, the append, and the response.
func BenchmarkAppendEntriesToFollower(b *testing.B) {
	for _, batch := range []int{1, 16, 64} {
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
			follower, err := NewRawNode(Config{
				ID: 2, Storage: NewMemoryStorage(),
				ElectionTick: 10, HeartbeatTick: 1,
				Rand: fixedRand{}, Bootstrap: NewConfiguration(ids(1, 2, 3)),
			})
			if err != nil {
				b.Fatalf("NewRawNode: %v", err)
			}

			cmd := make([]byte, 64)
			next := Index(1)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				entries := make([]LogEntry, batch)
				for j := range entries {
					entries[j] = LogEntry{
						Index: next + Index(j), Term: 1,
						Type: EntryNormal, Command: cmd,
					}
				}
				m := Message{
					Type: MsgAppendReq, From: 1, To: 2, Term: 1,
					PrevLogIndex: next - 1, PrevLogTerm: termOrZero(next - 1),
					Entries: entries, LeaderCommit: next - 1,
				}
				if err := follower.Step(m); err != nil && !errors.Is(err, ErrIgnoredMessage) {
					b.Fatalf("Step: %v", err)
				}
				follower.Messages()
				next += Index(batch)
			}
			b.StopTimer()
			b.ReportMetric(float64(batch), "entries/op")
		})
	}
}

func termOrZero(i Index) Term {
	if i == 0 {
		return 0
	}
	return 1
}

// BenchmarkHeartbeat measures the steady-state cost of a leader holding office
// with nothing to replicate. In a real deployment this runs several times a
// second forever, so it is the one cost the cluster always pays.
func BenchmarkHeartbeat(b *testing.B) {
	for _, nodes := range []int{3, 5} {
		b.Run(fmt.Sprintf("nodes=%d", nodes), func(b *testing.B) {
			n, leader := benchCluster(b, nodes)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := leader.Tick(); err != nil {
					b.Fatalf("Tick: %v", err)
				}
				n.deliver()
			}
		})
	}
}

// BenchmarkElectionTicks reports how many logical ticks a cold cluster takes to
// settle on a leader. A tick count rather than a duration: the core has no
// clock, so this is the number the driver multiplies by its tick interval.
func BenchmarkElectionTicks(b *testing.B) {
	for _, nodes := range []int{3, 5} {
		b.Run(fmt.Sprintf("nodes=%d", nodes), func(b *testing.B) {
			voters := make([]NodeID, nodes)
			for i := range voters {
				voters[i] = NodeID(i + 1)
			}

			total := 0
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				n := newNetwork(b, voters, withSeed(int64(i)))
				ticks := 0
				for ticks < 500 {
					n.tickAll(1)
					ticks++
					if len(n.leaders()) == 1 {
						break
					}
				}
				if len(n.leaders()) != 1 {
					b.Fatalf("seed %d: no leader after %d ticks", i, ticks)
				}
				total += ticks
			}
			b.StopTimer()
			b.ReportMetric(float64(total)/float64(b.N), "ticks/election")
		})
	}
}

// BenchmarkMemoryStorageAppend isolates the storage layer, so that a change in
// the numbers above can be attributed to the algorithm or to storage rather
// than guessed at.
func BenchmarkMemoryStorageAppend(b *testing.B) {
	st := NewMemoryStorage()
	cmd := make([]byte, 64)

	b.SetBytes(64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := st.AppendEntries([]LogEntry{{
			Index: Index(i + 1), Term: 1, Type: EntryNormal, Command: cmd,
		}})
		if err != nil {
			b.Fatalf("AppendEntries: %v", err)
		}
	}
}

// BenchmarkLogRepair measures repairing a divergent follower — the path the
// §5.3 conflict hint exists to make cheap.
func BenchmarkLogRepair(b *testing.B) {
	const divergentEntries = 200

	good := []Term{1}
	for i := 0; i < divergentEntries; i++ {
		good = append(good, 6)
	}
	stale := []Term{1}
	for i := 0; i < divergentEntries/2; i++ {
		stale = append(stale, 4)
	}
	for i := 0; i < divergentEntries/2; i++ {
		stale = append(stale, 5)
	}

	b.ResetTimer()
	roundTrips := 0
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		n := newNetwork(b, ids(1, 2, 3),
			withPreVote(false),
			withJitter(map[NodeID]int{1: 0, 2: 5, 3: 9}),
			withLog(1, good...), withLog(2, good...), withLog(3, stale...))
		n.isolate(3)
		n.tickAll(11)
		leader := n.nodes[n.leaders()[0]]
		n.heal()
		b.StartTimer()

		if err := leader.Tick(); err != nil {
			b.Fatalf("Tick: %v", err)
		}
		roundTrips += driveRepair(b, n, leader)
	}
	b.StopTimer()
	b.ReportMetric(float64(roundTrips)/float64(b.N), "roundtrips/repair")
	b.ReportMetric(float64(divergentEntries), "divergent-entries")
}

// driveRepair steps the leader and node 3 against each other until the follower
// stops replying, returning the number of round trips it took.
func driveRepair(b *testing.B, n *network, leader *RawNode) int {
	b.Helper()
	trips := 0
	for round := 0; round < 1000; round++ {
		var out []Message
		for _, m := range leader.Messages() {
			if m.To == 3 {
				out = append(out, m)
			}
		}
		if len(out) == 0 {
			return trips
		}
		for _, m := range out {
			trips++
			if err := n.nodes[3].Step(m); err != nil && !errors.Is(err, ErrIgnoredMessage) {
				b.Fatalf("Step on follower: %v", err)
			}
			for _, resp := range n.nodes[3].Messages() {
				if err := leader.Step(resp); err != nil && !errors.Is(err, ErrIgnoredMessage) {
					b.Fatalf("Step on leader: %v", err)
				}
			}
		}
	}
	b.Fatal("repair did not converge")
	return trips
}
