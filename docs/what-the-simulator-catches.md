# What the simulator catches that an integration test wouldn't

A consensus bug does not announce itself. It is a stale read served once, under a partition that healed a second later. It is a committed entry quietly replaced during an election nobody noticed. By the time anything is visibly wrong, the evidence is gone — the partition is over, the leader has changed, and the logs show a cluster that looks fine.

This is a note about why that makes ordinary testing insufficient for this class of system, and what the four real bugs found here actually looked like.

## The problem with testing consensus normally

An integration test starts some nodes, does something disruptive, and asserts the cluster survived. That is a reasonable thing to do and this project does it too. But consider what it actually samples.

Raft's correctness depends on the *interleaving* — the order in which messages arrive, which ones are lost, which node's timer fires first. A five-node cluster exchanging a few hundred messages has more possible orderings than there are atoms in anything worth comparing it to. An integration test takes one sample from that space, chosen not deliberately but by whatever the OS scheduler happened to do on that machine, that afternoon.

Run it a thousand times and you have a thousand samples, all clustered tightly around the same region: whatever orderings a healthy laptop with a fast loopback interface tends to produce. The orderings where consensus breaks are not in that region. They are the ones where a message from two terms ago arrives after a snapshot has already discarded the entries it refers to — and a fast, reliable network almost never produces those.

Three specific things make it worse:

**Failures are rare and the assertions are weak.** A test that checks "the cluster still responds" will pass through a genuine safety violation as long as the cluster recovers. Two leaders in one term, if both step down before anyone notices, looks exactly like a normal election.

**When it does fail, you cannot get it back.** A flaky consensus test that fails once in four hundred CI runs is worse than no test, because nobody can act on it. The scheduling that produced it is gone, the timing is unrepeatable, and the usual outcome is a retry annotation.

**The interesting states take a long time to reach.** Log compaction only matters once a log is long enough to compact. A follower needing a snapshot only happens when it falls behind the leader's log floor. Reaching those states in real time means running a cluster for hours.

## What the simulator does instead

The consensus core is pure: no clock, no goroutines, no channels, no I/O. It is a function from (current state, incoming message) to (new state, outgoing messages). Everything that would make it nondeterministic lives outside it, in a driver that the simulator replaces.

That single constraint buys three things.

**The whole schedule is data.** Message delivery order, message loss, partitions, node crashes, clock advance — all decided by a seeded PRNG in a discrete-event loop. Not "roughly random", but *exactly* determined by one integer.

**Time is virtual.** An election timeout is a counter, not a wall-clock wait. The cluster experiences hours of operation in seconds of CPU, so the states that need a long-running cluster are reachable in the first second of the run.

**Every state is inspectable.** There is no concurrency, so safety properties are checked after every single event rather than at the end. At most one leader per term. No committed entry ever changes. The commit index never decreases. Applied never exceeds committed. A violation is caught at the event that caused it, not three seconds later when a client notices.

The campaign this produces:

```
$ simctl fuzz --count=10000
ran 10000 seeds in 4m2.867s (41 seeds/sec, 13535461 entries committed,
                             2092853788 events)
no safety violations
```

**83 hours of simulated cluster time in under four minutes** — 1,376× real time — across 1.7 billion events, with every safety property checked after each one.

## One integer reproduces any failure

This is the property that makes the rest worth having.

```
$ for i in 1 2 3; do simctl run --seed=2; done
169359 events in 72.578ms (30000ms virtual), 1146 entries committed across
  7 leader terms, 15 crashes, 14 restarts, 10 partitions, 102 snapshots taken,
  41 sent, 4 membership changes, 77639 msgs (3523 dropped, 1482 duplicated),
  trace 26c664211d16bfc0
169359 events in 71.143ms ... trace 26c664211d16bfc0
169359 events in 72.652ms ... trace 26c664211d16bfc0
```

Identical event count and identical trace hash on every run. The wall-clock time
differs because that is the only thing not simulated.

A failure is pinned the same way. Seeds that once failed now pass, so the
demonstration uses the negative control — durability deliberately broken, which
*must* produce violations:

```
$ for i in 1 2 3; do simctl run --seed=27 --disk-loss=0.5; done
safety violation [committed-entries-never-change] at t=26106ms (seed 27):
  committed index 119 changed: node 1 committed term 1 (8488d043) at t=2520ms,
  node 3 now has term 3 (c38200e5)
```

The same violation, at the same index, at the same millisecond, every time — on
any machine, indefinitely. Add a print statement and it reappears with the print
statement in it. A randomized test that cannot do this reports failures nobody
can act on.

## The four bugs

Each of these is a real defect in this implementation. None would have been found by an integration test on a healthy network, and each is now a named regression test and a seeded mutation.

### A refused pre-vote handed the candidate an inflated term

Pre-vote exists to stop a partitioned node from disrupting a healthy cluster. A node that cannot reach anyone asks "would you vote for me at term+1?" before actually incrementing its term — so when the partition heals, it has not forced a pointless election.

The refusal replied with the *hypothetical* term it had been asked about. So a node refusing to disrupt the cluster handed the candidate exactly the inflated term the mechanism exists to prevent, and the healthy leader was deposed by a node that had just been told no.

This is the worst kind of bug: the feature appears to work. Pre-vote was running, the refusals were correct, and the disruption happened anyway. An integration test asserting "the cluster elects a leader" passes — a leader *is* elected, just needlessly, and only under a partition that has since healed.

### A delayed append below the commit index was scanned for conflicts

Found at **seed 188**, and reproduced exactly from that integer throughout diagnosis.

An `AppendEntries` delayed long enough to arrive after the receiver had compacted its log was checked for conflicts against entries that no longer existed. An entry that is merely *unknown* was reported as *conflicting*, and the node refused it as an attempt to overwrite its committed prefix.

Before compaction existed, this code was correct: the old entries were still there, the scan verified them, and it correctly found no conflict. Compaction is what turned a valid check into an invalid one, and the bug only appears when a message is delayed *across* a compaction — which needs a log long enough to compact and a message delayed long enough to span it. On a real network, essentially never. In virtual time, within the first twenty seeds.

### The storage layer conflated the log floor with the snapshot boundary

These are the same number right up until a leader keeps a tail of entries past the snapshot it has taken. Once they diverged, `Compact` concluded it had already run and silently did nothing.

Nothing failed. No error was returned, no assertion fired. The log grew forever behind a snapshot that claimed to have shortened it — a disk-space leak that would take days to notice in production and is invisible to any test that does not specifically check the log actually got shorter.

### A restarting node never restored its state machine from its own snapshot

Without compaction, a restarting node replays its entire log and arrives at the right state anyway. That is why this hid: the code path was wrong and the outcome was right, for as long as nothing was ever discarded.

Once compaction existed, the entries were gone. The node came back holding a fraction of its state and reported itself healthy. Silently — the node is up, it answers, it participates in elections. It simply has the wrong data.

## One finding that was not a bug

Eleven seeds reported state machine divergence. The cause was the simulator's own model of a state machine, not a defect in Raft.

It is recorded here as what it was. A tool that reports its own bugs as findings is a tool whose findings cannot be trusted, and the temptation to count eleven seeds as eleven catches is exactly the pressure worth naming.

## What this does not cover

The simulator proves things about the *algorithm*. It says nothing about the implementation around it, because it replaces gRPC with an in-memory queue, bbolt with a map, and the Go scheduler with a single-threaded loop. Bugs in any of those are invisible to it — by construction, since removing them is what makes it deterministic.

That gap is covered separately, by running a real cluster in containers under `tc`/`netem` packet loss and `iptables` partitions, recording every client operation, and checking the resulting history is linearizable with Porcupine. Those runs cannot be replayed — the faults are real and the scheduling is the kernel's — but they see everything the simulator's stand-ins hide.

Neither replaces the other:

| | simulator | chaos and linearizability |
| --- | --- | --- |
| Checks | algorithm safety, after every event | client-visible history |
| Sees | every node's internal state | only what a client could observe |
| Network | modelled | real gRPC, real kernel |
| Storage | a map | real bbolt, real fsync |
| Reproducible | from one integer | no |
| Speed | 1,376× real time | real time |

## How the test suite is checked

A test that cannot fail is indistinguishable from one that passes, so the suite is checked against 22 seeded mutations — plausible mistakes, each paired with the test that must catch it. If a mutation survives, that test is decorative.

This found three. A conflict-hint test that measured nothing at all, and two pre-vote tests that were over-determined — they passed whether or not the bug they were named after was present.

Without that check, the natural conclusion from a green suite would have been "the tests pass". The accurate one was "three of these tests would pass no matter what the code did".

---

Reproduce any of this:

```bash
make fuzz FUZZ_SEEDS=10000       # the campaign
make replay SEED=2               # one execution, exactly
make mutation                    # 22/22, and what each models
bin/simctl fuzz --count=200 --disk-loss=0.5   # the negative control
```

The negative control matters as much as the campaign. Breaking a documented assumption — losing acknowledged writes on disk — must produce violations, or the checker is not checking anything. It does.
