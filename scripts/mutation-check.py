#!/usr/bin/env python3
"""Mutation testing for the consensus core.

A passing test proves nothing until you have watched it fail. This script
introduces a known bug, runs the test that is supposed to catch it, and reports
whether the test actually noticed.

Each mutation below is a real mistake — most of them are the classic ones people
make implementing Raft from the paper. If a mutation survives, the test named
after it is decorative and the property it claims to protect is unguarded.

This has already caught two decorative tests in this repository.

Usage:
    scripts/mutation-check.py            # run every mutation
    scripts/mutation-check.py --list     # show them without running
    scripts/mutation-check.py -k commit  # only mutations matching a substring
"""

from __future__ import annotations

import argparse
import pathlib
import subprocess
import sys
from dataclasses import dataclass

ROOT = pathlib.Path(__file__).resolve().parent.parent

GREEN, RED, YELLOW, DIM, RESET = "\033[32m", "\033[31m", "\033[33m", "\033[2m", "\033[0m"


@dataclass
class Mutation:
    name: str
    """Stable identifier: <area>/<what the mutated code does wrong>.

    The area prefix groups the report and makes `-k election` mean something.
    The remainder is always a third-person description of the *defect*, not of
    the fix or the test — consistent voice, so a report reads as a list of
    things that would be wrong rather than a mixture of states and actions.

    These appear in CI logs, so treat them as stable identifiers: rename only
    when the mutation itself changes.
    """

    file: str
    old: str
    new: str

    expect: str
    """Go test name pattern that must fail once the mutation is applied."""


    why: str
    """What breaks in a real cluster if this mutation ships."""

    pkg: str = "./raft/"
    """Package holding that test.

    Needed because `go test -run` exits 0 when the pattern matches nothing, so
    pointing at the wrong package reports every mutation as surviving. That is
    exactly what happened the first time a mutation outside the core was added.
    """


MUTATIONS: list[Mutation] = [
    Mutation(
        name="election/restriction-compares-index-before-term",
        file="raft/log.go",
        old="""	myTerm := l.lastTerm()
	if lastTerm != myTerm {
		return lastTerm > myTerm
	}
	return lastIdx >= l.lastIndex()""",
        new="""	if lastIdx != l.lastIndex() {
		return lastIdx > l.lastIndex()
	}
	return lastTerm >= l.lastTerm()""",
        expect="TestElectionRestriction",
        why="'Longest log wins' elects a leader missing committed entries, which it "
        "then overwrites. Leader Completeness is gone.",
    ),
    Mutation(
        name="log/truncates-entries-already-held",
        file="raft/log.go",
        old="""	case conflict == 0:""",
        new="""	case false:""",
        expect="TestMaybeAppendIgnoresEntriesAlreadyHeld",
        why="A duplicated AppendEntries truncates the log and discards entries a "
        "later message already delivered.",
    ),
    Mutation(
        name="log/commit-index-moves-backwards",
        file="raft/log.go",
        old="""	if i <= l.committed {
		return
	}""",
        new="""	if i == 0 {
		return
	}""",
        expect="TestCommitIndexNeverMovesBackwards",
        why="A stale or duplicated message retracts a commit that was already "
        "promised to a client.",
    ),
    Mutation(
        name="election/votes-twice-in-one-term",
        file="raft/election.go",
        old="""	case r.vote == None && r.lead == None:""",
        new="""	case true:""",
        expect="TestNodeVotesAtMostOncePerTerm",
        why="Two leaders in one term. Election Safety is gone, and with it every "
        "guarantee built on top.",
    ),
    Mutation(
        name="election/pre-vote-ignores-live-leader",
        file="raft/election.go",
        old="""		return r.lead == None || r.electionElapsed >= r.randomizedElectionTimeout""",
        new="""		return true""",
        expect="TestPreVoteIsRefusedWhileALeaderIsAlive",
        why="Pre-vote becomes decorative: a node returning from a partition still "
        "deposes a healthy leader.",
    ),
    Mutation(
        name="election/rejected-pre-vote-echoes-hypothetical-term",
        file="raft/election.go",
        old="""	if m.PreVote && granted {""",
        new="""	if m.PreVote {""",
        expect="TestRejectedPreVoteRepliesWithTheRespondersOwnTerm|TestPreVotePreventsDisruption",
        why="The refusal itself hands the candidate an inflated term, so pre-vote "
        "causes exactly the disruption it exists to prevent. This was a real bug.",
    ),
    Mutation(
        name="election/timeout-not-randomized",
        file="raft/raft.go",
        old="""	r.randomizedElectionTimeout = r.cfg.ElectionTick + r.cfg.Rand.Intn(r.cfg.ElectionTick)""",
        new="""	r.randomizedElectionTimeout = r.cfg.ElectionTick""",
        expect="TestSplitVotesAlwaysResolve",
        why="Nodes campaign in lockstep, split the vote, and retry in lockstep. "
        "The cluster can fail to elect a leader indefinitely.",
    ),
    Mutation(
        name="replication/commits-on-quorum-without-current-term",
        file="raft/replication.go",
        old="""	if term != r.term {
		// Replicated to a majority, but from an earlier term. Not ours to
		// commit. See the reasoning above — this is the whole point.
		return false, nil
	}""",
        new="""	_ = term""",
        expect="TestCommitRuleRefusesEntriesFromEarlierTerms",
        why="Figure 8. An acknowledged write is silently overwritten by a later "
        "leader. The single most consequential mistake in a from-scratch Raft.",
    ),
    Mutation(
        name="replication/commit-counts-next-not-match",
        file="raft/replication.go",
        old="""			return p.Match""",
        new="""			return p.Next""",
        expect=".",
        why="Commits entries no follower has acknowledged, because Next is what the "
        "leader intends to send, not what arrived.",
    ),
    Mutation(
        name="replication/conflict-hint-ignored",
        file="raft/replication.go",
        old="""		if idx, found := r.lastIndexOfTerm(m.ConflictTerm); found {
			return idx + 1
		}
		return m.ConflictIndex""",
        new="""		return pr.Next - 1""",
        expect="TestConflictHintSkipsWholeTermsNotSingleIndexes",
        why="Repairing a divergent follower costs one round trip per entry instead "
        "of per term: 204 round trips instead of 6, measured.",
    ),
    Mutation(
        name="read/confirmed-without-a-quorum",
        file="raft/read.go",
        old="""		req.acks[from] = true
		if r.conf.HasQuorum(func(n NodeID) bool { return req.acks[n] }) {""",
        new="""		req.acks[from] = true
		if true {""",
        expect="TestReadIndexNeedsAMajorityNotJustOneReply|TestPartitionedLeaderCannotConfirmARead",
        why="A partitioned leader serves reads from a state machine that stopped "
        "advancing — stale data returned as current, with no error.",
    ),
    Mutation(
        name="read/served-before-a-current-term-entry-commits",
        file="raft/read.go",
        old="""	if !r.committedInCurrentTerm() {
		return ErrReadIndexUnavailable
	}""",
        new="""	if false {
		return ErrReadIndexUnavailable
	}""",
        expect="TestReadIndexWaitsForAnEntryFromTheCurrentTerm",
        why="A new leader's commit index understates what the cluster has "
        "committed, so the read can miss a write that already completed.",
    ),
    Mutation(
        name="session/retries-are-applied-twice",
        file="kvstore/kvstore.go",
        pkg="./kvstore/",
        old="""	if result, duplicate := s.checkSession(cmd.ClientID, cmd.Sequence); duplicate {""",
        new="""	if result, duplicate := s.checkSession(cmd.ClientID, cmd.Sequence); false {
		_ = result""",
        expect="TestRetriedWriteIsAppliedOnce|TestRetriedDeleteReplaysItsOriginalAnswer",
        why="Raft applies at least once, so a retried write lands twice. The map "
        "still looks right; the client-visible history does not, and that is what "
        "a linearizability checker reads.",
    ),
    Mutation(
        name="session/eviction-is-not-deterministic",
        file="kvstore/session.go",
        pkg="./kvstore/",
        old="""	slices.SortFunc(candidates, func(a, b candidate) int {
		if c := cmp.Compare(a.index, b.index); c != 0 {
			return c
		}
		return cmp.Compare(a.id, b.id)
	})""",
        new="""	_ = cmp.Compare[uint64]""",
        expect="TestSessionEvictionIsDeterministic",
        why="Nodes evict different sessions, so replicas diverge silently and only "
        "visibly much later.",
    ),
]


def run_tests(pattern: str, pkg: str = "./raft/") -> bool:
    """Return True if the test run passed."""
    result = subprocess.run(
        ["go", "test", pkg, "-count=1", "-run", pattern],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list", action="store_true", help="list mutations, run nothing")
    parser.add_argument("-k", metavar="SUBSTR", help="only mutations whose name contains SUBSTR")
    args = parser.parse_args()

    selected = [m for m in MUTATIONS if not args.k or args.k in m.name]
    if not selected:
        print(f"no mutation matches {args.k!r}", file=sys.stderr)
        return 2

    if args.list:
        for m in selected:
            print(f"{m.name}\n    {m.file} -> {m.expect}\n    {DIM}{m.why}{RESET}\n")
        return 0

    # Mutations are listed grouped by area; keep that order in the report.
    selected.sort(key=lambda m: m.name)

    # Confirm the suite is green first, or every result below is meaningless.
    if not run_tests(".", "./..."):
        print(f"{RED}the test suite fails before any mutation; fix that first{RESET}")
        return 2

    # A pattern that matches no test makes `go test` exit 0, which would report
    # the mutation as surviving for a reason that has nothing to do with the
    # code. Check up front that every expectation actually names a test.
    for m in selected:
        listed = subprocess.run(
            ["go", "test", m.pkg, "-run", m.expect, "-list", m.expect],
            cwd=ROOT, capture_output=True, text=True,
        )
        if not any(line.startswith("Test") for line in listed.stdout.splitlines()):
            print(f"{RED}{m.name}: no test in {m.pkg} matches {m.expect!r}{RESET}")
            return 2

    caught, survived = 0, []
    area = None
    for m in selected:
        if (this_area := m.name.split("/", 1)[0]) != area:
            area = this_area
            print(f"{DIM}{area}{RESET}")
        path = ROOT / m.file
        original = path.read_text(encoding="utf-8")

        if m.old not in original:
            print(f"  {YELLOW}SKIP{RESET}      {m.name}\n            pattern no longer present in {m.file}")
            survived.append(m.name + " (stale pattern)")
            continue

        path.write_text(original.replace(m.old, m.new, 1), encoding="utf-8")
        try:
            passed = run_tests(m.expect, m.pkg)
        finally:
            path.write_text(original, encoding="utf-8")

        if passed:
            print(f"  {RED}SURVIVED{RESET}  {m.name}")
            print(f"            {m.expect} still passes with the bug present")
            survived.append(m.name)
        else:
            print(f"  {GREEN}caught{RESET}    {m.name}")
            caught += 1

    print()
    print(f"{caught}/{len(selected)} mutations caught")
    if survived:
        print(f"{RED}survived:{RESET}")
        for name in survived:
            print(f"  - {name}")
        print("\nA surviving mutation means the test named after it cannot detect")
        print("the bug it is named after. Strengthen the test, do not delete it.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
