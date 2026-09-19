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
    """What bug is being introduced, in the words someone would use to describe it."""

    file: str
    old: str
    new: str

    expect: str
    """Go test name pattern that must fail once the mutation is applied."""

    why: str
    """What breaks in a real cluster if this mutation ships."""


MUTATIONS: list[Mutation] = [
    Mutation(
        name="election-restriction-compares-index-first",
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
        name="follower-truncates-entries-it-already-holds",
        file="raft/log.go",
        old="""	case conflict == 0:""",
        new="""	case false:""",
        expect="TestMaybeAppendIgnoresEntriesAlreadyHeld",
        why="A duplicated AppendEntries truncates the log and discards entries a "
        "later message already delivered.",
    ),
    Mutation(
        name="commit-index-can-move-backwards",
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
        name="node-can-vote-twice-in-one-term",
        file="raft/election.go",
        old="""	case r.vote == None && r.lead == None:""",
        new="""	case true:""",
        expect="TestNodeVotesAtMostOncePerTerm",
        why="Two leaders in one term. Election Safety is gone, and with it every "
        "guarantee built on top.",
    ),
    Mutation(
        name="pre-vote-ignores-whether-a-leader-is-alive",
        file="raft/election.go",
        old="""		return r.lead == None || r.electionElapsed >= r.randomizedElectionTimeout""",
        new="""		return true""",
        expect="TestPreVoteIsRefusedWhileALeaderIsAlive",
        why="Pre-vote becomes decorative: a node returning from a partition still "
        "deposes a healthy leader.",
    ),
    Mutation(
        name="rejected-pre-vote-echoes-the-asked-about-term",
        file="raft/election.go",
        old="""	if m.PreVote && granted {""",
        new="""	if m.PreVote {""",
        expect="TestRejectedPreVoteRepliesWithTheRespondersOwnTerm|TestPreVotePreventsDisruption",
        why="The refusal itself hands the candidate an inflated term, so pre-vote "
        "causes exactly the disruption it exists to prevent. This was a real bug.",
    ),
    Mutation(
        name="election-timeout-is-not-randomized",
        file="raft/raft.go",
        old="""	r.randomizedElectionTimeout = r.cfg.ElectionTick + r.cfg.Rand.Intn(r.cfg.ElectionTick)""",
        new="""	r.randomizedElectionTimeout = r.cfg.ElectionTick""",
        expect="TestSplitVotesAlwaysResolve",
        why="Nodes campaign in lockstep, split the vote, and retry in lockstep. "
        "The cluster can fail to elect a leader indefinitely.",
    ),
    Mutation(
        name="commit-on-quorum-alone-ignoring-the-leaders-term",
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
        name="commit-counts-next-instead-of-match",
        file="raft/replication.go",
        old="""			return p.Match""",
        new="""			return p.Next""",
        expect=".",
        why="Commits entries no follower has acknowledged, because Next is what the "
        "leader intends to send, not what arrived.",
    ),
    Mutation(
        name="conflict-hint-ignored-backing-up-one-index",
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
]


def run_tests(pattern: str) -> bool:
    """Return True if the test run passed."""
    result = subprocess.run(
        ["go", "test", "./raft/", "-count=1", "-run", pattern],
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

    # Confirm the suite is green first, or every result below is meaningless.
    if not run_tests("."):
        print(f"{RED}the test suite fails before any mutation; fix that first{RESET}")
        return 2

    caught, survived = 0, []
    for m in selected:
        path = ROOT / m.file
        original = path.read_text(encoding="utf-8")

        if m.old not in original:
            print(f"{YELLOW}SKIP{RESET}  {m.name}\n      pattern no longer present in {m.file}")
            survived.append(m.name + " (stale pattern)")
            continue

        path.write_text(original.replace(m.old, m.new, 1), encoding="utf-8")
        try:
            passed = run_tests(m.expect)
        finally:
            path.write_text(original, encoding="utf-8")

        if passed:
            print(f"{RED}SURVIVED{RESET}  {m.name}")
            print(f"          {m.expect} still passes with the bug present")
            survived.append(m.name)
        else:
            print(f"{GREEN}caught{RESET}    {m.name}")
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
