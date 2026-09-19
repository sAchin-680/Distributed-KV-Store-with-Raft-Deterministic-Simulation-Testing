# Architecture decision records

Short documents recording decisions that were expensive to make and would be
expensive to reverse — the reasoning, the alternatives, and what the decision
costs. Not a design document: an ADR captures *why*, at a moment in time, and is
never edited afterwards except to mark it superseded.

| ADR | Decision | Status |
| --- | --- | --- |
| [0001](0001-pure-deterministic-core.md) | The consensus core is a pure state machine with no clock, I/O or goroutines | Accepted |
| [0002](0002-synchronous-persistence.md) | Persist synchronously in the core, accepting a measured ~125 writes/sec ceiling | Accepted |
| [0003](0003-read-index.md) | Read-index for linearizable reads, rather than naive leader reads or a lease | Accepted |
| 0004 | StatefulSet rather than Deployment | Planned |

## Format

```text
#  Title

## Status
Proposed | Accepted | Superseded by NNNN

## Context
The forces at play. What makes this a decision rather than an obvious choice.

## Decision
What was chosen, stated plainly.

## Consequences
What this buys, and what it costs. The costs are the part worth writing down —
they are what a future reader needs in order to judge whether the decision still
holds.

## Alternatives considered
What else was on the table and why it lost.
```
