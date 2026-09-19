# Architecture decision records

Short documents recording decisions that were expensive to make and would be
expensive to reverse — the reasoning, the alternatives, and what the decision
costs. Not a design document: an ADR captures *why*, at a moment in time, and is
never edited afterwards except to mark it superseded.

| ADR | Decision | Status |
| --- | --- | --- |
| [0001](0001-pure-deterministic-core.md) | The consensus core is a pure state machine with no clock, I/O or goroutines | Accepted |
| 0002 | Read-index rather than naive leader reads | Planned (M6) |
| 0003 | Synchronous persistence in the core, giving up batching | Planned (M6) |
| 0004 | StatefulSet rather than Deployment | Planned (M11) |

## Format

```text
# NNNN — Title

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
