# Chaos runs

Recorded histories from a five-node cluster under real network faults, and the
result of checking each one for linearizability.

Every run: five containers, one process each, with `tc`/`netem` applying 60ms
±40ms delay, 3% loss and 5% reordering to every node, plus three `iptables`
partitions and two `SIGKILL`s on a fixed schedule. Ten concurrent clients over
four keys for 75 seconds.

## Results

| Run | Clients | Keys | Operations | Unknown outcome | Result | Search time |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 10 | 4 | 3,835 | 326 (8.5%) | undecided — see below | gave up |
| 2 | 10 | 4 | 3,471 | 325 (9.4%) | linearizable | 197 ms |
| 3 | 10 | 4 | 3,327 | 309 (9.3%) | linearizable | 27 ms |
| 4 | 5 | 6 | 341 | 116 (34%) | linearizable | 19 ms |

Run 4 was recorded deliberately after run 1 proved undecidable, with fewer
clients spread over more keys so the search would terminate. Its far lower
operation count is an artefact of the machine also running run 1's search at the
time; its very high proportion of unknown outcomes is not, and makes it the
harshest of the four despite being the smallest.

"Unknown outcome" means the client never learned whether the operation took
effect — a timeout, or a leader change mid-write. Those are kept in the history
as invoked-and-never-returned rather than discarded, because the write may have
committed anyway. Dropping them would remove exactly the operations most likely
to be involved in a violation, and could let an illegal history check clean.

Roughly 9% of operations ending in an unknown outcome is what three partitions
and two kills in 75 seconds should produce. A run with none would mean the
faults were not reaching the cluster.

## Reproducing

```bash
make chaos-up                 # five containers, wait for a leader
make chaos-run                # faults and workload together, then check
make chaos-check HISTORY=docs/chaos-runs/history-....json.gz
make chaos-down               # stop and delete the data
```

The histories here are gzipped; `chaos-check` reads either form.

## The first attempt failed, and the harness was at fault

The first three runs reported **NOT LINEARIZABLE**. The checker was right and
the store was not at fault.

Tracing the violation showed reads returning values that appeared nowhere in
that run's history — values written by a client in the *previous* run. Docker
volumes persist between runs, so each run was reading state an earlier one had
left behind, and a history that contains a read of a value it never shows being
written cannot be linearizable no matter how correct the system is.

Fixed by namespacing each run's keys, so a history is self-contained: every
value it reads is one it can see written.

Worth recording rather than quietly fixing. The brief for this phase says a
failing check means either a real bug or a recording bug, and chasing down which
is the work. Here it was the recording, and reporting it as a consensus bug
would have been wrong.

## An inconclusive search is not a pass

Run 1's first check ran out of time. Porcupine's search is exponential in the
number of concurrent operations, and a run with 326 unknown outcomes across only
four keys is close to the worst case for it — every unknown operation can be
placed almost anywhere, which multiplies the orderings to consider.

That result is reported as `UNKNOWN`, and `chaosctl` exits non-zero for it. It
has not been shown to be legal; it has only failed to be decided. Treating "I
gave up" as "it passed" is how a checker becomes decorative, which is the same
failure mode the mutation suite exists to prevent elsewhere in this project.

The fix is not to widen the timeout until it says yes. It is to record a history
the search can decide — fewer clients spread over more keys — which is what run
4 is. Re-checking a *saved* history with more time is the other legitimate move,
since re-running the experiment answers a different question; `chaosctl --check`
exists for exactly that.

Run 1 remains undecided and is recorded that way. Three of four runs are
linearizable and one could not be decided; reporting that as "four passed" would
be the same error as reading an exhausted search as success.
