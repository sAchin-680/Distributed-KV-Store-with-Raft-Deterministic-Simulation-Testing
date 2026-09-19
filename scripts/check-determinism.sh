#!/usr/bin/env bash
#
# Guards the properties that make seed-based replay work.
#
# The value of the whole simulation framework rests on one thing: the same seed
# must produce the same execution, every time, forever. That property does not
# announce itself when it breaks. A fuzz run just stops reproducing its own
# failures, and by the time anyone notices, the cause is a hundred commits back.
#
# These are cheap greps. They are worth more than the discipline they replace.
#
# Note the fourth hazard — deriving a decision from Go's randomized map iteration
# order — is not statically checkable in any honest way. It is covered instead by
# sim.TestSeedReplayProducesIdenticalTrace, which runs a seed twice and compares
# a hash of the full event trace. That test is the real guard; these greps just
# catch the three mistakes that are easy to make and easy to detect.

set -euo pipefail

CORE_DIRS=(raft)
fail=0

red()  { printf '\033[31m%s\033[0m\n' "$1"; }
green(){ printf '\033[32m%s\033[0m\n' "$1"; }

report() {
	red "FAIL: $1"
	printf '%s\n' "$2" | sed 's/^/      /'
	fail=1
}

go_sources() {
	for d in "${CORE_DIRS[@]}"; do
		[[ -d $d ]] && find "$d" -name '*.go' -not -name '*_test.go'
	done
}

mapfile -t CORE_FILES < <(go_sources)

if [[ ${#CORE_FILES[@]} -eq 0 ]]; then
	echo "no core sources yet; nothing to check"
	exit 0
fi

# 1. No wall-clock reads in the core. The core measures time in ticks handed to
#    it from outside; if it ever asks the OS what time it is, the simulator's
#    virtual clock stops being authoritative.
#    time.Duration is allowed — it is a unit, not a clock.
if hits=$(grep -nE '\btime\.[A-Z]' "${CORE_FILES[@]}" | grep -vE '\btime\.Duration\b' || true); [[ -n $hits ]]; then
	report "wall-clock access inside the consensus core" "$hits"
fi

# 2. No package-level math/rand. Every random decision must come from one
#    explicitly-passed, seeded *rand.Rand. Package-level calls read the global
#    source, which is seeded independently and shared across goroutines.
if hits=$(grep -rnE '\brand\.(Int|Intn|Int31|Int63|Float32|Float64|Perm|Shuffle|Read|Seed|NormFloat64|ExpFloat64)\b' \
	--include='*.go' . | grep -v '/proto/' || true); [[ -n $hits ]]; then
	report "package-level math/rand use (seed the source and pass it explicitly)" "$hits"
fi

# 3. No goroutines in the core. Concurrency is the driver's job. The core is a
#    synchronous state machine so that the simulator can step it one event at a
#    time and know that nothing else is in flight.
if hits=$(grep -nE '^[[:space:]]*go[[:space:]]+[a-zA-Z_(]' "${CORE_FILES[@]}" || true); [[ -n $hits ]]; then
	report "goroutine started inside the consensus core" "$hits"
fi

# 4. No channels or mutexes in the core either — same reason, and their presence
#    is a reliable sign that state is about to be shared across goroutines.
if hits=$(grep -nE '(chan\s|sync\.(Mutex|RWMutex|WaitGroup))' "${CORE_FILES[@]}" || true); [[ -n $hits ]]; then
	report "channel or lock inside the consensus core" "$hits"
fi

if [[ $fail -eq 0 ]]; then
	green "determinism guards passed (${#CORE_FILES[@]} core files checked)"
fi
exit $fail
