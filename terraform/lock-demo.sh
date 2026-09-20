#!/usr/bin/env bash
#
# Shows the state lock refusing a concurrent operation.
#
# Worth demonstrating rather than asserting. "Remote state with locking" is a
# line in a backend block until something has actually been refused, and the
# failure mode is silent: a backend with no locking looks identical until two
# applies overlap and one of them writes over the other's state.
#
# Four plans are started at once. One acquires the Lease; the rest are refused.

set -euo pipefail

ENVIRONMENT="${ENVIRONMENT:-staging}"
CONCURRENCY="${CONCURRENCY:-4}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DIR="$HERE/environments/$ENVIRONMENT"
OUT="$(mktemp -d)"

trap 'rm -rf "$OUT"' EXIT

[[ -d "$DIR/.terraform" ]] || {
	echo "run 'terraform init' in $DIR first" >&2
	exit 2
}

echo "starting $CONCURRENCY concurrent plans against $ENVIRONMENT"

for i in $(seq 1 "$CONCURRENCY"); do
	# -lock-timeout=0 so a blocked run reports immediately instead of waiting.
	# The default waits, which is the right behaviour in a pipeline and the
	# wrong one for showing that the lock exists.
	(cd "$DIR" && terraform plan -lock-timeout=0s -no-color >"$OUT/$i.txt" 2>&1) &
done
wait

echo
acquired=0
blocked=0
for i in $(seq 1 "$CONCURRENCY"); do
	if grep -qi "Error acquiring the state lock" "$OUT/$i.txt"; then
		printf '  plan %s  refused by the lock\n' "$i"
		blocked=$((blocked + 1))
	else
		printf '  plan %s  acquired the lock and ran\n' "$i"
		acquired=$((acquired + 1))
	fi
done

echo
echo "$acquired ran, $blocked refused"
echo
echo "the Lease that did it:"
kubectl -n terraform-state get lease "lock-tfstate-default-${ENVIRONMENT}" 2>/dev/null ||
	echo "  (released — a Lease only exists while an operation holds it)"

# Exactly one should have run. Zero means nothing was tested; more than one
# means the lock is not doing its job, which is the failure this exists to
# catch.
if ((acquired != 1)); then
	echo
	echo "expected exactly one to acquire the lock, got $acquired" >&2
	exit 1
fi
