#!/usr/bin/env bash
#
# Rolls every pod of a running cluster while clients are writing to it, and
# reports what the clients saw.
#
# The claim being tested is narrow and worth stating precisely: a rolling
# upgrade replaces every node, one at a time, without the cluster ever losing
# quorum — so a client that retries sees no failed operations, only slower ones
# while leadership moves.
#
# Three things have to hold for that to be true, and each is somewhere else in
# this directory:
#
#   - the PodDisruptionBudget stops Kubernetes taking more than one node at once
#   - the StatefulSet's identity survives the restart, so a returning pod
#     rejoins as the same voter rather than as a new one
#   - the client follows the leader hint rather than pinning to a pod
#
# The workload runs *inside* the cluster. A port-forward would break when its
# target pod restarts, and the resulting errors would be the harness failing,
# not the store.
#
#   rollout.sh run      drive a workload, roll the cluster, report
#   rollout.sh clean    remove the workload pod

set -euo pipefail

RELEASE="${RELEASE:-kv}"
NAMESPACE="${NAMESPACE:-default}"
CHART="${CHART:-deploy/helm/raftkv}"

# Long enough to cover the whole rollout with a margin either side. Five pods
# at roughly ten seconds each, plus the settle time before and after.
DURATION="${DURATION:-150s}"
SETTLE_SECONDS="${SETTLE_SECONDS:-10}"
CLIENTS="${CLIENTS:-8}"
KEYS="${KEYS:-5}"

# Operations are given longer than usual here. A two-second timeout would
# record an operation as unknown whenever it spanned an election, and an
# election is precisely what a rolling upgrade causes — the report would then
# measure the timeout rather than the cluster. Failover takes 1-2s by
# construction (tick x electionTicks, randomised to double), so anything above
# about 3s is the cluster genuinely not making progress.
OP_TIMEOUT="${OP_TIMEOUT:-5s}"

WORKLOAD_POD="${RELEASE}-rollout-workload"
OUT_DIR="${OUT_DIR:-docs/rollout}"

k() { kubectl --namespace "$NAMESPACE" "$@"; }

log() { printf '%s  %s\n' "$(date +%H:%M:%S)" "$*"; }

fullname() { echo "${RELEASE}-raftkv"; }

pods() { k get pods -l "app.kubernetes.io/instance=${RELEASE}" -o name | sed 's|pod/||' | sort; }

# Names are stable across a rollout — that is the whole point of a StatefulSet —
# so the evidence that anything was replaced is the pod UID, which is not.
identities() {
	k get pods -l "app.kubernetes.io/instance=${RELEASE}" \
		-o custom-columns='POD:.metadata.name,UID:.metadata.uid,STARTED:.status.startTime' \
		--no-headers | sort
}

# ---------------------------------------------------------------------------

clean() {
	k delete pod "$WORKLOAD_POD" --ignore-not-found --now >/dev/null 2>&1 || true
}

require_cluster() {
	local ready
	ready=$(k get pods -l "app.kubernetes.io/instance=${RELEASE}" \
		-o jsonpath='{range .items[*]}{.status.containerStatuses[0].ready}{"\n"}{end}' |
		grep -c true || true)
	if [[ "${ready:-0}" -lt 3 ]]; then
		echo "rollout: only ${ready:-0} pods are ready; the cluster must be healthy before rolling it" >&2
		exit 1
	fi
	log "cluster healthy: ${ready} pods ready"
}

endpoints() {
	local image_port list=""
	image_port=$(k get svc "$(fullname)" -o jsonpath='{.spec.ports[0].port}')
	for pod in $(pods); do
		list+="${pod}.$(fullname)-headless:${image_port},"
	done
	echo "${list%,}"
}

# The workload addresses pods individually rather than through the load-balanced
# service. With one virtual IP the client cannot tell endpoints apart, so its
# failover — try the next address when this one refuses — has nothing to move
# to. Naming all five gives it somewhere to go the instant one is replaced.
start_workload() {
	local image
	image=$(k get statefulset "$(fullname)" -o jsonpath='{.spec.template.spec.containers[0].image}')

	log "starting workload: ${CLIENTS} clients, ${KEYS} keys, ${DURATION}"
	k run "$WORKLOAD_POD" --restart=Never --image="$image" --image-pull-policy=IfNotPresent \
		--command -- /usr/local/bin/chaosctl \
		--endpoints="$(endpoints)" \
		--duration="$DURATION" \
		--clients="$CLIENTS" \
		--keys="$KEYS" \
		--op-timeout="$OP_TIMEOUT" \
		--check-timeout=120s \
		--key-prefix="rollout-$(date +%s)" >/dev/null

	k wait --for=condition=Ready "pod/$WORKLOAD_POD" --timeout=60s >/dev/null
	log "workload running"
}

# The rollout itself.
#
# `rollout restart` rather than a version bump, deliberately: it replaces every
# pod with an identical one, which isolates what is being measured. A real
# upgrade also changes the binary, and a failure would then be ambiguous
# between "rolling is unsafe" and "the new binary is broken". Kubernetes treats
# both identically, so the restart is the honest test of the mechanism.
roll() {
	log "rolling the statefulset"
	k rollout restart "statefulset/$(fullname)"

	# Descending ordinal order, one at a time, each waiting for the previous to
	# report Ready. Readiness requires a known leader, so the wait is not
	# cosmetic: it is what stops the next pod going down before the cluster has
	# re-formed around the last one.
	local start elapsed
	start=$(date +%s)
	k rollout status "statefulset/$(fullname)" --timeout=300s
	elapsed=$(($(date +%s) - start))
	log "rollout finished in ${elapsed}s"
	echo "$elapsed" >"${TMPDIR:-/tmp}/rollout-elapsed.$$"
}

run() {
	require_cluster
	clean
	mkdir -p "$OUT_DIR"

	local before
	before=$(identities)
	log "cluster before the rollout:"
	echo "$before"

	start_workload

	log "letting the workload settle for ${SETTLE_SECONDS}s before touching anything"
	sleep "$SETTLE_SECONDS"

	roll

	log "waiting for the workload to finish"
	k wait --for=jsonpath='{.status.phase}'=Succeeded "pod/$WORKLOAD_POD" --timeout=300s >/dev/null 2>&1 ||
		k wait --for=jsonpath='{.status.phase}'=Failed "pod/$WORKLOAD_POD" --timeout=60s >/dev/null 2>&1 || true

	local stamp report
	stamp=$(date -u +%Y%m%d-%H%M%S)
	report="${OUT_DIR}/rollout-${stamp}.log"

	{
		echo "rolling upgrade, ${stamp}"
		echo
		echo "rollout took: $(cat "${TMPDIR:-/tmp}/rollout-elapsed.$$" 2>/dev/null || echo '?')s"
		echo
		echo "before (names are stable; the UID is what changes):"
		echo "$before"
		echo
		echo "after:"
		identities
		echo
		echo "--- workload ---"
		k logs "$WORKLOAD_POD"
	} | tee "$report"

	rm -f "${TMPDIR:-/tmp}/rollout-elapsed.$$"
	log "report written to $report"

	# The exit status is the workload's: a non-zero one means either the check
	# found a violation or it did not finish, and both matter.
	local phase
	phase=$(k get pod "$WORKLOAD_POD" -o jsonpath='{.status.phase}')
	clean
	[[ "$phase" == Succeeded ]]
}

case "${1:-run}" in
run) run ;;
clean) clean ;;
*)
	echo "usage: rollout.sh {run|clean}" >&2
	exit 2
	;;
esac
