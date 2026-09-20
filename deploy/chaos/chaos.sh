#!/usr/bin/env bash
#
# Damages the network underneath a running raftkv cluster, on a schedule.
#
# The simulator already explores loss, delay, reordering and partitions far more
# thoroughly than this ever will, and it can replay anything it finds from a
# seed. This cannot do either. What it can do is use the *real* network stack,
# the real gRPC implementation, real fsyncs and the real Go scheduler — the
# things the simulator replaces with stand-ins, and therefore the things it
# cannot say anything about.
#
# Everything here runs inside the containers, so the faults apply to the
# cluster's own interfaces rather than to the host.
#
#   chaos.sh latency     add delay and loss to every node
#   chaos.sh partition   split the cluster, hold, then heal
#   chaos.sh kill        stop a node, hold, then restart it
#   chaos.sh run         a scheduled sequence of all of the above
#   chaos.sh heal        remove every fault
#   chaos.sh status      show what is currently applied

set -euo pipefail

NODES=(raftkv-node1 raftkv-node2 raftkv-node3 raftkv-node4 raftkv-node5)
IFACE="${IFACE:-eth0}"

# Defaults chosen against a 500ms–1s election timeout: enough delay to reorder
# messages and expose timing assumptions, not so much that the cluster spends
# the run electing leaders instead of doing work.
DELAY_MS="${DELAY_MS:-60}"
JITTER_MS="${JITTER_MS:-40}"
LOSS_PCT="${LOSS_PCT:-3}"
PARTITION_SECONDS="${PARTITION_SECONDS:-12}"
DOWN_SECONDS="${DOWN_SECONDS:-10}"

log() { printf '%s  %s\n' "$(date +%H:%M:%S)" "$*"; }

in_node() { docker exec "$1" "${@:2}"; }

require_cluster() {
	for node in "${NODES[@]}"; do
		if ! docker inspect -f '{{.State.Running}}' "$node" 2>/dev/null | grep -q true; then
			echo "chaos: $node is not running; start the cluster first" >&2
			exit 1
		fi
	done
}

# ---------------------------------------------------------------------------
# Latency and loss
# ---------------------------------------------------------------------------

# netem shapes everything leaving the interface. Applied to every node, so the
# delay a message experiences is the sum of both ends — which is what a real
# network does and what makes the effective round trip worth reasoning about.
apply_latency() {
	log "adding ${DELAY_MS}ms ±${JITTER_MS}ms delay and ${LOSS_PCT}% loss to every node"
	for node in "${NODES[@]}"; do
		in_node "$node" tc qdisc replace dev "$IFACE" root netem \
			delay "${DELAY_MS}ms" "${JITTER_MS}ms" distribution normal \
			loss "${LOSS_PCT}%" \
			reorder 5% 50% 2>/dev/null ||
			log "  warning: could not shape $node"
	done
}

clear_latency() {
	for node in "${NODES[@]}"; do
		in_node "$node" tc qdisc del dev "$IFACE" root 2>/dev/null || true
	done
}

# ---------------------------------------------------------------------------
# Partitions
# ---------------------------------------------------------------------------

# A partition is built by dropping traffic between two groups at the IP layer.
#
# Dropping rather than rejecting, deliberately: a rejected connection produces
# an immediate error, which a node notices at once. A dropped packet produces
# silence, and silence is indistinguishable from an idle peer — which is the
# situation that actually makes consensus hard, and the one a partitioned
# leader cannot tell from a healthy cluster with nothing to do.
partition() {
	local -a side_a=("$@")
	local -a side_b=()
	for node in "${NODES[@]}"; do
		local found=0
		for a in "${side_a[@]}"; do [[ "$node" == "$a" ]] && found=1; done
		((found)) || side_b+=("$node")
	done

	log "partitioning [${side_a[*]}] from [${side_b[*]}] for ${PARTITION_SECONDS}s"

	for a in "${side_a[@]}"; do
		for b in "${side_b[@]}"; do
			local b_ip
			b_ip=$(container_ip "$b")
			in_node "$a" iptables -A INPUT -s "$b_ip" -j DROP 2>/dev/null || true
			in_node "$a" iptables -A OUTPUT -d "$b_ip" -j DROP 2>/dev/null || true
		done
	done

	sleep "$PARTITION_SECONDS"
	log "healing the partition"
	clear_partitions
}

clear_partitions() {
	for node in "${NODES[@]}"; do
		in_node "$node" iptables -F INPUT 2>/dev/null || true
		in_node "$node" iptables -F OUTPUT 2>/dev/null || true
	done
}

container_ip() {
	docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$1"
}

# ---------------------------------------------------------------------------
# Crashes
# ---------------------------------------------------------------------------

# SIGKILL rather than a graceful stop. A clean shutdown is the easy case and the
# one already covered; what matters is the node that dies mid-write and has to
# recover from whatever reached the disk.
kill_node() {
	local node="$1"
	log "killing $node for ${DOWN_SECONDS}s"
	docker kill --signal=SIGKILL "$node" >/dev/null
	sleep "$DOWN_SECONDS"
	log "restarting $node"
	docker start "$node" >/dev/null
}

# ---------------------------------------------------------------------------

heal() {
	log "removing every fault"
	clear_latency
	clear_partitions
	for node in "${NODES[@]}"; do
		docker start "$node" >/dev/null 2>&1 || true
	done
}

status() {
	for node in "${NODES[@]}"; do
		local running qdisc rules
		running=$(docker inspect -f '{{.State.Running}}' "$node" 2>/dev/null || echo false)
		if [[ "$running" != true ]]; then
			printf '%-16s down\n' "$node"
			continue
		fi
		qdisc=$(in_node "$node" tc qdisc show dev "$IFACE" 2>/dev/null | head -1 | sed 's/^qdisc //')
		rules=$(in_node "$node" iptables -S 2>/dev/null | grep -c DROP || true)
		printf '%-16s up   drops=%-3s %s\n' "$node" "${rules:-0}" "${qdisc:-none}"
	done
}

# A scheduled sequence rather than a random one: the point is a repeatable run
# that can be described in a report, not another randomized search. Randomized
# search is what the simulator is for, and it does it better.
run_schedule() {
	require_cluster
	log "chaos run starting"

	apply_latency
	sleep 5

	partition raftkv-node1 raftkv-node2
	sleep 5

	kill_node raftkv-node3
	sleep 5

	# A partition that isolates a single node, which is the case where that node
	# campaigns into the void and returns with an inflated term — the one
	# pre-vote exists to make harmless.
	partition raftkv-node5
	sleep 5

	# Two failures at once on a five-node cluster: still a quorum, but only just.
	kill_node raftkv-node4 &
	partition raftkv-node1 raftkv-node2
	wait
	sleep 5

	heal
	log "chaos run finished"
}

case "${1:-run}" in
latency)
	require_cluster
	apply_latency
	;;
partition)
	require_cluster
	shift
	partition "${@:-raftkv-node1 raftkv-node2}"
	;;
kill)
	require_cluster
	kill_node "${2:-raftkv-node3}"
	;;
run)
	run_schedule
	;;
heal)
	heal
	;;
status)
	status
	;;
*)
	echo "usage: chaos.sh {run|latency|partition|kill|heal|status}" >&2
	exit 2
	;;
esac
