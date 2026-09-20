#!/usr/bin/env bash
#
# Installs Prometheus, Alertmanager and Grafana alongside the cluster, and
# proves the alerts fire by taking quorum away.
#
# The demonstration is the point. An alert rule that has never fired is an
# assertion: the expression may not match the metric names, the threshold may be
# unreachable, the routing may drop it. None of that shows up until an incident,
# which is the worst time to find out. So this breaks the cluster on purpose and
# checks the alert actually arrives at Alertmanager.
#
#   monitoring.sh up        install the stack and wait for it
#   monitoring.sh open      port-forward Grafana, Prometheus and Alertmanager
#   monitoring.sh verify    check every node is being scraped
#   monitoring.sh demo      break quorum, watch the alert fire, heal, watch it clear
#   monitoring.sh down      remove the stack

set -euo pipefail

NAMESPACE="${NAMESPACE:-monitoring}"
RELEASE="${RELEASE:-kv}"
APP_NAMESPACE="${APP_NAMESPACE:-default}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

GRAFANA_PORT="${GRAFANA_PORT:-3000}"
PROMETHEUS_PORT="${PROMETHEUS_PORT:-9090}"
ALERTMANAGER_PORT="${ALERTMANAGER_PORT:-9093}"

# How long the cluster is held below quorum. Long enough to clear the alert's
# 15s "for" clause plus a scrape interval and the group wait, with margin.
BREAK_SECONDS="${BREAK_SECONDS:-75}"

log() { printf '%s  %s\n' "$(date +%H:%M:%S)" "$*"; }
k() { kubectl --namespace "$NAMESPACE" "$@"; }
app() { kubectl --namespace "$APP_NAMESPACE" "$@"; }

fullname() { echo "${RELEASE}-raftkv"; }

# ---------------------------------------------------------------------------

up() {
	log "installing the monitoring stack"
	kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

	# The dashboard is a file in the repository, not a string inside a
	# manifest, so it can be reviewed and imported into any Grafana.
	kubectl --namespace "$NAMESPACE" create configmap grafana-dashboards \
		--from-file="$HERE/dashboards/" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

	kubectl apply -f "$HERE/prometheus.yaml" >/dev/null
	kubectl apply -f "$HERE/alertmanager.yaml" >/dev/null
	kubectl apply -f "$HERE/grafana.yaml" >/dev/null

	log "waiting for it to come up"
	for d in prometheus alertmanager grafana; do
		k rollout status "deployment/$d" --timeout=180s
	done
	log "installed"
	verify
}

down() {
	kubectl delete namespace "$NAMESPACE" --ignore-not-found
}

# Runs a query against Prometheus from inside the cluster, so no port-forward is
# needed and the check works the same in CI.
promql() {
	k run "promql-$$" --rm -i --restart=Never --quiet --image=curlimages/curl:8.10.1 -- \
		-fsS --max-time 10 --get --data-urlencode "query=$1" \
		"http://prometheus:9090/api/v1/query" 2>/dev/null
}

alerts_firing() {
	k run "alerts-$$" --rm -i --restart=Never --quiet --image=curlimages/curl:8.10.1 -- \
		-fsS --max-time 10 "http://alertmanager:9093/api/v2/alerts?active=true" 2>/dev/null
}

# ---------------------------------------------------------------------------

# An alert cannot fire on a metric that is not being collected, so the first
# thing to establish is that every node is actually a target.
verify() {
	log "checking every node is being scraped"

	local expected actual
	expected=$(app get pods -l "app.kubernetes.io/instance=${RELEASE}" --no-headers | wc -l | tr -d ' ')
	actual=$(promql 'count(raftkv_up == 1)' | sed -n 's/.*"value":\[[0-9.]*,"\([0-9]*\)"\].*/\1/p')

	if [[ "${actual:-0}" != "$expected" ]]; then
		log "only ${actual:-0} of $expected nodes are reporting; giving it a moment"
		sleep 15
		actual=$(promql 'count(raftkv_up == 1)' | sed -n 's/.*"value":\[[0-9.]*,"\([0-9]*\)"\].*/\1/p')
	fi

	printf '  nodes scraped:   %s of %s\n' "${actual:-0}" "$expected"
	printf '  leader:          %s\n' \
		"$(promql 'raftkv_raft_is_leader == 1' | sed -n 's/.*"node":"\([0-9]*\)".*/node \1/p' | head -1)"
	printf '  term:            %s\n' \
		"$(promql 'max(raftkv_raft_term)' | sed -n 's/.*"value":\[[0-9.]*,"\([0-9]*\)"\].*/\1/p')"
	printf '  rules loaded:    %s\n' \
		"$(promql 'count(ALERTS) or vector(0)' | grep -c value || echo 0)"

	[[ "${actual:-0}" == "$expected" ]]
}

# Takes quorum away and checks the alert arrives.
#
# Three of five nodes are scaled away rather than partitioned, because the
# question here is whether the *alerting* works, not whether Raft does. The
# chaos suite covers partitions; this covers the rule, the threshold and the
# routing.
demo() {
	local replicas quorum
	replicas=$(app get statefulset "$(fullname)" -o jsonpath='{.spec.replicas}')
	quorum=$((replicas / 2 + 1))
	log "cluster has $replicas nodes, quorum is $quorum"

	log "scaling down to $((quorum - 1)) — one short of a majority"
	app scale statefulset "$(fullname)" --replicas=$((quorum - 1)) >/dev/null
	app wait --for=delete "pod/$(fullname)-$((replicas - 1))" --timeout=120s >/dev/null 2>&1 || true

	log "holding below quorum for ${BREAK_SECONDS}s"
	local fired=""
	local deadline=$((SECONDS + BREAK_SECONDS))
	while ((SECONDS < deadline)); do
		fired=$(alerts_firing | tr ',' '\n' | sed -n 's/.*"alertname":"\(RaftKV[A-Za-z]*\)".*/\1/p' | sort -u | tr '\n' ' ')
		if [[ -n "$fired" ]]; then
			log "alerts firing: $fired"
			break
		fi
		sleep 5
	done

	if [[ -z "$fired" ]]; then
		log "no alert fired while the cluster was below quorum — the rule is not working"
		app scale statefulset "$(fullname)" --replicas="$replicas" >/dev/null
		return 1
	fi

	# Let it settle so the report shows the full set rather than whichever
	# happened to evaluate first.
	sleep 20
	echo
	echo "firing while below quorum:"
	alerts_firing | tr ',' '\n' | sed -n 's/.*"alertname":"\(RaftKV[A-Za-z]*\)".*/  \1/p' | sort -u
	echo

	log "healing: scaling back to $replicas"
	app scale statefulset "$(fullname)" --replicas="$replicas" >/dev/null
	app rollout status "statefulset/$(fullname)" --timeout=180s

	log "waiting for the alerts to clear"
	local cleared=0
	deadline=$((SECONDS + 120))
	while ((SECONDS < deadline)); do
		if [[ -z "$(alerts_firing | sed -n 's/.*"alertname":"\(RaftKV[A-Za-z]*\)".*/\1/p')" ]]; then
			cleared=1
			break
		fi
		sleep 5
	done

	if ((cleared)); then
		log "alerts cleared; the cluster recovered on its own"
	else
		log "alerts are still firing after recovery — check the rules"
		return 1
	fi
}

open_ui() {
	log "Grafana      http://localhost:${GRAFANA_PORT}"
	log "Prometheus   http://localhost:${PROMETHEUS_PORT}"
	log "Alertmanager http://localhost:${ALERTMANAGER_PORT}"
	log "ctrl-c to stop"
	k port-forward svc/grafana "${GRAFANA_PORT}:3000" >/dev/null 2>&1 &
	k port-forward svc/prometheus "${PROMETHEUS_PORT}:9090" >/dev/null 2>&1 &
	k port-forward svc/alertmanager "${ALERTMANAGER_PORT}:9093" >/dev/null 2>&1 &
	trap 'kill $(jobs -p) 2>/dev/null' EXIT
	wait
}

case "${1:-up}" in
up) up ;;
down) down ;;
verify) verify ;;
demo) demo ;;
open) open_ui ;;
*)
	echo "usage: monitoring.sh {up|down|verify|demo|open}" >&2
	exit 2
	;;
esac
