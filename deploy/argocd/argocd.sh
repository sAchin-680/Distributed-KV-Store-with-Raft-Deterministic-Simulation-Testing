#!/usr/bin/env bash
#
# Installs ArgoCD, points it at the chart in this repository, and then breaks
# the cluster by hand to show it put back.
#
# The demonstration is the whole value. "Self-healing" is a checkbox in a YAML
# file until something has actually drifted and been corrected, and the failure
# modes are quiet ones: a project that forbids the resource, a sync policy that
# detects drift but never acts, an Application stuck Progressing because a
# StatefulSet never goes Ready. None of that is visible from the manifest.
#
#   argocd.sh up       install ArgoCD and create the Application
#   argocd.sh status    show what ArgoCD thinks the cluster should be
#   argocd.sh demo      scale the cluster below quorum by hand, watch it healed
#   argocd.sh open      port-forward the UI, print the admin password
#   argocd.sh down      remove ArgoCD and the Application

set -euo pipefail

NAMESPACE="${NAMESPACE:-argocd}"
APP="${APP:-raftkv-staging}"
APP_NAMESPACE="${APP_NAMESPACE:-raftkv-staging}"
RELEASE="${RELEASE:-kv}"
ARGOCD_VERSION="${ARGOCD_VERSION:-v2.13.2}"
UI_PORT="${UI_PORT:-8080}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log() { printf '%s  %s\n' "$(date +%H:%M:%S)" "$*"; }
k() { kubectl --namespace "$NAMESPACE" "$@"; }
app() { kubectl --namespace "$APP_NAMESPACE" "$@"; }

fullname() { echo "${RELEASE}-raftkv"; }

argo() { k get application "$APP" -o jsonpath="$1" 2>/dev/null; }

sync_status() { argo '{.status.sync.status}'; }
health_status() { argo '{.status.health.status}'; }

# The image tag each environment is pinned to, read straight out of the values
# file the pipeline writes. Anchored on the one "  tag:" line rather than a
# window after "image:", because the comment block above the tag varies in
# length between the two files and a fixed -A window silently misses it.
pinned_tag() { sed -n 's/^  tag: *//p' "$1" 2>/dev/null | tr -d '"' | head -1; }

status() {
	for a in raftkv-staging raftkv-production; do
		# The Application name is also its namespace: both come from the
		# environment name, so they are derived rather than coincidental.
		printf '  %-18s %-10s %-9s %s pods   %s\n' "$a" \
			"$(k get application "$a" -o jsonpath='{.status.sync.status}' 2>/dev/null)" \
			"$(k get application "$a" -o jsonpath='{.status.health.status}' 2>/dev/null)" \
			"$(kubectl -n "$a" get pods -l app.kubernetes.io/name=raftkv \
				-o jsonpath='{range .items[*]}{.status.containerStatuses[0].ready}{"\n"}{end}' 2>/dev/null | grep -c true || echo 0)" \
			"$(k get application "$a" -o jsonpath='{.status.sync.revision}' 2>/dev/null | cut -c1-8)"
	done
	printf '  tracking:          %s @ %s\n' \
		"$(argo '{.spec.source.repoURL}')" "$(argo '{.spec.source.targetRevision}')"
	printf '  staging image:     %s\n' "$(pinned_tag "$HERE/../helm/raftkv/values-staging.yaml")"
	printf '  production image:  %s\n' "$(pinned_tag "$HERE/../helm/raftkv/values-production.yaml")"
}

# Breaks the cluster the way a person would, and times the correction.
#
# Scaling below quorum rather than deleting a pod, deliberately. Deleting a pod
# is something Kubernetes already fixes on its own, so it would demonstrate the
# StatefulSet controller rather than ArgoCD. Changing the replica count is drift
# from what the repository says, which only a GitOps controller notices — and it
# is the realistic mistake, because it looks like a reasonable thing to do
# during an incident and quietly leaves the cluster unable to commit.
demo() {
	local desired
	desired=$(app get statefulset "$(fullname)" -o jsonpath='{.spec.replicas}')
	log "the chart says ${desired} nodes; quorum is $((desired / 2 + 1))"

	log "scaling to 2 by hand — below quorum, exactly the mistake that hurts"
	app scale statefulset "$(fullname)" --replicas=2 >/dev/null
	local broke=$SECONDS

	log "waiting for ArgoCD to notice"
	local detected=0
	while ((SECONDS - broke < 180)); do
		if [[ "$(sync_status)" == OutOfSync ]]; then
			detected=$((SECONDS - broke))
			log "detected as OutOfSync after ${detected}s"
			break
		fi
		# Self-heal can be fast enough that OutOfSync is never observed between
		# polls. Reaching the right replica count is the outcome either way.
		if [[ "$(app get statefulset "$(fullname)" -o jsonpath='{.spec.replicas}')" == "$desired" ]]; then
			log "already corrected before a poll caught it OutOfSync"
			break
		fi
		sleep 2
	done

	log "waiting for the replica count to be restored"
	local healed=0
	while ((SECONDS - broke < 300)); do
		if [[ "$(app get statefulset "$(fullname)" -o jsonpath='{.spec.replicas}')" == "$desired" ]]; then
			healed=$((SECONDS - broke))
			log "replica count back to ${desired} after ${healed}s"
			break
		fi
		sleep 2
	done

	if ((healed == 0)); then
		log "ArgoCD did not restore the replica count; self-heal is not working"
		return 1
	fi

	log "waiting for the cluster to be healthy again"
	app rollout status "statefulset/$(fullname)" --timeout=300s

	local deadline=$((SECONDS + 120))
	while ((SECONDS < deadline)); do
		[[ "$(sync_status)" == Synced && "$(health_status)" == Healthy ]] && break
		sleep 5
	done

	echo
	echo "drift corrected without anyone being asked:"
	printf '  detected after:  %ss\n' "${detected:-under one poll}"
	printf '  replicas back:   %ss\n' "$healed"
	printf '  final state:     %s / %s\n' "$(sync_status)" "$(health_status)"
	echo

	# The second half of the demonstration: prune. Deleting a resource the chart
	# declares is drift in the other direction, and it is the one that matters
	# most here — without the PodDisruptionBudget, a node drain can take the
	# cluster below quorum and nothing will stop it.
	log "deleting the PodDisruptionBudget by hand"
	app delete poddisruptionbudget "$(fullname)" --ignore-not-found >/dev/null
	local pdbBroke=$SECONDS
	local pdbBack=0
	while ((SECONDS - pdbBroke < 300)); do
		if app get poddisruptionbudget "$(fullname)" >/dev/null 2>&1; then
			pdbBack=$((SECONDS - pdbBroke))
			break
		fi
		sleep 2
	done

	if ((pdbBack == 0)); then
		log "the PodDisruptionBudget was not restored"
		return 1
	fi
	log "PodDisruptionBudget restored after ${pdbBack}s"
	app get poddisruptionbudget "$(fullname)"
}

open_ui() {
	local password
	password=$(k get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' 2>/dev/null | base64 -d || echo '(secret not found)')
	log "ArgoCD  https://localhost:${UI_PORT}   user: admin   password: ${password}"
	log "the certificate is self-signed, so the browser will warn"
	k port-forward svc/argocd-server "${UI_PORT}:443"
}

case "${1:-up}" in
up) up ;;
down) down ;;
status) status ;;
demo) demo ;;
open) open_ui ;;
*)
	echo "usage: argocd.sh {up|down|status|demo|open}" >&2
	exit 2
	;;
esac
