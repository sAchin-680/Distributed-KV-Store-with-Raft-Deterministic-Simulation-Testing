{{- define "raftkv.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "raftkv.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "raftkv.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "raftkv.labels" -}}
app.kubernetes.io/name: {{ include "raftkv.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "raftkv.selectorLabels" -}}
app.kubernetes.io/name: {{ include "raftkv.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "raftkv.headless" -}}
{{- printf "%s-headless" (include "raftkv.fullname" .) -}}
{{- end -}}

{{/*
The peer list, built from the stable DNS names a StatefulSet guarantees.

This is the whole reason for the StatefulSet. Every node is addressed as
<id>@<name>-<ordinal>.<headless>:<port>, and those names outlive any individual
pod — a pod that is rescheduled onto a different machine with a different IP
comes back at the same name. Raft addresses peers by an identity that must hold
for the life of the cluster, and a Deployment has no such identity to offer.

Node IDs are the ordinal plus one, because zero means "no node" in the protocol.
*/}}
{{- define "raftkv.peers" -}}
{{- $full := include "raftkv.fullname" . -}}
{{- $svc := include "raftkv.headless" . -}}
{{- $port := .Values.ports.peer -}}
{{- $peers := list -}}
{{- range $i := until (int .Values.replicaCount) -}}
{{- $peers = append $peers (printf "%d@%s-%d.%s:%d" (add1 $i) $full $i $svc (int $port)) -}}
{{- end -}}
{{- join "," $peers -}}
{{- end -}}

{{/*
A majority of the configured replicas. What the disruption budget protects.
*/}}
{{- define "raftkv.quorum" -}}
{{- div (int .Values.replicaCount) 2 | add1 -}}
{{- end -}}
