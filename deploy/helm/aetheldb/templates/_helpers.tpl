{{/* SPDX-License-Identifier: Apache-2.0 */}}
{{/* Resource-name prefix (release name unless overridden). */}}
{{- define "aetheldb.fullname" -}}
{{- if .Values.fullnameOverride }}{{ .Values.fullnameOverride | trunc 50 | trimSuffix "-" }}{{- else }}{{ .Release.Name | trunc 50 | trimSuffix "-" }}{{- end }}
{{- end -}}

{{/* Common labels. */}}
{{- define "aetheldb.labels" -}}
app.kubernetes.io/name: aetheldb
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/part-of: aetheldb
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: aetheldb-{{ .Chart.Version }}
{{- end -}}

{{/* Per-service image reference. Usage: include "aetheldb.image" (dict "svc" "pageserver" "root" $) */}}
{{- define "aetheldb.image" -}}
{{- $img := .root.Values.image -}}
{{- printf "%s/%s/%s:%s" $img.registry $img.repository .svc $img.tag -}}
{{- end -}}

{{/* The Secret holding object-store credentials and the control token. */}}
{{- define "aetheldb.secretName" -}}
{{- if .Values.objectStore.existingSecret }}{{ .Values.objectStore.existingSecret }}{{- else }}{{ include "aetheldb.fullname" . }}-secrets{{- end }}
{{- end -}}

{{/* The headless safekeeper service name (for stable pod DNS). */}}
{{- define "aetheldb.safekeeperSvc" -}}
{{ include "aetheldb.fullname" . }}-safekeeper
{{- end -}}

{{/* Whether the chart should render its own Secret. */}}
{{- define "aetheldb.manageSecret" -}}
{{- if and (not .Values.objectStore.existingSecret) (or .Values.objectStore.credentials (and .Values.controlToken.value (not .Values.controlToken.existingSecret))) }}true{{- end }}
{{- end -}}
