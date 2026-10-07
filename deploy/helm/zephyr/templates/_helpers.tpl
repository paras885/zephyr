{{/*
Chart name, truncated and sanitized for use in Kubernetes object names.
*/}}
{{- define "zephyr.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Fully qualified app name, used as the default release-scoped resource prefix.
*/}}
{{- define "zephyr.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "zephyr.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels applied to every object this chart renders.
*/}}
{{- define "zephyr.labels" -}}
helm.sh/chart: {{ include "zephyr.chart" . }}
{{ include "zephyr.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Selector labels, kept separate from zephyr.labels because selectors are immutable.
*/}}
{{- define "zephyr.selectorLabels" -}}
app.kubernetes.io/name: {{ include "zephyr.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "zephyr.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "zephyr.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
Container image reference: prefers an exact digest over a tag so every
replica and the migration Job run identical, reproducible code.
*/}}
{{- define "zephyr.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else if .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository .Values.image.tag -}}
{{- else -}}
{{- fail "Set image.digest (preferred) or image.tag" -}}
{{- end -}}
{{- end -}}
