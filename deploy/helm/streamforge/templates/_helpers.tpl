{{- define "streamforge.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "streamforge.fullname" -}}
{{- if .Values.fullnameOverride }}{{ .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}{{ else }}{{ printf "%s-%s" .Release.Name (include "streamforge.name" .) | trunc 63 | trimSuffix "-" }}{{ end }}
{{- end }}

{{- define "streamforge.labels" -}}
app.kubernetes.io/name: {{ include "streamforge.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | quote }}
{{- end }}
