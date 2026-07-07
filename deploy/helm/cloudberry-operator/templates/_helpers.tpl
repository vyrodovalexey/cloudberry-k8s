{{/*
Expand the name of the chart.
*/}}
{{- define "cloudberry-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "cloudberry-operator.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "cloudberry-operator.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "cloudberry-operator.labels" -}}
helm.sh/chart: {{ include "cloudberry-operator.chart" . }}
{{ include "cloudberry-operator.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: cloudberry-operator
{{- end }}

{{/*
Selector labels
*/}}
{{- define "cloudberry-operator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "cloudberry-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "cloudberry-operator.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "cloudberry-operator.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Operator image with tag
*/}}
{{- define "cloudberry-operator.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) }}
{{- end }}

{{/*
Webhook service name
*/}}
{{- define "cloudberry-operator.webhookServiceName" -}}
{{- if .Values.webhook.serviceName -}}
{{- .Values.webhook.serviceName -}}
{{- else -}}
{{- printf "%s-webhook" (include "cloudberry-operator.fullname" .) -}}
{{- end -}}
{{- end }}

{{/*
Webhook certificate secret name
*/}}
{{- define "cloudberry-operator.webhookCertSecretName" -}}
{{- if .Values.webhook.certSecretName -}}
{{- .Values.webhook.certSecretName -}}
{{- else -}}
{{- printf "%s-webhook-certs" (include "cloudberry-operator.fullname" .) -}}
{{- end -}}
{{- end }}

{{/*
ConfigMap name
*/}}
{{- define "cloudberry-operator.configMapName" -}}
{{- printf "%s-config" (include "cloudberry-operator.fullname" .) }}
{{- end }}

{{/*
Secret name for OIDC
*/}}
{{- define "cloudberry-operator.oidcSecretName" -}}
{{- if .Values.oidc.existingSecret }}
{{- .Values.oidc.existingSecret }}
{{- else }}
{{- printf "%s-oidc" (include "cloudberry-operator.fullname" .) }}
{{- end }}
{{- end }}

{{/*
Custom SecurityContextConstraints name for the CloudberryCluster workload.
Defaults to "<fullname>-cluster" when openshift.scc.name is empty.
*/}}
{{- define "cloudberry-operator.clusterSCCName" -}}
{{- if .Values.openshift.scc.name -}}
{{- .Values.openshift.scc.name -}}
{{- else -}}
{{- printf "%s-cluster" (include "cloudberry-operator.fullname" .) -}}
{{- end -}}
{{- end }}

{{/*
ServiceAccount name that the CloudberryCluster workload pods run under. The
operator does not set an explicit ServiceAccountName on the DB pod template, so
the pods use the namespace `default` ServiceAccount unless overridden.
*/}}
{{- define "cloudberry-operator.clusterServiceAccountName" -}}
{{- default "default" .Values.openshift.clusterServiceAccount -}}
{{- end }}

{{/*
Pod-level securityContext for the operator Deployment.
On OpenShift (openshift.enabled=true) the fixed runAsUser/runAsGroup/fsGroup are
dropped so the platform SCC injects the namespace UID range; runAsNonRoot and the
seccomp profile are always preserved. On vanilla Kubernetes the value is rendered
verbatim from .Values.podSecurityContext (byte-identical to prior behavior).
*/}}
{{- define "cloudberry-operator.podSecurityContext" -}}
{{- if .Values.openshift.enabled -}}
{{- $psc := omit .Values.podSecurityContext "runAsUser" "runAsGroup" "fsGroup" -}}
{{- toYaml $psc -}}
{{- else -}}
{{- toYaml .Values.podSecurityContext -}}
{{- end -}}
{{- end }}

{{/*
Container-level securityContext for the operator container.
On OpenShift the fixed runAsUser is dropped (namespace SCC injects it) while
allowPrivilegeEscalation:false, readOnlyRootFilesystem, runAsNonRoot and
capabilities drop:[ALL] are preserved. On vanilla Kubernetes the value is
rendered verbatim from .Values.securityContext.
*/}}
{{- define "cloudberry-operator.containerSecurityContext" -}}
{{- if .Values.openshift.enabled -}}
{{- $sc := omit .Values.securityContext "runAsUser" "runAsGroup" -}}
{{- toYaml $sc -}}
{{- else -}}
{{- toYaml .Values.securityContext -}}
{{- end -}}
{{- end }}
