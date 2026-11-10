{{/* Names and labels shared by every template. */}}
{{- define "sprigdb.name" -}}sprigdb{{- end -}}
{{- define "sprigdb.labels" -}}
app.kubernetes.io/name: sprigdb
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
sprigdb.io/dc: {{ .Values.dc.id }}
{{- end -}}
{{- define "sprigdb.image" -}}{{ .Values.image.repository }}:{{ .Values.image.tag }}{{- end -}}
{{/* The etcd DC tier's client endpoints. */}}
{{- define "sprigdb.dcSeeds" -}}
{{- $n := int .Values.etcd.replicas -}}
{{- $scheme := ternary "https" "http" .Values.tls.enabled -}}
[{{- range $i, $_ := until $n -}}{{ if $i }}, {{ end }}"{{ $scheme }}://sprigdb-etcd-dc-{{ $i }}.sprigdb-etcd-dc.{{ $.Release.Namespace }}.svc:2379"{{- end -}}]
{{- end -}}
{{/* Pod security every SprigDB role shares. */}}
{{- define "sprigdb.podSecurity" -}}
securityContext:
  runAsNonRoot: true
  runAsUser: 65532
  fsGroup: 65532
  seccompProfile: {type: RuntimeDefault}
{{- end -}}
{{- define "sprigdb.tlsVolumes" -}}
{{- if .root.Values.tls.enabled }}
- name: tls
  secret: {secretName: {{ .secret }}}
- name: ca-bundle
  configMap: {name: {{ .root.Values.tls.bundleConfigMap }}}
{{- end }}
- name: config
  configMap: {name: sprigdb-{{ .role }}}
{{- end -}}
{{- define "sprigdb.tlsMounts" -}}
{{- if .Values.tls.enabled }}
- {name: tls, mountPath: /etc/sprigdb/tls, readOnly: true}
- {name: ca-bundle, mountPath: /etc/sprigdb/ca, readOnly: true}
{{- end }}
- {name: config, mountPath: /etc/sprigdb/config, readOnly: true}
{{- end -}}
