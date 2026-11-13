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
{{/* The Admin API's scheme: https with TLS, http only when allowed. */}}
{{- define "sprigdb.adminScheme" -}}
{{- if .Values.tls.enabled -}}
https
{{- else if (.Values.controller.adminApi | default dict).allowPlain -}}
http
{{- else -}}
{{- fail "the Admin API would be plain HTTP without tls.enabled: set controller.adminApi.allowPlain=true to allow it (development only)" -}}
{{- end -}}
{{- end -}}
{{/* The Admin API inside the cluster: the controller Service. */}}
{{- define "sprigdb.adminEndpoint" -}}
{{ include "sprigdb.adminScheme" . }}://sprigdb-controller.{{ .Release.Namespace }}.svc:7480
{{- end -}}
{{/* The gateway's optional client listeners, in Service order. */}}
{{- define "sprigdb.optionalListeners" -}}
mongo,redis,bolt,kafka,amqp,amqps
{{- end -}}
{{/* Every client listener the gateway runs, in its order. */}}
{{- define "sprigdb.allListeners" -}}
mysql,mariadb,http,https,mongo,redis,bolt,kafka,amqp,amqps
{{- end -}}
{{/* Listeners whose TLS is off by default (plain HTTP and plain AMQP):
     gateway.clientTls.clientCert.default skips them; name one under
     clientCert.listeners to set it anyway. */}}
{{- define "sprigdb.plainListeners" -}}
http,amqp
{{- end -}}
{{/* listener.<name>.client_cert as the settings job sets it, for
     (dict "root" $ "name" <listener>): clientCert.listeners.<name>, else
     clientCert.default for a TLS listener, else off. YAML reads a bare off
     as false; either means off. */}}
{{- define "sprigdb.clientCert" -}}
{{- $cc := (.root.Values.gateway.clientTls | default dict).clientCert | default dict -}}
{{- $per := $cc.listeners | default dict -}}
{{- $v := "off" -}}
{{- if hasKey $per .name -}}
{{- $v = get $per .name -}}
{{- else if not (has .name (splitList "," (include "sprigdb.plainListeners" .root))) -}}
{{- $v = $cc.default | default "off" -}}
{{- end -}}
{{- if or (kindIs "bool" $v) (eq (toString $v) "") -}}
off
{{- else -}}
{{ $v }}
{{- end -}}
{{- end -}}
{{/* Whether any listener asks for client certificates: "true" or "". */}}
{{- define "sprigdb.anyClientCert" -}}
{{- range $name := splitList "," (include "sprigdb.allListeners" .) -}}
{{- if ne (include "sprigdb.clientCert" (dict "root" $ "name" $name)) "off" -}}
true
{{- end -}}
{{- end -}}
{{- end -}}
{{/* The client CA file the gateways read (gateway.clientTls.clientCa), or
     "" when neither a secret nor a ConfigMap is named. */}}
{{- define "sprigdb.clientCaPath" -}}
{{- $ca := (.Values.gateway.clientTls | default dict).clientCa | default dict -}}
{{- if or $ca.secretName $ca.configMap -}}
{{ $ca.mountPath | default "/etc/sprigdb/client-ca" }}/{{ $ca.key | default "ca.crt" }}
{{- end -}}
{{- end -}}
