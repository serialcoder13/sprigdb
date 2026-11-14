---
title: Deploy on Kubernetes
audience: [operator]
sidebar:
  order: 2
---

# Deploy on Kubernetes

The Helm chart in `charts/sprigdb/` installs one SprigDB data centre into one Kubernetes cluster: etcd, the controllers, gateways, workers and the SprigDB operator. The operator creates MariaDB clusters for you through [mariadb-operator](https://github.com/mariadb-operator/mariadb-operator), registers them with the controller, and fails them over. Install the chart once per data centre, each in its own Kubernetes cluster.

## Before you install

You need:

| Requirement | Why |
|---|---|
| Kubernetes 1.27 or later | the chart's `kubeVersion` |
| Helm 3 | |
| mariadb-operator (`k8s.mariadb.com/v1alpha1`) | runs the MariaDB servers the SprigDB operator asks for |
| cert-manager | issues every role's certificate (with `tls.enabled`, the default) |
| trust-manager | publishes the CA bundle every pod trusts, as a ConfigMap (with `tls.enabled`) |
| A cert-manager `ClusterIssuer` for this DC | named in `tls.issuerRef` |
| A `sprigdb` image in a registry the cluster can pull from | named in `image.repository` and `image.tag` |
| A storage class for etcd and MariaDB volumes | |

The chart installs none of the operators above. Build the image with `deploy/compose-vm/build-image.sh <tag>` or `docker build -f deploy/compose-vm/Dockerfile`, push it, and set `image.repository` and `image.tag` to it.

## What the chart installs

| Resource | Name | Notes |
|---|---|---|
| StatefulSet and headless Service | `sprigdb-etcd-dc` | The DC tier: `etcd.replicas` members spread across zones, mutual TLS, a volume of `etcd.storage` each |
| StatefulSet and Service | `sprigdb-etcd-global` | This DC's member of the global tier, when `global.voter.enabled` |
| CronJob | `sprigdb-etcd-defrag` | Defragments each DC-tier member in turn, on `etcd.defrag.schedule` |
| StatefulSet and Service | `sprigdb-controller` | Admin API on 7480, internal port 7400 (permits and peer pings), telemetry on 9180. Pods are `sprigdb-controller-0`, `-1`, ...; they start in parallel and elect a leader through etcd |
| Headless Service | `sprigdb-controller-pods` | One DNS name per controller pod, published before the pod is ready. Each controller registers `<pod>.sprigdb-controller-pods.<namespace>.svc:7480` as its Admin address, where gateways forward Admin calls to the leader. Also the controllers' telemetry port, 9180 |
| Deployment, HorizontalPodAutoscaler and Service | `sprigdb-gateway` | Client ports 3306, 3307, 27017, 6379, 8080 and 8443; peer port 7403 |
| Headless Service | `sprigdb-gateway-pods` | One DNS name per gateway pod, `<pod IP with dashes>.sprigdb-gateway-pods.<namespace>.svc`, published before the pod is ready. Each gateway registers that name, so Kafka clients, other gateways (cross-gateway `KILL`, drains, transaction forwarding, queue and pub/sub relays on 7403) and workers can reach it; the gateway certificate covers it |
| Service | `sprigdb-gateway-health` | The gateways' telemetry port, 9180 |
| Deployment | `sprigdb-worker` | Background jobs |
| Headless Service | `sprigdb-worker-telemetry` | The workers' telemetry port, 9180 |
| Deployment, ServiceAccount, Role and RoleBinding | `sprigdb-operator` | The only role with Kubernetes permissions, limited to the release's namespace; replicas elect a leader through a `Lease`. It may list `batch` Jobs, to read mariadb-operator's backup Jobs |
| Headless Service | `sprigdb-operator-telemetry` | The operators' telemetry port, 9180 |
| ConfigMaps | `sprigdb-controller`, `-gateway`, `-worker`, `-agent` | Each role's `sprigdb.toml` |
| Secret | `sprigdb-admin-token` | The Admin API's bearer token, generated once and kept across upgrades and uninstalls |
| Secret | `sprigdb-gateway-backends` | The secret every gateway derives tenant users' MariaDB passwords from; generated once and kept |
| cert-manager Certificates | `sprigdb-<role>-tls` | One per role (below) |
| Job | `sprigdb-settings-<revision>` | After every install and upgrade: registers the DC with its tags and applies `settings` |
| CustomResourceDefinitions | `sprigclusters.sprigdb.io`, `sprigclusterpools.sprigdb.io` | In `crds/` |
| SprigClusterPool | one per entry in `pools` | |
| PodDisruptionBudgets | etcd, controller, gateway, operator | `maxUnavailable: 1` each |
| NetworkPolicies | `sprigdb-etcd-dc`, `sprigdb-controller` | With `networkPolicy.enabled`. With `monitoring.serviceMonitor` too, the controllers' port 9180 is open to any source, so Prometheus can scrape it wherever it runs |
| ServiceMonitor | `sprigdb` | With `monitoring.serviceMonitor` |

Every SprigDB pod runs as a non-root user (65532) with a read-only root file system, no privilege escalation, all capabilities dropped and the `RuntimeDefault` seccomp profile. Only the operator mounts a service-account token. Readiness probes use `/readyz` and liveness probes `/healthz` on port 9180.

## Values

| Value | Type | Default | Meaning |
|---|---|---|---|
| `dc.id` | string | `fra1` | The data centre's id: lowercase letters, digits and hyphens, starting with a letter, at most 32 characters |
| `dc.tags` | map | `{jurisdiction: EU, country: DE}` | Tags the settings Job registers the DC with |
| `deploymentId` | string | an example UUID | The deployment's id; the same in every DC |
| `global.endpoints` | list | two example URLs | The global tier's client URLs (`[meta] global_seeds`) |
| `global.voter.enabled` | bool | `true` | Run this DC's global-tier etcd member |
| `global.voter.advertise` | string | an example name | The name other DCs reach this DC's global member at |
| `etcd.replicas` | int | `3` | DC-tier members: 1, 3 or 5 |
| `etcd.storage` | string | `20Gi` | Volume per member |
| `etcd.image` | string | `quay.io/coreos/etcd:v3.6.5` | |
| `etcd.autoCompaction` | object | `{mode: periodic, retention: 1h}` | Keeps one hour of revisions; without it etcd's database grows with every write |
| `etcd.defrag.enabled` | bool | `true` | The defrag CronJob |
| `etcd.defrag.schedule` | string | `17 3 * * *` | Cron schedule |
| `etcd.defrag.clientSecret` | string | `""` | A TLS Secret whose certificate's CN is `root`, needed when etcd authentication is on; empty uses the DC tier's own certificate |
| `controller.replicas` | int | `3` | One leads the DC; the others stand by and serve permits |
| `controller.globalCandidate` | bool | `true` | Whether these controllers may lead the global tier |
| `controller.adminApi.tlsSecret` | string | `""` | A `kubernetes.io/tls` Secret for the Admin API; its names must include `sprigdb-controller.<namespace>.svc` and `*.sprigdb-controller-pods.<namespace>.svc`. Empty: the controller's own certificate, which covers both |
| `controller.adminApi.allowPlain` | bool | `false` | Allow a plain-HTTP Admin API when `tls.enabled` is `false`. Development only; without it such an install is refused |
| `controller.bootstrap` | bool | `false` | Set `true` in the deployment's first DC only: its controllers write the deployment id, its DC record and the tenant id counter once. Without it, controllers wait for another DC to bootstrap |
| `gateway.autoscaling.minReplicas` | int | `3` | |
| `gateway.autoscaling.maxReplicas` | int | `30` | |
| `gateway.autoscaling.targetCPUUtilizationPercentage` | int | `60` | The autoscaler's CPU target |
| `gateway.service.type` | string | `LoadBalancer` | `LoadBalancer`, `NodePort` or `ClusterIP` |
| `gateway.listeners.<name>.enabled` | bool | `false` | Turns on an optional client listener: `mongo`, `redis`, `bolt`, `kafka`, `amqp` (without TLS) or `amqps` (over TLS). See [Client listeners and TLS](#client-listeners-and-tls) |
| `gateway.listeners.<name>.port` | int | 27017, 6379, 7687, 9092, 5672, 5671 | The listener's port, in the same order |
| `gateway.clientTls.secretName` | string | `""` | A `kubernetes.io/tls` Secret with the certificate clients see. Empty: nothing is mounted |
| `gateway.clientTls.mountPath` | string | `/etc/sprigdb/client-tls` | Where that Secret is mounted in the gateways |
| `gateway.clientTls.clientCert.default` | string | `"off"` | Client certificates on every TLS listener (`mysql`, `mariadb`, `https`, `mongo`, `redis`, `bolt`, `kafka`, `amqps`): `"off"`, `optional` or `required`. Quote `"off"`; a bare `off` is read as false, which the chart also treats as off. See [Client certificates](#client-certificates) |
| `gateway.clientTls.clientCert.listeners.<name>` | string | none | One listener's mode, overriding `default`. The only way to set `http` and `amqp`, whose TLS is off by default |
| `gateway.clientTls.clientCa.secretName`, `.configMap` | string | `""` | A Secret, or a ConfigMap (such as a trust-manager bundle), holding the PEM CAs that sign client certificates. Required when any listener isn't off. Without it, the chart leaves the client-certificate settings alone |
| `gateway.clientTls.clientCa.key` | string | `ca.crt` | The file in that Secret or ConfigMap |
| `gateway.clientTls.clientCa.mountPath` | string | `/etc/sprigdb/client-ca` | Where it is mounted in the gateways |
| `worker.replicas` | int | `2` | |
| `operator.replicas` | int | `2` | |
| `operator.agentSidecar` | bool | `true` | Adds a fence-only SprigDB agent to every MariaDB pod |
| `operator.readyTimeoutSeconds` | int | `1800` | How long a new MariaDB cluster may take to become Ready before its `SprigCluster` is `Failed` (at least 60) |
| `operator.controllerAdmin` | string | `""` | The controller's Admin API; empty means `https://sprigdb-controller.<namespace>.svc:7480` (`http://` with `controller.adminApi.allowPlain`) |
| `nodeServices.type` | string | `LoadBalancer` | One Service per MariaDB pod, so other DCs can reach every node |
| `nodeServices.nodePortHost` | string | `""` | With `NodePort`: the host every node port is reached at, for single-host clusters such as kind |
| `tls.enabled` | bool | `true` | `false` is for development only: no Certificates, etcd and the roles without TLS. It also needs `controller.adminApi.allowPlain: true` |
| `tls.issuerRef` | object | `{kind: ClusterIssuer, name: sprig-fra1-ca}` | The issuer of every certificate |
| `tls.bundleConfigMap` | string | `sprigdb-ca-bundle` | The trust-manager bundle with every DC's CA, mounted as `ca.crt` |
| `image.repository`, `image.tag` | string | placeholders | Your `sprigdb` image |
| `image.pullPolicy` | string | `IfNotPresent` | |
| `settings.global`, `settings.dc` | map | `{}` | Settings the Job sets at global scope and at this DC's scope, as `key: value` |
| `pools` | list | one pool, `default` | MariaDB cluster pools (below) |
| `monitoring.serviceMonitor` | bool | `false` | Create a Prometheus Operator `ServiceMonitor` |
| `networkPolicy.enabled` | bool | `true` | Only SprigDB pods may reach etcd, and the controller's Admin and telemetry ports |

`values.schema.json` checks the values on install: a wrong type, an etcd size other than 1, 3 or 5, or a pool without its required fields is refused before anything is created.

## What has been tested on a cluster

The chart's smoke test, `deploy/kind/smoke.sh`, installs it on a local kind cluster with TLS on and checks, from pods inside the cluster:

- `helm lint` and `helm template`;
- the controller StatefulSet, each controller's per-pod Admin address, and the Admin API over HTTPS with `--ca-file`;
- logins on the MySQL and MariaDB ports;
- the gateway Service publishing all ten listener ports, with Bolt, Kafka and AMQP clients connecting;
- required client certificates: a valid certificate is accepted, a missing or foreign one refused;
- a hand-set `listener.<name>.client_cert` value surviving `helm upgrade`;
- `/metrics` on all four telemetry Services;
- an upgrade from the earlier chart, whose controllers were a Deployment, with the data kept.

Run it with `deploy/kind/smoke.sh`; `SMOKE_PHASES` picks a subset of its phases (`lint`, `install`, `admin`, `clients`, `mtls`, `metrics`, `backup`, `upgrade`), and `SMOKE_KEEP=1` keeps the cluster afterwards.

Not covered by that test: cert-manager and mariadb-operator themselves (the test uses stand-ins, so no real backup runs), and MongoDB clients.

## Install

Write one values file per DC:

```yaml
# values-fra1.yaml
dc:
  id: fra1
  tags: {jurisdiction: EU, country: DE}
deploymentId: 6f1c0d52-3b1e-4c47-9a55-0d6f8f6b2a10
global:
  endpoints: [https://etcd-g.fra1.example.net:2379, https://etcd-g.ams1.example.net:2379]
  voter: {enabled: true, advertise: etcd-g.fra1.example.net}
controller:
  bootstrap: true            # first DC of the deployment only
image:
  repository: registry.example.net/sprigdb
  tag: "1.0.0"
tls:
  issuerRef: {kind: ClusterIssuer, name: sprig-fra1-ca}
  bundleConfigMap: sprigdb-ca-bundle
pools:
  - {name: default, topology: replication, nodes: 3, storage: 500Gi, minClusters: 2, maxClusters: 20}
settings:
  dc:
    listener.mongo.enabled: true
```

```sh
helm install sprigdb charts/sprigdb -n sprigdb --create-namespace -f values-fra1.yaml
kubectl -n sprigdb get pods
kubectl -n sprigdb get sprigclusterpools,sprigclusters
```

What happens next:

1. etcd starts; the controllers connect, the first DC's controllers bootstrap the deployment, and one controller becomes the DC leader.
2. The settings Job waits for the controller, registers the DC with `dc.tags`, and applies `settings.global`, the optional listeners, the client certificate, the client-certificate modes and CA (only when `gateway.clientTls.clientCa` is set), and then `settings.dc` (all recorded with reason `helm`). It runs again after every upgrade.
3. The operator creates `minClusters` SprigClusters in each pool and turns each into a mariadb-operator `MariaDB`.
4. When a MariaDB is Ready, the operator creates the system accounts `sprig_ctl` and `sprig_repl` over TLS, stores their passwords in the Secret `sprigdb-cluster-credentials` (mounted on controllers, workers and gateways at `/etc/sprigdb/creds`), and registers the cluster with the controller. The controller's probes start, and the SprigCluster becomes `Active`.

Then reach the Admin API with `sprigctl`:

```sh
kubectl -n sprigdb port-forward svc/sprigdb-controller 7480:7480 &
kubectl -n sprigdb get secret sprigdb-admin-token -o jsonpath='{.data.token}' | base64 -d > admin.token
kubectl -n sprigdb get configmap sprigdb-ca-bundle -o jsonpath='{.data.ca\.crt}' > ca.crt
sprigctl --endpoint https://sprigdb-controller.sprigdb.svc:7480 --ca-file ca.crt --token-file admin.token cluster list
```

With `tls.enabled` (the default), the Admin API is HTTPS, and every client in the chart checks it against the CA bundle. Its certificate names the in-cluster Service, so map `sprigdb-controller.sprigdb.svc` to `127.0.0.1` in `/etc/hosts` while you use the port forward, or set `controller.adminApi.tlsSecret` to a certificate that also names `localhost`. The API is protected by the bearer token and the network policy; reach it only through a port forward. Create named operators next, as in [Deploy with Docker Compose](deploy-compose.md#first-operator-and-tenants): the bootstrap token stops working once the first operator logs in.

### Further data centres

Install the chart in each other DC's Kubernetes cluster with its own `dc.id`, the same `deploymentId`, the same `global.endpoints`, and `controller.bootstrap: false`. Each DC's trust bundle must hold every DC's CA, so controllers and etcd members in different DCs trust each other. See [Data centres](data-centres.md).

## Client listeners and TLS

Each role gets a certificate from `tls.issuerRef`, with DNS names `sprigdb-<role>`, `sprigdb-<role>.<namespace>.svc` and `*.sprigdb-<role>.<namespace>.svc`, used for both server and client authentication:

| Certificate | Lifetime |
|---|---|
| `sprigdb-gateway-tls` | 24 hours, renewed 8 hours before expiry |
| `sprigdb-controller-tls`, `-worker-tls`, `-operator-tls` | 720 hours |
| `sprigdb-etcd-dc-tls`, `sprigdb-etcd-global-tls` | 2160 hours |

Gateways re-read their certificate files when cert-manager renews them, with no restart. MariaDB servers get server and client certificates from the same issuer through mariadb-operator, and require TLS.

The client listeners take their certificate from the `tls.bundles.default.*` settings. To serve clients a certificate for your public name, put it in a `kubernetes.io/tls` Secret (the chain with the leaf first in `tls.crt`, and `tls.key`) and name it in `gateway.clientTls.secretName`. The chart mounts it in every gateway, and the settings Job points `tls.bundles.default.cert_path` and `key_path` at it. Gateways re-read it when it changes, with no restart.

```yaml
gateway:
  clientTls: {secretName: sprigdb-public-tls}
```

Without `clientTls.secretName`, set those two settings yourself (in `settings.dc`) to files the gateways can read, for example the mounted gateway certificate at `/etc/sprigdb/tls/tls.crt` and `tls.key`, which names only the in-cluster Service names. Listener changes apply live: see [TLS](../security/tls.md).

### Client certificates

To let accounts require client certificates (`REQUIRE X509`, `REQUIRE SUBJECT`, `REQUIRE ISSUER`), or to use Studio's `studio.operator_mtls`, have the listeners ask for them. Put the CA that signs your clients' certificates in a Secret or ConfigMap and name it, then choose the mode:

```yaml
gateway:
  clientTls:
    secretName: sprigdb-public-tls
    clientCert:
      default: optional          # every TLS listener: verify a certificate when the client sends one
      listeners: {https: required}
    clientCa: {secretName: sprigdb-client-ca, key: ca.crt}
```

What the chart does with these values:

- It mounts the CA in every gateway at `clientCa.mountPath`, and the settings Job sets `tls.bundles.default.client_ca_path` to `<mountPath>/<key>` at this DC's scope.
- When `clientCa.secretName` or `clientCa.configMap` is set, the settings Job sets `listener.<name>.client_cert` on all ten listeners at this DC's scope, on every install and upgrade: the listener's entry under `clientCert.listeners`, else `clientCert.default` for the TLS listeners, else `off`. The chart then owns these values, and one set by hand with `sprigctl` at the DC scope is overwritten at the next upgrade; put lasting values in these chart values, or in `settings.dc`, which the Job applies afterwards.
- When no `clientCa` is set, the Job writes neither `tls.bundles.default.client_ca_path` nor any `listener.<name>.client_cert`, so values you set by hand with `sprigctl` survive upgrades.
- To turn client certificates off with the chart, set the modes to `"off"` and keep `clientCa` for that upgrade, so the Job writes the `off` values; remove `clientCa` afterwards if you like.
- `http` and `amqp` take a value only from `clientCert.listeners`, and their TLS must be turned on first (`listener.http.tls` or `listener.amqp.tls` in `settings.dc`); otherwise the gateway rejects the listener change.
- If any listener's mode isn't off and neither `clientCa.secretName` nor `clientCa.configMap` is set, the chart refuses to render: "gateway.clientTls.clientCert asks for client certificates: name the CA that signs them in gateway.clientTls.clientCa (secretName or configMap)".
- The gateways re-read the CA file within a few seconds of it changing, as they do the server certificate, with no restart. When the Secret or ConfigMap is updated (by trust-manager, for example), Kubernetes updates the mounted file and the gateways pick it up. Keep the old and the new CA in the bundle while clients move to certificates from the new one.

`helm install` and `helm upgrade` print which listeners ask for certificates. See [Client certificates](../security/tls.md#client-certificates) for the modes and what each protocol checks.

The gateway Service always publishes 3306, 3307, 8080 and 8443. The other listeners are off by default. Turn one on with its toggle; the settings Job then sets `listener.<name>.enabled` and `listener.<name>.bind` at this DC's scope, and the Service publishes the port:

```yaml
gateway:
  listeners:
    mongo: {enabled: true, port: 27017}
    kafka: {enabled: true, port: 9092}
    amqps: {enabled: true, port: 5671}
```

Kafka clients also connect to each gateway directly, at the address Kafka's metadata gives them: each gateway's registered address, unless `[gateway] kafka_advertise_host` is set. Clients outside the Kubernetes cluster need those addresses to be routable.

## MariaDB clusters: SprigCluster and SprigClusterPool

The operator watches two custom resources in `sprigdb.io/v1alpha1`, polling every 5 seconds.

A **SprigCluster** (short name `sc`) is one MariaDB cluster:

| Field | Type | Default | Meaning |
|---|---|---|---|
| `spec.topology` | `galera`, `replication` or `single` | required | `replication` is a primary with replicas, semi-synchronous `AFTER_SYNC`, GTID strict mode and `sync_binlog = 1` |
| `spec.nodes` | int, 1 to 9 | `1` | MariaDB pods |
| `spec.mariadb.image` | string | required | Pools default to `mariadb:12.3` |
| `spec.storage.size` | quantity | required | Volume per node |
| `spec.storage.storageClassName` | string | | |
| `spec.resources` | object | | Kubernetes resource requests and limits for the MariaDB container |
| `spec.zones` | list | | Zones to spread the nodes across |
| `spec.failoverDriver` | `sprig` or `operator` | `sprig` | Who chooses the replica to promote (below) |
| `spec.backup.schedule` | string | | With `backup`: a mariadb-operator `PhysicalBackup` on this schedule |
| `spec.backup.target` | `s3://...` | | Where backups go |
| `spec.backup.endpoint`, `credentialsSecret`, `maxRetention` | string | | The object store's endpoint, its credentials Secret, and how long backups are kept |

Its status shows `phase`, `clusterId` (the SprigDB cluster id, such as `fra1-c3`), the nodes and their advertised addresses, the last failover, and nodes waiting for a rebuild:

| Phase | Meaning |
|---|---|
| `Pending` | Accepted, nothing created yet |
| `Provisioning` | The MariaDB isn't Ready, or a node Service has no address yet |
| `Bootstrapping` | Creating the system accounts |
| `Probing` | Registered; the controller is probing it |
| `Active` | In service |
| `Failed` | Not Ready within `operator.readyTimeoutSeconds`; a change to the spec starts a new attempt |

Every phase change is a Kubernetes Event on the SprigCluster, and a refused request (for example mariadb-operator's webhook rejecting the MariaDB) leaves its reason in `status.message` with a `ReconcileError` Event:

```sh
kubectl -n sprigdb get sc
kubectl -n sprigdb describe sc default-1
```

A **SprigClusterPool** (short name `scp`) holds a template and the bounds `minClusters` and `maxClusters`. The operator keeps at least `minClusters` SprigClusters in each pool, named after the pool, and never more than `maxClusters`. The chart makes one pool per entry in `pools`:

| Field | Type | Default | Meaning |
|---|---|---|---|
| `name` | string | required | Pool name |
| `topology` | `galera`, `replication` or `single` | required | |
| `nodes` | int, 1 to 9 | required | Nodes per cluster |
| `storage` | quantity | required | Volume per node |
| `storageClassName` | string | | |
| `minClusters` | int | `0` | Clusters created at install |
| `maxClusters` | int | required | Upper bound |
| `image` | string | `mariadb:12.3` | MariaDB image |
| `failoverDriver` | `sprig` or `operator` | `operator` | |

Each MariaDB starts with SprigDB's baseline server settings: a row-based binlog with full images and metadata kept for seven days, `sync_binlog = 1`, `innodb_flush_log_at_trx_commit = 1`, GTID strict mode, a buffer pool of 60% of the memory in `spec.resources`, a redo log of a quarter of the buffer pool (at least 1 GB), and `max_allowed_packet` 1 MB above the gateway's limit. These are set at creation only; later changes go through `SET GLOBAL`, so editing them never rolls the pods.

## Failover on Kubernetes

With `failoverDriver: sprig` (the default for a SprigCluster) mariadb-operator's automatic failover is off, and the SprigDB operator decides. Every second it checks each registered primary-with-replicas cluster. When the primary doesn't answer and its pod isn't Ready for `failover.cluster_detect_seconds` (5 seconds), it lets every replica apply what it has received, picks the replica whose received position contains every other's (so it holds every acknowledged commit), and asks mariadb-operator to switch over to it. If no replica qualifies, it stops and reports instead of losing data. It also repairs replicas a failover left stopped with GTID errors, or marks them for rebuild (`status.rebuild` and a `NodeNeedsRebuild` Event).

With `failoverDriver: operator`, mariadb-operator's own failover promotes a replica. That promotion doesn't guarantee zero loss under semi-synchronous replication, so use it only where you accept that.

With `operator.agentSidecar` on, each MariaDB pod runs a fence-only SprigDB agent that keeps a node `read_only` unless it holds the cluster's writer lease and its DC permit is valid. See [Failover](failover.md).

## Scaling

- **Gateways** scale with the HorizontalPodAutoscaler between `minReplicas` and `maxReplicas` on CPU. Gateways are stateless; any number can run.
- **Controllers, workers and the operator**: change `controller.replicas`, `worker.replicas` or `operator.replicas` and run `helm upgrade`. Only one controller leads the DC; more replicas make takeover faster, not work faster.
- **Nodes in a cluster**: change `spec.nodes` on the SprigCluster. The operator patches the MariaDB, adds or removes the per-pod Services, and updates the cluster record so the gateways follow.
- **More clusters**: raise a pool's `minClusters`, or create a SprigCluster yourself. Placement starts using a cluster once it is `Active`.
- **Fewer clusters**: drain the cluster so its tenants move off, retire its record, then delete the SprigCluster:

```sh
sprigctl cluster drain fra1-c3 --yes
sprigctl cluster show fra1-c3          # wait until no shards are left
sprigctl cluster retire fra1-c3 --yes
kubectl -n sprigdb delete sc default-2
```

Owner references delete the MariaDB, its backups and its Services. The volume claims and mariadb-operator's generated password Secrets stay; delete them when you no longer need the data. See [Clusters](clusters.md).

## Upgrades

```sh
helm upgrade sprigdb charts/sprigdb -n sprigdb -f values-fra1.yaml --set image.tag=1.0.1
```

- Deployments and StatefulSets roll one pod at a time within the PodDisruptionBudgets. A gateway being replaced turns `/readyz` off, keeps serving for `gateway.drain_lb_delay_seconds` (10 seconds) so the Service stops sending it connections, closes its listeners, and gives open sessions up to `gateway.drain_timeout_seconds` (30 seconds); its pod's grace period is 90 seconds.
- The settings Job runs again after each upgrade and re-applies `settings`.
- The admin token and the gateways' backend secret are kept (`helm.sh/resource-policy: keep`), so upgrades and reinstalls don't change them.
- Helm doesn't update CRDs in `crds/` on upgrade. When a release changes them, apply them first: `kubectl apply -f charts/sprigdb/crds/`.
- From a chart version whose controllers were a Deployment, `helm upgrade` replaces the `sprigdb-controller` Deployment with a StatefulSet of the same name; see [Upgrades](upgrades.md#kubernetes).

See [Upgrades](upgrades.md) for the order of roles and catalog migrations.

## Monitoring

Every pod serves `/metrics` on 9180. With `monitoring.serviceMonitor: true` the chart creates a ServiceMonitor for the Prometheus Operator, scraping every 15 seconds. It selects every Service of the release that has a port named `telemetry`, so it scrapes:

| Role | Service |
|---|---|
| Gateways | `sprigdb-gateway-health` |
| Controllers | `sprigdb-controller-pods` |
| Workers | `sprigdb-worker-telemetry` |
| Operators | `sprigdb-operator-telemetry` |

etcd isn't among them; scrape its metrics separately, since two alert rules read them.

The leading operator lists mariadb-operator's backup Jobs every minute and exports `sprig_backup_last_success_timestamp_seconds{cluster, kind="full"}`: the completion time of each cluster's newest successful backup, under its SprigDB cluster id. The `BackupMissed` alert reads it, so it fires for a SprigCluster with `spec.backup` whose backups stop. The value never moves back, and after an operator restart it starts from the Jobs still listed. A cluster without `spec.backup` has no series.

Load the alert rules in `deploy/alerts/` and the dashboards in `deploy/dashboards/`; see [Monitoring](monitoring.md).

## Not supported yet

- Growing a pool past `minClusters` on its own: raise `minClusters`, or create a SprigCluster.

## Related

- [Deploy with Docker Compose](deploy-compose.md)
- [Clusters](clusters.md)
- [Data centres](data-centres.md)
- [Failover](failover.md)
- [Configuration](configuration.md)
