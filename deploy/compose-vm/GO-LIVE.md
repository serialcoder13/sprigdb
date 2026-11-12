# Go-live checklist: SprigDB's first run on real VMs

What to have ready before the first install on real VMs, in the order you need it. RUNBOOK.md has the procedures; this list is only what to prepare and decide. Everything here was rehearsed on one laptop (three Compose projects standing in for three VMs, hardening rounds 1 to 3); none of it has run on a real VM yet.

## 1. VMs

- [ ] **Three VMs** for the data center (one is enough to try it, with no high availability): Ubuntu 24.04 or Debian 12, Docker Engine 27+ with the compose plugin, `openssl`, `jq`, `rsync`.
- [ ] **Size** (each of the three): 4 vCPU, 16 GB, for MariaDB 8 GB (5 GB buffer pool), gateway 3 GB, controller 1 GB, worker 1 GB, etcd 512 MB, agent 256 MB. Set the limits in `.env`.
- [ ] **Disks:** a data disk for Docker's volumes (MariaDB's datadir and binlogs: 7 days of binlog at your write rate, plus a full copy for a replica rebuild), and room in `backups/` for `SPRIG_BACKUP_KEEP` encrypted backups.
- [ ] **Clocks:** NTP (chrony or systemd-timesyncd) on every VM. Leases tolerate drift up to `failover.clock_drift_margin_ms`, not minutes.
- [ ] **A load balancer:** the cloud's TCP load balancer, or a fourth small VM (1 vCPU, 1 GB) for `compose.lb.yaml` (Envoy). Two of those behind a floating address (keepalived) if the balancer must not be a single point of failure.

## 2. Network

- [ ] **One private network** for the three VMs, with fixed private addresses (for example 10.0.0.11 to .13).
- [ ] **Open between the VMs only:** etcd 2379 and 2380, MariaDB 3306 (the backend), the controller's 7480 (Admin) and 7481 (internal), the gateway's peer port 7403, telemetry 9180 to 9183 (for the balancer's health checks and Prometheus).
- [ ] **Open to clients** (through the load balancer): the client ports you use from 3306, 3307, 8443, 27017, 6379, 7687, 9092 and 5671. Kafka clients also need each VM's Kafka port directly (Kafka connects to every broker it is told about).
- [ ] **Not open anywhere:** Prometheus 9090 and Envoy's admin 9901 (loopback; reach them over SSH).
- [ ] **Firewall rule test:** from a machine outside the private network, the internal ports must not answer.

## 3. Names (DNS)

- [ ] **The client name**, for example `db.example.com`, pointing at the load balancer. It goes into `SPRIG_PUBLIC_HOST` and into the gateways' certificate.
- [ ] **One Kafka name per VM**, for example `db1.example.com` to `db3.example.com`, each pointing at its VM (`SPRIG_KAFKA_HOST` on each VM). Kafka's Metadata names each gateway by it.
- [ ] Private names are optional: the bundle works with private IPs (`SPRIG_INTERNAL_SANS=IP:10.0.0.11,IP:10.0.0.12,IP:10.0.0.13`).

## 4. Certificates

- [ ] **Decide:** the bundle's own CA (`sprig-vm.sh init` makes it; every client then needs `ca.pem`), or a public certificate for the client name. With a public certificate, put its chain and key in `secrets/tls/gateway.pem` and `gateway-key.pem`; the internal certificates stay on the bundle's CA.
- [ ] **Where the CA key lives** after `init`: off the VMs (a password manager, a vault, an offline disk). It comes back only to rotate certificates or add a VM.
- [ ] **Rotation reminder:** the generated certificates last 397 days (`SPRIG_CERT_DAYS`); the `SprigCertExpiringSoon` alert warns 14 days ahead. `rotate-certs` needs no restart.

## 5. Secrets

- [ ] **Who runs the bundle:** one Unix user per VM that owns `/opt/sprigdb/secrets` (0700) and may use Docker.
- [ ] **A safe copy of `secrets/`** (without the two offline keys) somewhere other than the VMs and the backups. Without it a restored backup can't be opened.
- [ ] **The backups' private key** (`secrets/backup/backup-key.pem`): off the VMs with the CA key. Only a restore needs it.
- [ ] **The first operator:** a named person with a TOTP app, created right after install; then the service token for `sprig-vm.sh` (`secrets/sprig/ops-token`). The bootstrap token stops working at the first operator login.

## 6. Backups

- [ ] **A target off the VMs:** a mounted disk or share, or `host:path` over SSH (`SPRIG_BACKUP_COPY_TO`). An object-store bucket works through a mount (s3fs, rclone mount, or the cloud's file gateway).
- [ ] **A schedule:** `./sprig-vm.sh backup` hourly from cron or a systemd timer on the VM whose MariaDB is the primary (any VM's node can be backed up; the primary's is the newest).
- [ ] **Retention** on the target (the bundle prunes only its own copies on the VM).
- [ ] **A restore drill** before real data arrives: restore the newest backup onto a spare VM and check a tenant's tables.

## 7. Monitoring

- [ ] **Prometheus:** the bundle's (`docker compose --profile monitoring up -d prometheus` on one VM, `SPRIG_PROMETHEUS_CONFIG=prometheus.ha.yml`, `./sprig-vm.sh monitoring-config`), or yours scraping every VM's telemetry ports 9180 to 9183.
- [ ] **Alertmanager** and where alerts go (email, Slack, a pager): the bundle loads the rules but sends nothing.
- [ ] **Grafana** with `deploy/dashboards/*.json`.
- [ ] **Logs:** keep Docker's rotated container logs, or ship them (journald, Loki, a SIEM). Audit lines carry `"audit":true`.

## 8. The run itself

1. Build or copy the image (`build-image.sh`, `docker save` / `docker load`), the same tag on every VM.
2. `.env` on each VM from `.env.example`, "Several VMs" and "MariaDB high availability" filled in.
3. `init` on VM 1, copy `secrets/` (without the offline keys) to VM 2 and VM 3, then move the offline keys off.
4. `up-etcd` on all three, `up` on VM 1, VM 2, VM 3, `configure` on VM 1 again (registers the MariaDB cluster once both replicas replicate).
5. The load balancer: `lb-up` on its VM (or the cloud's balancer with the health check below).
6. First operator, then a test tenant, then `smoke/smoke.sh` through the balancer's name.
7. Rehearse once with no real clients: stop VM 1's MariaDB (`docker compose stop mariadb-1`) and watch a writer move to VM 2 or VM 3 (`./sprig-vm.sh status`); start it again (it rejoins as a replica).

Health check for a cloud load balancer: TCP to each client port, with an HTTP health check `GET /readyz` on the gateway's telemetry port (9180) expecting 200, every 1 to 2 s, 2 failures to mark it down. A stopping gateway turns `/readyz` off 10 s before it closes its listeners.
