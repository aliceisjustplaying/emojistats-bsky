# Hetzner Server Runbook (Debian 12, November 2025)

This document covers provisioning a fresh Debian 12 host with PostgreSQL 18 + TimescaleDB, Redis 8, Bun/Node/Deno, Prometheus 3.7, Grafana 11, and the exporters/dashboards needed for full observability.

---

## 1. Base System Prep

```bash
sudo apt update && sudo apt full-upgrade -y
sudo adduser --disabled-password --gecos "" deploy
sudo usermod -aG sudo deploy

# baseline tooling + firewall
sudo apt install -y curl wget ca-certificates gnupg lsb-release unzip ufw
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw allow OpenSSH
sudo ufw enable
```

Edit `/etc/ssh/sshd_config` to disable password + root logins, add your public key to `/home/deploy/.ssh/authorized_keys`, then reboot once.

---

## 2. PostgreSQL 18 + TimescaleDB 2.x

1. **Install from PGDG** ([postgresql.org](https://www.postgresql.org/download/linux/debian/?utm_source=openai))

```bash
sudo apt install -y postgresql-common
sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh
sudo apt install -y postgresql-18 postgresql-client-18 postgresql-doc-18
sudo systemctl enable --now postgresql@18-main
```

2. **Add TimescaleDB repo + package** ([packagecloud.io](https://packagecloud.io/timescale/timescaledb/packages/debian/bookworm/timescaledb-2-2.13.0-postgresql-13_2.13.0~debian12_arm64.deb?distro_version_id=215&utm_source=openai))

```bash
curl -s https://packagecloud.io/install/repositories/timescale/timescaledb/script.deb.sh | sudo bash
sudo apt install -y timescaledb-2-postgresql-18
sudo timescaledb-tune --quiet --yes
sudo systemctl restart postgresql@18-main
```

3. **Enable extensions per database**

```sql
CREATE DATABASE appdb;
\c appdb
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS timescaledb_toolkit;
```

4. **Retention & compression (optional)**

```sql
SELECT add_retention_policy('metrics', INTERVAL '90 days');
SELECT add_compression_policy('metrics', INTERVAL '14 days');
```

---

## 3. Redis 8 (source build with TLS)

Following [redis.io](https://redis.io/docs/latest/operate/oss_and_stack/install/build-stack/debian-bookworm/?utm_source=openai):

```bash
sudo apt install -y build-essential tcl pkg-config openssl libssl-dev cmake git
cd /usr/src
sudo wget -O redis-8.0.0.tar.gz https://github.com/redis/redis/archive/refs/tags/8.0.0.tar.gz
sudo tar xzf redis-8.0.0.tar.gz && cd redis-8.0.0
sudo BUILD_TLS=yes make -j"$(nproc)" install
```

Create user/config:

```bash
sudo useradd -r -s /bin/false redis
sudo mkdir -p /etc/redis /var/lib/redis
sudo cp redis.conf /etc/redis/redis.conf
sudo chown -R redis:redis /etc/redis /var/lib/redis
```

Adjust `/etc/redis/redis.conf`:

```
supervised systemd
dir /var/lib/redis
appendonly yes
protected-mode no   # only if firewalled/private
```

Systemd unit `/etc/systemd/system/redis.service` from upstream sample, then:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now redis
```

---

## 4. JavaScript Runtimes

| Runtime                 | Install                                    | Notes                                                                                                                                                               |
| ----------------------- | ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| Node.js (multi-version) | `curl -fsSL https://fnm.vercel.app/install | bash -s -- --install-dir "$HOME/.fnm" --skip-shell` → add `eval "$(fnm env --use-on-cd --shell bash)"`to your shell rc, then`fnm install --lts && fnm default lts`. | `fnm` is a fast Rust-based manager compatible with `.nvmrc`. ([github.com](https://github.com/Schniz/fnm?utm_source=openai)) |
| Bun                     | `curl -fsSL https://bun.sh/install         | bash`                                                                                                                                                               | Adds `~/.bun/bin` to PATH for runtime/bundler/test tooling. ([bun.sh](https://bun.sh/docs/installation?utm_source=openai))   |
| Deno 2                  | `curl -fsSL https://deno.land/install.sh   | sh`                                                                                                                                                                 | Deno 2 includes npm compatibility + `deno add/remove`. ([deno.com](https://deno.com/?utm_source=openai))                     |

---

## 5. Observability Stack

### 5.1 Prometheus 3.7 ([prometheus.io](https://prometheus.io/download/?utm_source=openai))

```bash
cd /tmp
wget https://github.com/prometheus/prometheus/releases/download/v3.7.3/prometheus-3.7.3.linux-amd64.tar.gz
tar xzf prometheus-3.7.3.linux-amd64.tar.gz
sudo mv prometheus-3.7.3.linux-amd64/{prometheus,promtool} /usr/local/bin/
sudo mv prometheus-3.7.3.linux-amd64/{consoles,console_libraries} /etc/prometheus/
sudo useradd --no-create-home --shell /usr/sbin/nologin prometheus
sudo mkdir -p /etc/prometheus /var/lib/prometheus
sudo chown -R prometheus:prometheus /etc/prometheus /var/lib/prometheus
```

Create `/etc/systemd/system/prometheus.service` (prometheus user, `--config.file=/etc/prometheus/prometheus.yml --storage.tsdb.path=/var/lib/prometheus --web.enable-lifecycle`). Then `sudo systemctl daemon-reload && sudo systemctl enable --now prometheus` per [skynats.com](https://www.skynats.com/blog/how-to-install-and-configure-prometheus-on-ubuntu-for-system-monitoring/?utm_source=openai).

Example scrape config:

```yaml
global:
  scrape_interval: 15s
scrape_configs:
  - job_name: "postgresql"
    static_configs:
      - targets: ["127.0.0.1:9187"]
  - job_name: "redis"
    static_configs:
      - targets: ["127.0.0.1:9121"]
```

### 5.2 Exporters

| Service    | Install                                                                                                                                                                                                                                      | Reference                                                                                                                        |
| ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| PostgreSQL | Download latest `postgres_exporter` from `prometheus-community/postgres_exporter`, place binary in `/usr/local/bin`, run with `DATA_SOURCE_NAME="user=postgres host=/var/run/postgresql/ sslmode=disable"`. Systemd unit runs as `postgres`. | [github.com/prometheus-community/postgres_exporter](https://github.com/prometheus-community/postgres_exporter?utm_source=openai) |
| Redis      | Grab latest release from `oliver006/redis_exporter`, place binary in `/usr/local/bin`, run `redis_exporter --redis.addr redis://127.0.0.1:6379`.                                                                                             | [github.com/oliver006/redis_exporter](https://github.com/oliver006/redis_exporter?utm_source=openai)                             |

### 5.3 Grafana OSS 11 ([docs.scs.community](https://docs.scs.community/docs/operating-scs/guides/openstack-health-monitor/Debian12-Install/?utm_source=openai))

```bash
sudo mkdir -p /etc/apt/keyrings
wget -q -O - https://apt.grafana.com/gpg.key | sudo gpg --dearmor -o /etc/apt/keyrings/grafana.gpg
echo "deb [signed-by=/etc/apt/keyrings/grafana.gpg] https://apt.grafana.com stable main" | sudo tee /etc/apt/sources.list.d/grafana.list
sudo apt update && sudo apt install -y grafana
tsudo systemctl enable --now grafana-server
```

Add Prometheus (`http://localhost:9090`) as a data source.

### 5.4 Dashboards to import

| Purpose                  | Grafana ID | Source                                                                                                                                                                               |
| ------------------------ | ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| PostgreSQL overview      | **14114**  | [grafana.com/grafana/dashboards/14114](https://grafana.com/grafana/dashboards/14114-postgres-overview?utm_source=openai)                                                             |
| PostgreSQL classic board | **9628**   | [Packt “Monitoring Postgres”](https://subscription.packtpub.com/book/data/9781838648138/9/ch09lvl1sec99/how-to-import-a-dashboard-for-monitoring-postgres-metrics?utm_source=openai) |
| Redis exporter board     | **14091**  | [grafana.com/grafana/dashboards/14091](https://grafana.com/grafana/dashboards/14091-redis-dashboard-for-prometheus-redis-exporter-1-x/?utm_source=openai)                            |

Import via Grafana → Dashboards → Import → enter ID → select Prometheus data source.

---

## 6. Prometheus Metrics Cheat Sheet

### PostgreSQL

- `postgres_exporter` exposes `pg_*` metrics (TPS, cache hit ratio, deadlocks, replication lag). Grafana Cloud docs show alert templates + recording rules. ([grafana.com](https://grafana.com/docs/grafana-cloud/monitor-applications/asserts/enable-prom-metrics-collection/data-stores/postgresql/?utm_source=openai))
- Example alerts: low `pg_stat_database_xact_commit`, high `pg_stat_activity`, replication delay > X.

### Redis

- `redis_exporter` exposes `redis_commands_processed_total`, `redis_memory_used_bytes`, `redis_connected_clients`, etc. Redis Cloud docs explain mapping those to alerting rules. ([redis.io](https://redis.io/docs/latest/integrate/prometheus-with-redis-cloud/?utm_source=openai))
- Typical alerts: eviction rate spikes (`rate(redis_evicted_keys_total[5m])`), `used_memory / maxmemory > 0.8`, connection spikes.

---

## 7. Optional Extras

- **node_exporter** for host metrics (Grafana “Node Exporter Full” board).
- **Grafana Alloy (Agent)** if you later move to OTEL pipelines. ([grafana.com](https://grafana.com/docs/agent/latest/flow/get-started/install/linux/?utm_source=openai))
- **Backups**: pgBackRest or barman for Timescale, nightly rsync of Parquet outputs.

---

## 8. Validation Checklist

1. `systemctl status postgresql@18-main redis prometheus grafana-server` → all `active (running)`.
2. `psql -X -c "SELECT version(), extname FROM pg_extension"` confirms Timescale.
3. `curl -s localhost:9187/metrics | head` shows Postgres exporter data.
4. `curl -s localhost:9121/metrics | head` shows Redis exporter data.
5. Prometheus `/targets` lists exporter endpoints `UP`.
6. Grafana dashboards render live metrics.

With this runbook you can rehydrate the full stack (database, cache, JS runtimes, and observability) on any Debian 12 Hetzner box in under an hour.

---

## 9. App Deployment Notes

- **Backfill service (`packages/backfill`)**

  - Environment: set `EMOJISTATS_DATABASE_URL`, `BSKY_DID_PLC_URL`, `EMOJI_BACKFILL_PARQUET_DIR`, `BACKFILL_METRICS_PORT`, etc.
  - `EMOJI_BACKFILL_CONCURRENCY` defaults to **64**; combined with built-in token buckets it obeys 20 `getRepo` requests/sec per PDS and 3,000 other atproto requests per five minutes. Tune this env var only if CPU/memory pressure requires it.
  - Run via systemd/pm2: `ExecStart=/home/deploy/.bun/bin/bun run backfill` in `/srv/emojistats/packages/backfill`.

- **Live ingest (`packages/live-ingest`)**

  - Seed Redis once with `bun run seed-redis` (after backfill > aggregates refreshed).
  - Start the Jetstream worker with `bun run start`. It uses the same rate-limit buckets so it won’t exceed Bluesky’s quotas.
  - Expose metrics at `LIVE_METRICS_PORT` (default 9480) for Prometheus.

- **Monitoring**
  - Scrape `BACKFILL_METRICS_PORT`, `LIVE_METRICS_PORT`, `postgres_exporter` (9187), `redis_exporter` (9121).
  - Alert on queue depth, rate-limit saturation, and exporter downtime.

Tying it all together: clone the repo under `/srv/emojistats`, copy `.env.example` files, run `bun install`, then enable both systemd services. Prometheus + Grafana (configured above) will give you instant visibility into throughput, rate limits, and resource usage.
