# M5: Integration + Hardening — Technical Spec

## Timeline: Weeks 20-28 (overlaps M4 by 3 weeks)

## Goal

Production-ready deployment at MAI. K8s packaging. Monitoring. Documentation. Battle-tested under real training workloads.

---

## M5.1: Kubernetes Deployment (Weeks 20-22)

### Helm Chart: `deploy/helm/warpdrive/`

```
deploy/helm/warpdrive/
├── Chart.yaml
├── values.yaml
├── templates/
│   ├── _helpers.tpl
│   ├── daemonset.yaml          # Node agent on GPU nodes
│   ├── deployment.yaml         # Control plane
│   ├── service.yaml            # Control plane service
│   ├── configmap.yaml          # WarpDrive config
│   ├── secret.yaml             # Credentials references
│   ├── serviceaccount.yaml
│   ├── clusterrole.yaml        # For node agent pod info access
│   ├── clusterrolebinding.yaml
│   ├── pdb.yaml                # PodDisruptionBudget for control plane
│   └── NOTES.txt
└── README.md
```

### DaemonSet: `templates/daemonset.yaml`

```yaml
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: {{ include "warpdrive.fullname" . }}-agent
spec:
  selector:
    matchLabels:
      app: warpdrive-agent
  template:
    metadata:
      labels:
        app: warpdrive-agent
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9090"
    spec:
      nodeSelector:
        {{- toYaml .Values.agent.nodeSelector | nindent 8 }}
        # Default: nvidia.com/gpu: "true"
      tolerations:
        {{- toYaml .Values.agent.tolerations | nindent 8 }}
      serviceAccountName: {{ include "warpdrive.fullname" . }}-agent
      hostPID: false
      containers:
        - name: warpdrive-agent
          image: "{{ .Values.agent.image.repository }}:{{ .Values.agent.image.tag }}"
          securityContext:
            privileged: true    # Required for FUSE mount
            capabilities:
              add: ["SYS_ADMIN"]
          args:
            - "--config=/etc/warpdrive/config.yaml"
          ports:
            - name: metrics
              containerPort: 9090
          volumeMounts:
            - name: config
              mountPath: /etc/warpdrive
            - name: fuse-mount
              mountPath: {{ .Values.agent.mountPoint | default "/data" }}
              mountPropagation: Bidirectional  # CRITICAL: allows host to see FUSE mount
            - name: nvme-cache
              mountPath: {{ .Values.agent.cachePath | default "/nvme/warpdrive-cache" }}
            - name: fuse-device
              mountPath: /dev/fuse
          resources:
            {{- toYaml .Values.agent.resources | nindent 12 }}
          livenessProbe:
            httpGet:
              path: /healthz
              port: 9090
            initialDelaySeconds: 10
            periodSeconds: 30
          readinessProbe:
            exec:
              command:
                - mountpoint
                - -q
                - {{ .Values.agent.mountPoint | default "/data" }}
            initialDelaySeconds: 5
            periodSeconds: 10
      volumes:
        - name: config
          configMap:
            name: {{ include "warpdrive.fullname" . }}-config
        - name: fuse-mount
          hostPath:
            path: {{ .Values.agent.mountPoint | default "/data" }}
            type: DirectoryOrCreate
        - name: nvme-cache
          hostPath:
            path: {{ .Values.agent.cachePath | default "/nvme/warpdrive-cache" }}
            type: DirectoryOrCreate
        - name: fuse-device
          hostPath:
            path: /dev/fuse
```

### Control Plane Deployment: `templates/deployment.yaml`

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "warpdrive.fullname" . }}-control
spec:
  replicas: 1
  selector:
    matchLabels:
      app: warpdrive-control
  template:
    spec:
      containers:
        - name: control
          image: "{{ .Values.controlPlane.image.repository }}:{{ .Values.controlPlane.image.tag }}"
          args:
            - "--config=/etc/warpdrive/config.yaml"
          ports:
            - name: grpc
              containerPort: 50051
            - name: http
              containerPort: 8080
          env:
            - name: WARPDRIVE_DB_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: {{ include "warpdrive.fullname" . }}-secrets
                  key: db-password
          volumeMounts:
            - name: config
              mountPath: /etc/warpdrive
      volumes:
        - name: config
          configMap:
            name: {{ include "warpdrive.fullname" . }}-config
```

### values.yaml

```yaml
agent:
  image:
    repository: warpdrive/warpdrive-mount
    tag: v1.0.0
  mountPoint: /data
  cachePath: /nvme/warpdrive-cache
  nodeSelector:
    nvidia.com/gpu: "true"
  tolerations:
    - key: nvidia.com/gpu
      operator: Exists
      effect: NoSchedule
  resources:
    requests:
      cpu: "1"
      memory: 2Gi
    limits:
      cpu: "4"
      memory: 8Gi

controlPlane:
  image:
    repository: warpdrive/warpdrive-control
    tag: v1.0.0
  database:
    host: postgres.internal
    port: 5432
    name: warpdrive
    user: warpdrive

config:
  cache:
    maxSize: 2TB
    blockSize: 4MB
  backends: []
    # Override in your values file
```

### Install Command

```bash
# Basic install
helm install warpdrive deploy/helm/warpdrive \
  --namespace warpdrive --create-namespace \
  --values my-values.yaml

# Verify
kubectl -n warpdrive get pods
kubectl -n warpdrive exec -it ds/warpdrive-agent -- ls /data/
```

---

## M5.2: Slurm Integration (Weeks 21-24)

### systemd Service: `deploy/systemd/warpdrive-agent.service`

```ini
[Unit]
Description=WarpDrive Data Fabric Agent
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/local/bin/warpdrive-mount --config /etc/warpdrive/config.yaml
ExecStop=/bin/fusermount -u /data
Restart=on-failure
RestartSec=5
LimitNOFILE=1048576
Environment=GOMAXPROCS=8

# Security
CapabilityBoundingSet=CAP_SYS_ADMIN
AmbientCapabilities=CAP_SYS_ADMIN

[Install]
WantedBy=multi-user.target
```

### Slurm Prolog: `deploy/slurm/warpdrive-prolog.sh`

```bash
#!/bin/bash
# /etc/slurm/prolog.d/warpdrive-prolog.sh
# Runs before each Slurm job on the compute node

set -euo pipefail

WARMUP_PATH="${WARPDRIVE_WARMUP_PATH:-${SLURM_JOB_COMMENT:-}}"
WARMUP_MAX_SIZE="${WARPDRIVE_WARMUP_MAX_SIZE:-0}"
WARMUP_WORKERS="${WARPDRIVE_WARMUP_WORKERS:-32}"

# Check if mount is healthy
if ! mountpoint -q /data; then
    echo "KHADI ERROR: /data is not mounted. Restarting agent..."
    systemctl restart warpdrive-agent
    sleep 5
    if ! mountpoint -q /data; then
        echo "KHADI ERROR: /data still not mounted after restart"
        exit 1
    fi
fi

# Warm cache if path specified
if [ -n "$WARMUP_PATH" ]; then
    echo "KHADI: Warming cache for job ${SLURM_JOB_ID}: ${WARMUP_PATH}"
    timeout 1800 warpdrive-ctl warm "$WARMUP_PATH" \
        --recursive \
        --max-size "$WARMUP_MAX_SIZE" \
        --workers "$WARMUP_WORKERS" \
        2>&1 | logger -t warpdrive-prolog
    echo "KHADI: Cache warm complete for job ${SLURM_JOB_ID}"
fi
```

### Slurm Epilog: `deploy/slurm/warpdrive-epilog.sh`

```bash
#!/bin/bash
# /etc/slurm/epilog.d/warpdrive-epilog.sh
# Runs after each Slurm job completes

# Report job stats to control plane
warpdrive-ctl job-report \
    --job-id "$SLURM_JOB_ID" \
    --user "$SLURM_JOB_USER" \
    --status "$SLURM_JOB_EXIT_CODE" \
    2>&1 | logger -t warpdrive-epilog
```

### Ansible Playbook: `deploy/slurm/ansible/install-warpdrive.yaml`

```yaml
---
- name: Install WarpDrive on Slurm compute nodes
  hosts: compute_nodes
  become: yes
  tasks:
    - name: Copy warpdrive binaries
      copy:
        src: "{{ item }}"
        dest: /usr/local/bin/
        mode: '0755'
      loop:
        - binaries/warpdrive-mount
        - binaries/warpdrive-ctl

    - name: Create config directory
      file:
        path: /etc/warpdrive
        state: directory
        mode: '0755'

    - name: Deploy config
      template:
        src: config.yaml.j2
        dest: /etc/warpdrive/config.yaml
        mode: '0644'

    - name: Create cache directory
      file:
        path: /nvme/warpdrive-cache
        state: directory
        mode: '0755'

    - name: Install systemd service
      copy:
        src: warpdrive-agent.service
        dest: /etc/systemd/system/warpdrive-agent.service
      notify: restart warpdrive

    - name: Install Slurm prolog
      copy:
        src: warpdrive-prolog.sh
        dest: /etc/slurm/prolog.d/warpdrive-prolog.sh
        mode: '0755'

    - name: Install Slurm epilog
      copy:
        src: warpdrive-epilog.sh
        dest: /etc/slurm/epilog.d/warpdrive-epilog.sh
        mode: '0755'

    - name: Enable and start warpdrive agent
      systemd:
        name: warpdrive-agent
        enabled: yes
        state: started
        daemon_reload: yes

  handlers:
    - name: restart warpdrive
      systemd:
        name: warpdrive-agent
        state: restarted
        daemon_reload: yes
```

---

## M5.3: Monitoring + Alerting (Weeks 22-25)

### Prometheus Metrics: `pkg/metrics/metrics.go`

```go
package metrics

import (
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promauto"
)

var (
    // Cache metrics
    CacheHits = promauto.NewCounter(prometheus.CounterOpts{
        Name: "warpdrive_cache_hit_total",
        Help: "Total cache hits",
    })
    CacheMisses = promauto.NewCounter(prometheus.CounterOpts{
        Name: "warpdrive_cache_miss_total",
        Help: "Total cache misses",
    })
    CacheSize = promauto.NewGauge(prometheus.GaugeOpts{
        Name: "warpdrive_cache_size_bytes",
        Help: "Current cache size in bytes",
    })
    CacheEvictions = promauto.NewCounter(prometheus.CounterOpts{
        Name: "warpdrive_cache_evictions_total",
        Help: "Total cache evictions",
    })
    CacheUtilization = promauto.NewGauge(prometheus.GaugeOpts{
        Name: "warpdrive_cache_utilization_ratio",
        Help: "Cache utilization (0-1)",
    })
    
    // Backend metrics
    BackendRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
        Name:    "warpdrive_backend_request_duration_seconds",
        Help:    "Backend request duration",
        Buckets: []float64{.001, .005, .01, .05, .1, .5, 1, 5, 10, 30},
    }, []string{"backend", "operation"})
    
    BackendErrors = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "warpdrive_backend_errors_total",
        Help: "Backend errors by type",
    }, []string{"backend", "error_type"})
    
    BackendBytesRead = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "warpdrive_backend_bytes_read_total",
        Help: "Total bytes read from backends",
    }, []string{"backend"})
    
    // FUSE metrics
    FUSEOperations = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "warpdrive_fuse_operations_total",
        Help: "FUSE operations by type",
    }, []string{"operation"}) // read, readdir, lookup, getattr
    
    FUSELatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
        Name:    "warpdrive_fuse_operation_duration_seconds",
        Help:    "FUSE operation latency",
        Buckets: []float64{.0001, .0005, .001, .005, .01, .05, .1, .5, 1},
    }, []string{"operation"})
    
    // Readahead metrics
    ReadaheadHits = promauto.NewCounter(prometheus.CounterOpts{
        Name: "warpdrive_readahead_hit_total",
        Help: "Readahead-prefetched blocks that were subsequently read",
    })
    ReadaheadWasted = promauto.NewCounter(prometheus.CounterOpts{
        Name: "warpdrive_readahead_wasted_total",
        Help: "Readahead-prefetched blocks that were evicted before being read",
    })
    
    // Auth metrics
    AuthRefreshes = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "warpdrive_auth_refresh_total",
        Help: "Credential refreshes",
    }, []string{"provider", "status"}) // status: success, failure
)

// Health endpoint handler
func HealthzHandler(w http.ResponseWriter, r *http.Request) {
    // Check: FUSE mount is active, cache DB is accessible, backends reachable
    // Return 200 if healthy, 503 if degraded
}
```

### Grafana Dashboard: `deploy/monitoring/grafana-dashboard.json`

Panels:
1. **Cache Hit Rate** — `rate(warpdrive_cache_hit_total[5m]) / (rate(warpdrive_cache_hit_total[5m]) + rate(warpdrive_cache_miss_total[5m]))`
2. **Cache Size** — `warpdrive_cache_size_bytes` gauge
3. **Backend Latency (p99)** — `histogram_quantile(0.99, rate(warpdrive_backend_request_duration_seconds_bucket[5m]))`
4. **Backend Errors** — `rate(warpdrive_backend_errors_total[5m])` by backend
5. **FUSE Throughput** — `rate(warpdrive_backend_bytes_read_total[5m])` by backend
6. **FUSE Operation Latency** — `histogram_quantile(0.95, rate(warpdrive_fuse_operation_duration_seconds_bucket[5m]))`

### Alert Rules: `deploy/monitoring/alerts.yaml`

```yaml
groups:
  - name: warpdrive
    rules:
      - alert: WarpDriveCacheHitRateLow
        expr: |
          rate(warpdrive_cache_hit_total[10m]) / 
          (rate(warpdrive_cache_hit_total[10m]) + rate(warpdrive_cache_miss_total[10m])) < 0.5
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "WarpDrive cache hit rate below 50% on {{ $labels.instance }}"
          
      - alert: WarpDriveBackendErrorRate
        expr: rate(warpdrive_backend_errors_total[5m]) > 0.01
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Backend {{ $labels.backend }} error rate >1%"
          
      - alert: WarpDriveCacheFull
        expr: warpdrive_cache_utilization_ratio > 0.95
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "WarpDrive cache >95% full on {{ $labels.instance }}"
          
      - alert: WarpDriveMountDown
        expr: up{job="warpdrive-agent"} == 0
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "WarpDrive agent down on {{ $labels.instance }}"
```

---

## M5.4: MAI Design Partner Deployment (Weeks 22-28)

### Deployment Runbook

```markdown
## Week 22-23: Staging Deployment

### Prerequisites
- [ ] MAI staging K8s cluster access granted
- [ ] Cross-tenant app registration consented in MAI tenants
- [ ] Private Link endpoints configured for cross-tenant Blob access
- [ ] Vast S3 credentials stored in Key Vault
- [ ] PostgreSQL instance provisioned (Azure Database for PostgreSQL)
- [ ] NVMe local disk available on GPU nodes (/nvme or /mnt/resource_nvme)

### Steps
1. Create namespace: `kubectl create namespace warpdrive`
2. Create secret with DB password and any static credentials
3. Deploy with Helm:
   ```bash
   helm install warpdrive deploy/helm/warpdrive \
     --namespace warpdrive \
     --values deploy/mai/staging-values.yaml
   ```
4. Verify:
   - Agent pods running on GPU nodes: `kubectl -n warpdrive get pods -o wide`
   - Mount accessible: `kubectl -n warpdrive exec ds/warpdrive-agent -- ls /data/`
   - Control plane up: `kubectl -n warpdrive port-forward svc/warpdrive-control 8080:8080`
   - Dashboard accessible at http://localhost:8080

## Week 23-24: Researcher Pilot
- Onboard 2-3 researchers
- Provide quickstart guide
- Monitor via Grafana dashboard
- Daily check-in for feedback

## Week 25-26: Admin Onboarding
- Deploy governance dashboard to MAI admins
- Walk through: usage view, stale data, quotas
- Configure team mappings via Entra group sync

## Week 26-28: Production Expansion
- Scale to production cluster
- Add remaining backends (all tenants + Vast)
- Enable alerting
- Establish on-call runbook
```

---

## M5.5: Documentation (Weeks 24-28)

### Documents to Create

1. **Operator Guide** (`docs/operator-guide.md`): Installation, config reference, backend setup, auth setup, troubleshooting
2. **Researcher Quickstart** (`docs/quickstart.md`): 1-page, from "mount is available" to "training on remote data"
3. **Admin Guide** (`docs/admin-guide.md`): Dashboard usage, CLI reference, quota management, stale data workflows
4. **Architecture Doc** (`docs/architecture.md`): Component overview, data flow diagrams, security model, failure modes
5. **Runbook** (`docs/runbook.md`): Common issues, how to diagnose, how to recover

---

## COPILOT PROMPT — M5.1: Kubernetes Helm Chart

```
You are creating a Helm chart for WarpDrive, a cross-cloud data fabric that runs on GPU Kubernetes clusters.

Project directory: deploy/helm/warpdrive/

## What to build

A complete Helm chart that deploys:
1. DaemonSet (warpdrive-agent): runs on every GPU node, provides FUSE mount
2. Deployment (warpdrive-control): control plane service (gRPC + REST + dashboard)
3. Services, ConfigMap, ServiceAccount, RBAC

## Files to create

1. `Chart.yaml`: name=warpdrive, version=1.0.0, appVersion=1.0.0

2. `values.yaml` with defaults:
   - agent.image.repository/tag, mountPoint=/data, cachePath=/nvme/warpdrive-cache
   - agent.nodeSelector: nvidia.com/gpu=true
   - agent.resources: requests 1 CPU / 2Gi memory, limits 4 CPU / 8Gi
   - controlPlane.image.repository/tag
   - controlPlane.database: host, port, name, user
   - config.cache: maxSize=2TB, blockSize=4MB
   - config.backends: [] (override in user values)

3. `templates/daemonset.yaml`:
   - Privileged container (required for FUSE)
   - Mount /dev/fuse from host
   - Mount data directory with mountPropagation: Bidirectional (so other pods see the FUSE mount)
   - Mount NVMe cache path from host
   - Mount config from ConfigMap
   - Prometheus scrape annotations on port 9090
   - Liveness probe: HTTP /healthz
   - Readiness probe: exec `mountpoint -q /data`

4. `templates/deployment.yaml`:
   - Single replica control plane
   - Ports: 50051 (gRPC), 8080 (HTTP)
   - DB password from Secret env var
   - Config from ConfigMap

5. `templates/service.yaml`:
   - ClusterIP service for control plane: grpc (50051) and http (8080)

6. `templates/configmap.yaml`:
   - WarpDrive config.yaml generated from values

7. `templates/secret.yaml`:
   - DB password (base64 encoded from values)

8. `templates/serviceaccount.yaml`, `clusterrole.yaml`, `clusterrolebinding.yaml`:
   - Agent needs: pods/list (to get node info), nodes/get

9. `templates/_helpers.tpl`: standard name/fullname/labels helpers

10. `templates/NOTES.txt`: post-install instructions

## Critical Kubernetes details
- The DaemonSet MUST be privileged and have SYS_ADMIN capability for FUSE
- mountPropagation: Bidirectional on the data mount is REQUIRED — without it, other pods on the node cannot see the FUSE mount
- The hostPath for data mount must use DirectoryOrCreate type
- GPU node selector ensures agents only run where GPUs are present
- Add tolerations for GPU node taints (nvidia.com/gpu NoSchedule)
```

---

## COPILOT PROMPT — M5.3: Monitoring

```
You are adding Prometheus metrics and Grafana dashboards to WarpDrive.

Project: Go module at github.com/warpdrive/warpdrive
Package: pkg/metrics/

## What to build

### 1. Prometheus metrics (pkg/metrics/metrics.go):
Use github.com/prometheus/client_golang/prometheus/promauto

Counters:
- warpdrive_cache_hit_total, warpdrive_cache_miss_total, warpdrive_cache_evictions_total
- warpdrive_backend_errors_total (labels: backend, error_type)
- warpdrive_backend_bytes_read_total (labels: backend)
- warpdrive_fuse_operations_total (labels: operation)
- warpdrive_readahead_hit_total, warpdrive_readahead_wasted_total
- warpdrive_auth_refresh_total (labels: provider, status)

Gauges:
- warpdrive_cache_size_bytes, warpdrive_cache_utilization_ratio

Histograms:
- warpdrive_backend_request_duration_seconds (labels: backend, operation) buckets: .001,.005,.01,.05,.1,.5,1,5,10,30
- warpdrive_fuse_operation_duration_seconds (labels: operation) buckets: .0001,.0005,.001,.005,.01,.05,.1,.5,1

### 2. Health endpoint:
- GET /healthz on port 9090
- Checks: FUSE mount active (mountpoint -q), badger DB accessible, at least one backend reachable
- Returns 200 if all healthy, 503 with JSON details if degraded

### 3. Instrument existing code:
- In pkg/cache/cache.go: increment hit/miss/eviction counters, update size gauge
- In pkg/backend/rclone.go: observe request duration histogram, increment error counter
- In pkg/fuse/file.go: increment operation counter, observe latency histogram
- In pkg/auth/: increment refresh counter on credential resolution

### 4. HTTP server setup in cmd/warpdrive-mount/main.go:
- Start HTTP server on :9090 serving /metrics (prometheus handler) and /healthz
- Run alongside FUSE mount in separate goroutine

### 5. Grafana dashboard (deploy/monitoring/grafana-dashboard.json):
- JSON model for Grafana import
- 6 panels: Cache Hit Rate, Cache Size, Backend Latency p99, Backend Errors, FUSE Throughput, FUSE Op Latency
- Templated for instance/job selectors

### 6. Alert rules (deploy/monitoring/alerts.yaml):
- Prometheus AlertManager rules
- WarpDriveCacheHitRateLow: <50% for 10min → warning
- WarpDriveBackendErrorRate: >1% for 5min → warning
- WarpDriveCacheFull: >95% for 5min → warning
- WarpDriveMountDown: agent not responding for 2min → critical
```

---

## M5 Exit Criteria

- [ ] `helm install warpdrive` from zero to working in <30 minutes on a new cluster
- [ ] GPU pods on the same node can access /data/ via hostPath
- [ ] Slurm deployment via Ansible playbook works on bare-metal nodes
- [ ] Prolog script warms cache before Slurm jobs
- [ ] Grafana dashboard shows all 6 panels with live data
- [ ] All 4 alert rules configured and verified (fire + resolve)
- [ ] MAI researchers actively training on cross-tenant data via /data/
- [ ] MAI admins using governance dashboard
- [ ] Zero data loss or corruption during deployment
- [ ] Operator guide, quickstart, admin guide, architecture doc, runbook complete
