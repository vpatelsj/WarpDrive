# WarpDrive Helm Chart

Deploy WarpDrive cross-cloud data fabric on Kubernetes GPU clusters.

## Prerequisites

- Kubernetes 1.24+
- Helm 3.x
- macFUSE or FUSE installed on nodes
- GPU nodes with `nvidia.com/gpu` label

## Installation

```bash
# Basic install
helm install warpdrive deploy/helm/warpdrive \
  --namespace warpdrive --create-namespace \
  --values my-values.yaml

# Verify
kubectl -n warpdrive get pods
kubectl -n warpdrive exec -it ds/warpdrive-agent -- ls /data/
```

## Configuration

See [values.yaml](values.yaml) for all configuration options.

### Required: Backend Configuration

Override `config.backends` in your values file:

```yaml
config:
  backends:
    - name: training-data
      type: azureblob
      mount_path: /training
      config:
        account: mystorageaccount
        container: training-data
      auth:
        method: managed_identity
```

## Uninstall

```bash
helm uninstall warpdrive -n warpdrive
```
