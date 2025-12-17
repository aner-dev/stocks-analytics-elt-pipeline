# 🐛 [RESOLVED] Airflow-LocalStack Connectivity Failure (Podman Networking)

## Issue

The Airflow Scheduler container failed to establish a connection with the LocalStack service (`localstack:4566`), resulting in network errors when executing AWS commands (e.g., `awslocal s3 ls`).

**Observed Errors:**

## Root Cause Analysis

The core problem was a **persistent DNS Resolution Failure** within the internal Astro/Podman network. The Airflow container could not resolve the service name **`localstack`** to its corresponding IP address, rendering the default hostname useless.

### Technical Principle Involved

We bypassed the failing internal DNS by leveraging the **Host Alias** mechanism. This principle involves the container accessing the desired port via the host machine's dedicated internal alias, instead of relying on the faulty container-to-container name resolution.

## Resolution

The connection was successfully stabilized by forcing the Airflow container to connect to LocalStack via the host's internal network alias.

### 1. Configuration Change

The environment variable for the AWS endpoint was explicitly set within the `compose.yaml` to ensure persistence and higher priority than any inherited `.env` value.

**File:** `docker-compose.override.yml` (or `compose.yaml`)

**Injection:** Set the working alias under the `scheduler` service:

```yaml
services:
  scheduler:
    # ...
    environment:
      AWS_ENDPOINT_URL: "[http://host.containers.internal:4566](http://host.containers.internal:4566)"
```
