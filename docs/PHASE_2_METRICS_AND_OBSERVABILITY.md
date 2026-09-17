# Phase 2: Live Metrics & System Monitoring (`GET /metrics`)

## 1. Executive Summary & The Problem We Solved

In any distributed queue, operators need visibility into:
1. **Queue Backlog / Depth:** Is the queue growing faster than workers can process?
2. **Execution Rates:** How many jobs succeeded vs failed vs retried?
3. **Worker Health:** How many active workers are actually connected and consuming?

In Abhinav's Go repository, a `/metrics` route was exposed on the worker:
```go
func metrics_handler(w http.ResponseWriter, r *http.Request) {
    ...
    metrics.Total_jobs_in_queue = total_jobs_in_queue
    metrics.Jobs_done = jobs_done
    metrics.Jobs_failed = jobs_failed
    ...
}
```

### The Critical Bug in the Go Version: Data Races
In `cmd/worker/main.go`, `jobs_done++` and `jobs_failed++` were plain global integers mutated by 3 concurrent goroutines without a mutex or `sync/atomic`:
```go
// IN THE GO REPO (DATA RACE BUG):
jobs_failed++  // Race condition: Read-Modify-Write is NOT atomic in Go!
jobs_done++    // Will silently lose counts under high concurrent throughput!
```
Furthermore, if you run multiple worker containers across multiple machines, in-memory local counters in Go cannot aggregate metrics across the cluster.

In Phase 2, we solved this with **Centralized Redis Atomic Counters** (`HINCRBY`) and exposed a real-time `GET /metrics` endpoint.

---

## 2. Architecture & Data Structures

```
  +-------------------------------------------------------------------+
  |                          REDIS STATE                              |
  |                                                                   |
  |  Hash: 'workqueue:stats'                                          |
  |  +--------------------+----------------------------------------+  |
  |  | Field              | Value (Incremented via HINCRBY)        |  |
  |  +--------------------+----------------------------------------+  |
  |  | completed          | 1420                                   |  |
  |  | failed             | 12                                     |  |
  |  | retried            | 34                                     |  |
  |  | dlq_count          | 3                                      |  |
  |  +--------------------+----------------------------------------+  |
  |                                                                   |
  |  Queue Depths (Queried via Pipeline):                             |
  |  - LLEN 'queue:default'  -> Ready backlog                         |
  |  - ZCARD 'queue:delayed' -> In backoff wait                       |
  |  - LLEN 'queue:dead'     -> Dead Letter Queue                     |
  |                                                                   |
  |  Active Workers:                                                  |
  |  - Set: 'workqueue:workers'                                       |
  |  - Key: 'worker:heartbeat:{worker_id}' with TTL=15s               |
  +---------------------------------+---------------------------------+
                                    |
                            Pipeline Query
                                    |
                                    v
                     +------------------------------+
                     |    GET /metrics Endpoint     |
                     +------------------------------+
```

---

## 3. Code Deep-Dive

### Atomic Counter Operations: [shared/redis_client.py](file:///c:/workque/shared/redis_client.py)
```python
async def incr_stat(client: Redis, field: str, amount: int = 1) -> int:
    """
    Atomically increments a statistic counter in Redis to prevent data races.
    """
    return await client.hincrby("workqueue:stats", field, amount)
```

### Worker Heartbeat Registration: [shared/redis_client.py](file:///c:/workque/shared/redis_client.py)
Every 5 seconds, each worker coroutine executes a heartbeat update:
```python
async def register_worker_heartbeat(client: Redis, worker_id: str, ttl: int = 15):
    pipe = client.pipeline()
    pipe.sadd("workqueue:workers", worker_id)
    pipe.set(f"worker:heartbeat:{worker_id}", str(time.time()), ex=ttl)
    await pipe.execute()
```
If a worker crashes, its heartbeat key automatically expires after 15 seconds. When `/metrics` is called, `get_active_workers_count()` cleans up stale workers from the set, reporting the true active worker count.

### The Unified `/metrics` Endpoint: [producer/main.py](file:///c:/workque/producer/main.py)
```python
@app.get("/metrics", response_model=QueueMetrics)
async def get_metrics():
    """
    Real-time operational metrics for queue depths, completed jobs, failures, and active workers.
    """
    metrics_dict = await get_system_metrics(redis_client, PRODUCER_START_TIME)
    return QueueMetrics(**metrics_dict)
```

Sample JSON Response:
```json
{
  "total_jobs_in_queue": 4,
  "jobs_delayed": 1,
  "jobs_in_dlq": 0,
  "jobs_completed": 128,
  "jobs_failed": 2,
  "jobs_retried": 5,
  "active_workers": 3,
  "uptime_seconds": 341.2
}
```

---

## 4. Senior Interview Questions & Answers

### Q1: "Why use Redis hashes and `HINCRBY` instead of an in-memory counter in Python?"
> **Answer:** "In a real distributed system, we scale horizontally by running multiple worker containers (e.g., 5 containers with 4 workers each = 20 workers). In-memory counters only track local container stats and are wiped on process restart. By using Redis hashes (`workqueue:stats`) with `HINCRBY`, updates are atomic, persistent, and automatically aggregated across the entire cluster without data races."

### Q2: "How did you prevent the performance cost of multiple Redis round-trips in `/metrics`?"
> **Answer:** "We use a **Redis Pipeline** (`client.pipeline()`). Instead of making separate network round-trips for `LLEN queue:default`, `ZCARD queue:delayed`, and `LLEN queue:dead`, we buffer all three commands and transmit them in a single TCP socket packet, reducing network latency by ~66%."

### Q3: "How does worker liveness / discovery work without a complex consensus protocol like Raft or ZooKeeper?"
> **Answer:** "We use the **Lease / Heartbeat Pattern** with Redis keys and TTLs. Workers periodically refresh `worker:heartbeat:{id}` with a 15-second expiration. If a worker machine dies or is killed by the OOM killer, the key expires, and the monitor detects that the lease lapsed, giving us accurate active worker counts without heavy coordination."
