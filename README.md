# WorkQueue

A high-performance, distributed background job processing system built with **FastAPI**, **Redis**, and **asyncio**, featuring concurrent worker pools, CPU vs I/O dispatching, exponential backoff retries, and atomic system metrics.

Inspired by [AbhinavXJ/WorkQueue](https://github.com/AbhinavXJ/WorkQueue) (written in Go), this Python implementation is engineered with enterprise reliability patterns and full feature parity.

---

## Architecture Overview

```
                                  +------------------------------------+
                                  |            Client / HTTP           |
                                  +-----------------+------------------+
                                                    |
                         +--------------------------+--------------------------+
                         |                                                     |
                   POST /enqueue                                          GET /metrics
                         |                                                     |
                         v                                                     v
             +-----------------------+                             +-----------------------+
             |   Producer (FastAPI)  |                             |   Producer (FastAPI)  |
             | - UUID generation     |                             | - Live Queue Depths   |
             | - Pydantic validation |                             | - Completed / Failed  |
             | - Hash initialization |                             | - In-flight & Delayed |
             +-----------+-----------+                             +-----------------------+
                         |
                 LPUSH queue:default
                         |
                         v
             +-------------------------------------------------------------------------+
             |                                REDIS                                    |
             |  - queue:default    (FIFO in-memory queue)                              |
             |  - queue:delayed    (Sorted Set for exponential backoff retries)        |
             |  - queue:dead       (Dead Letter Queue for exhausted jobs)              |
             |  - job:<id>         (Hash: status, payload, metadata, timings)          |
             |  - workqueue:stats  (Atomic counters via HINCRBY)                       |
             |  - workqueue:workers(Active worker heartbeats with TTLs)                |
             +------------------------------------+------------------------------------+
                                                  |
                                      BRPOP / Delayed Scheduler
                                                  |
                                                  v
             +-------------------------------------------------------------------------+
             |                    Worker Service Pool (Asyncio)                        |
             |                                                                         |
             |   +-------------------+   +-------------------+   +-------------------+ |
             |   |   worker-1 task   |   |   worker-2 task   |   |   worker-3 task   | |
             |   +---------+---------+   +---------+---------+   +---------+---------+ |
             |             |                       |                       |           |
             |             +-----------------------+-----------------------+           |
             |                                     |                                   |
             |                       Task Dispatcher (CPU vs I/O)                      |
             |                      /                            \                     |
             |            [I/O Bound: async]              [CPU Bound: ProcessPool]     |
             |            - send_email                    - resize_image               |
             |            - webhooks                      - generate_pdf               |
             |                                                                         |
             |   - Persistent Audit Logger (`logs/audit.log` & `logs/worker.log`)      |
             |   - Graceful Drain on SIGINT/SIGTERM                                    |
             +-------------------------------------------------------------------------+
```

---

## Feature Comparison: Python WorkQueue vs Go WorkQueue

| Feature | Go Version (`AbhinavXJ/WorkQueue`) | Python WorkQueue (This Project) |
| :--- | :--- | :--- |
| **Worker Concurrency** | 3 Goroutines | Configurable Async Worker Pool (`WORKER_CONCURRENCY=3`) |
| **Task Routing & GIL Handling** | Go runtime threads | Hybrid Dispatcher: Asyncio for I/O + `ProcessPoolExecutor` for CPU |
| **Job Identity & Querying** | None (Anonymous payloads) | UUID4 generation + `GET /jobs/{job_id}` |
| **Metrics (`GET /metrics`)** | In-memory variables (data races) | Atomic Redis counters (`HINCRBY`) + pipeline queries |
| **Failure Handling** | `log.Fatal()` (crashes entire process) | Automatic retry decrement + **Dead Letter Queue** (`queue:dead`) |
| **Retry Strategy** | Immediate re-queuing | **Exponential Backoff** ($2^{\text{attempt}}$ seconds) via Redis Sorted Sets |
| **Shutdown** | None (Immediate process kill) | Graceful draining of in-flight jobs on `SIGINT`/`SIGTERM` |
| **Audit Logging** | Hardcoded `/WorkQueue/logs.txt` | Rotating file logs (`logs/worker.log` & `logs/audit.log`) + `GET /audit-logs` |
| **API Documentation** | Manual README | Auto-generated interactive OpenAPI/Swagger at `/docs` |
| **Testing** | None | Automated unit & integration suite with `pytest` + `fakeredis` |

---

## Project Structure

```
workqueue/
├── docs/                                    ← In-depth study guides for each architectural phase
│   ├── PHASE_1_CONCURRENCY_AND_WORKER_POOL.md
│   ├── PHASE_2_METRICS_AND_OBSERVABILITY.md
│   ├── PHASE_3_AUDIT_LOGGING_AND_TRACING.md
│   ├── PHASE_4_EXPONENTIAL_BACKOFF_AND_RELIABILITY.md
│   ├── PHASE_5_CPU_VS_IO_BOUND_GIL_BYPASS.md
│   └── PHASE_6_TESTING_DOCKER_INTERVIEW_MASTERY.md
├── producer/
│   ├── Dockerfile
│   └── main.py                              ← FastAPI service (/enqueue, /metrics, /jobs, /dead-letter, /audit-logs)
├── worker/
│   ├── Dockerfile
│   ├── handlers.py                          ← Hybrid CPU/I/O task dispatcher with ProcessPoolExecutor
│   └── main.py                              ← Worker pool, delayed queue scheduler, graceful shutdown
├── shared/
│   ├── logger.py                            ← Dual console/file logger and structured AuditLogger
│   ├── redis_client.py                      ← Redis client, atomic metrics, and heartbeat leasing
│   └── schemas.py                           ← Pydantic v2 Job and QueueMetrics models
├── tests/
│   ├── test_handlers.py                     ← Handler execution & validation tests
│   ├── test_queue_and_metrics.py            ← FIFO ordering, metrics, and delayed queue migration tests
│   └── test_schemas.py                      ← Schema serialization & default value tests
├── logs/                                    ← Persistent audit & worker log outputs
├── docker-compose.yml                       ← Containerized orchestration with persistent volumes
├── requirements.txt
└── README.md
```

---

## Services & Endpoints

### 1. Producer Service (FastAPI)
- `POST /enqueue` — Ingest background task, assign UUID, validate payload, and push to queue.
- `GET /metrics` — Real-time queue depths, cumulative success/failure counts, active workers, and uptime.
- `GET /jobs/{job_id}` — Query the lifecycle status (`pending`, `processing`, `completed`, `retrying`, `dead`) and timing of a specific job.
- `GET /dead-letter` — Retrieve failed jobs stored in the Dead Letter Queue (`queue:dead`).
- `GET /audit-logs` — Retrieve recent structured execution audit logs from `logs/audit.log`.
- `GET /health` — Check service health and Redis connectivity.
- `GET /docs` — Interactive Swagger UI documentation.

### 2. Worker Service (Asyncio)
- **Concurrent Worker Pool:** Multiple worker coroutines (`worker-1`, `worker-2`, `worker-3`) concurrently polling `queue:default` via atomic `BRPOP`.
- **Hybrid Task Execution:**
  - `send_email`: Asynchronous non-blocking network I/O simulation.
  - `resize_image` & `generate_pdf`: Offloaded to `ProcessPoolExecutor` to bypass the Python GIL and utilize multi-core CPU capacity.
- **Delayed Queue Scheduler:** Background coroutine migrating due exponential backoff tasks from `queue:delayed` back to `queue:default`.
- **Fault Tolerance:** Failed tasks retry with exponential backoff ($2^s$ seconds delay). Permanent failures are routed to `queue:dead`.
- **Graceful Shutdown:** Intercepts `SIGINT`/`SIGTERM` to drain in-flight jobs cleanly before stopping.

---

## Quickstart

### Run with Docker Compose

```bash
docker compose up --build
```

To scale workers horizontally across multiple containers:
```bash
docker compose up --build --scale worker=3
```

---

### Run Locally (Without Docker)

1. **Start Redis** (ensure Redis is running on port 6379, or use Docker):
```bash
docker run -d -p 6379:6379 redis:7-alpine
```

2. **Terminal 1 — Start Producer**:
```bash
python -m uvicorn producer.main:app --reload --port 8000
```

3. **Terminal 2 — Start Worker Pool**:
```bash
python -m worker.main
```

---

## Testing & API Examples

### 1. Enqueue an Email Task (I/O Bound)
```bash
curl -X POST http://localhost:8000/enqueue \
  -H "Content-Type: application/json" \
  -d '{"type": "send_email", "retries": 3, "payload": {"to": "team@example.com", "subject": "Quarterly Update"}}'
```

### 2. Enqueue an Image Resize Task (CPU Bound)
```bash
curl -X POST http://localhost:8000/enqueue \
  -H "Content-Type: application/json" \
  -d '{"type": "resize_image", "retries": 3, "payload": {"width": 1920, "height": 1080}}'
```

### 3. Check Live System Metrics
```bash
curl http://localhost:8000/metrics
```
Response:
```json
{
  "total_jobs_in_queue": 0,
  "jobs_delayed": 0,
  "jobs_in_dlq": 0,
  "jobs_completed": 12,
  "jobs_failed": 0,
  "jobs_retried": 0,
  "active_workers": 3,
  "uptime_seconds": 184.2
}
```

### 4. Query Job Status
```bash
curl http://localhost:8000/jobs/<job_id>
```

### 5. Inspect Audit Logs
```bash
curl http://localhost:8000/audit-logs
```

### 6. Run Automated Test Suite
```bash
pytest -v tests/
```

---

## Detailed Study Guides
Each architectural pillar is thoroughly explained in the [`docs/`](file:///c:/workque/docs/) directory:
- [Phase 1: Worker Concurrency & Multi-Worker Architecture](file:///c:/workque/docs/PHASE_1_CONCURRENCY_AND_WORKER_POOL.md)
- [Phase 2: Live Metrics & System Monitoring](file:///c:/workque/docs/PHASE_2_METRICS_AND_OBSERVABILITY.md)
- [Phase 3: Persistent Audit & Event Logging](file:///c:/workque/docs/PHASE_3_AUDIT_LOGGING_AND_TRACING.md)
- [Phase 4: Advanced Reliability & Exponential Backoff Retries](file:///c:/workque/docs/PHASE_4_EXPONENTIAL_BACKOFF_AND_RELIABILITY.md)
- [Phase 5: CPU-Bound vs I/O-Bound Task Separation (GIL Bypass)](file:///c:/workque/docs/PHASE_5_CPU_VS_IO_BOUND_GIL_BYPASS.md)
- [Phase 6: Automated Testing Suite & Master Interview Playbook](file:///c:/workque/docs/PHASE_6_TESTING_DOCKER_INTERVIEW_MASTERY.md)
