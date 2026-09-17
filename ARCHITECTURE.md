# WorkQueue Architecture Overview

For the complete, end-to-end system design, component breakdown, and senior interview compendium, refer to the master design document:
👉 **[docs/DESIGN.md](file:///c:/workque/docs/DESIGN.md)**

---

## High-Level Architecture Diagram

```
                              +---------------------------------------+
                              |              Client / HTTP            |
                              +-------------------+-------------------+
                                                  |
                         +------------------------+------------------------+
                         |                                                 |
                   POST /enqueue                                      GET /metrics
                         |                                                 |
                         v                                                 v
             +-----------------------+                         +-----------------------+
             |   Producer (FastAPI)  |                         |   Producer (FastAPI)  |
             | - UUID generation     |                         | - Live Queue Depths   |
             | - Pydantic validation |                         | - Completed / Failed  |
             | - Hash initialization |                         | - In-flight & Delayed |
             +-----------+-----------+                         +-----------------------+
                         |
                 LPUSH queue:default
                         |
                         v
             +-------------------------------------------------------------+
             |                           REDIS 7                           |
             |  - queue:default    (FIFO in-memory queue via Lists)        |
             |  - queue:delayed    (Sorted Set for exponential backoff)    |
             |  - queue:dead       (Dead Letter Queue for exhausted jobs)  |
             |  - job:<id>         (Hash: status, payload, metadata)      |
             |  - workqueue:stats  (Atomic counters via HINCRBY)           |
             |  - workqueue:workers(Active worker heartbeats with TTLs)    |
             +-----------------------------+-------------------------------+
                                           |
                              BRPOP / Delayed Scheduler
                                           |
                                           v
             +-------------------------------------------------------------+
             |                   Worker Service (Asyncio)                  |
             |                                                             |
             |  +-------------------+  +-------------------+  +----------+ |
             |  |   Worker Task 1   |  |   Worker Task 2   |  | Worker N | |
             |  +---------+---------+  +---------+---------+  +----+-----+ |
             |            |                      |                 |       |
             |            +----------------------+-----------------+       |
             |                                   |                         |
             |                Task Dispatcher (CPU vs I/O)                 |
             |               /                            \                |
             |     [I/O Bound: async]              [CPU Bound: ProcessPool]|
             |     - send_email                    - resize_image          |
             |     - webhooks                      - generate_pdf          |
             |                                                             |
             |  - Persistent Audit Logger (`logs/audit.log` & `worker.log`)|
             |  - Graceful Shutdown Drain (SIGINT/SIGTERM)                 |
             +-------------------------------------------------------------+
```

---

## Architectural Pillars

1. **Producer Service (`FastAPI`):** Non-blocking ingestion yielding sub-5ms response times.
2. **Broker & State Registry (`Redis 7`):** Strict FIFO queuing with `LPUSH`/`BRPOP`, O(1) state lookups via Hashes (`job:{id}`), and atomic metrics counters via `HINCRBY`.
3. **Concurrent Worker Pool (`asyncio`):** Configurable concurrency pool executing multiple tasks simultaneously.
4. **Hybrid CPU/IO Dispatcher:** Native coroutines for I/O tasks (`send_email`) and `ProcessPoolExecutor` for CPU-heavy tasks (`resize_image`, `generate_pdf`) to bypass the Python GIL.
5. **Fault Tolerance & Reliability:** Exponential backoff retries ($2^{\text{attempt}}$ seconds) using Redis Sorted Sets, and Dead Letter Queue (`queue:dead`) for poison pill isolation.
6. **Observability & Audit Trail:** Real-time metrics endpoint (`GET /metrics`), rotating technical logs (`logs/worker.log`), and persistent structured audit logs (`logs/audit.log`).

---

## Reference Documents
- **[docs/DESIGN.md](file:///c:/workque/docs/DESIGN.md)**: Master System Design & Interview Compendium
- **[docs/PHASE_1_CONCURRENCY_AND_WORKER_POOL.md](file:///c:/workque/docs/PHASE_1_CONCURRENCY_AND_WORKER_POOL.md)**
- **[docs/PHASE_2_METRICS_AND_OBSERVABILITY.md](file:///c:/workque/docs/PHASE_2_METRICS_AND_OBSERVABILITY.md)**
- **[docs/PHASE_3_AUDIT_LOGGING_AND_TRACING.md](file:///c:/workque/docs/PHASE_3_AUDIT_LOGGING_AND_TRACING.md)**
- **[docs/PHASE_4_EXPONENTIAL_BACKOFF_AND_RELIABILITY.md](file:///c:/workque/docs/PHASE_4_EXPONENTIAL_BACKOFF_AND_RELIABILITY.md)**
- **[docs/PHASE_5_CPU_VS_IO_BOUND_GIL_BYPASS.md](file:///c:/workque/docs/PHASE_5_CPU_VS_IO_BOUND_GIL_BYPASS.md)**
- **[docs/PHASE_6_TESTING_DOCKER_INTERVIEW_MASTERY.md](file:///c:/workque/docs/PHASE_6_TESTING_DOCKER_INTERVIEW_MASTERY.md)**
- **[master_interview_prep_guide.md](file:///c:/workque/master_interview_prep_guide.md)**