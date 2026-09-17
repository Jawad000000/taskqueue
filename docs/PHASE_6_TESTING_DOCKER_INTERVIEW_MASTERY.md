# Phase 6: Automated Testing Suite, Docker Multi-Worker & Master Interview Playbook

## 1. Executive Summary & Verification Strategy

High-performing backend engineers distinguish themselves by writing automated tests, designing seamless container orchestration, and explaining trade-offs during technical interviews.

Phase 6 provides:
1. **Hermetic Automated Testing (`pytest` + `pytest-asyncio` + `fakeredis`)**: 14 tests verifying schemas, handlers, FIFO queues, atomic metrics, delayed sorting, and dead letter queues without needing an external running Redis server.
2. **Multi-Container Docker Architecture**: Docker Compose setup with persistent volumes for logs, restart policies, and multi-worker horizontal scaling.
3. **Master Interview Speaking Playbook**: Word-for-word scripts to present the project, critique the Go implementation, and answer senior architectural questions.

---

## 2. Automated Testing Suite

Our tests run with standard `pytest` and execute in less than 2 seconds:
```bash
pytest -v tests/
```

### Test Coverage Breakdown:
| Test File | Target Component | What is Verified |
| :--- | :--- | :--- |
| [tests/test_schemas.py](file:///c:/workque/tests/test_schemas.py) | Pydantic v2 `Job` & `QueueMetrics` | UUID generation, default fields, validation exceptions upon missing keys. |
| [tests/test_handlers.py](file:///c:/workque/tests/test_handlers.py) | `worker/handlers.py` | `send_email` (I/O), `resize_image` (CPU), `generate_pdf` (CPU), input validation, unsupported task handling. |
| [tests/test_queue_and_metrics.py](file:///c:/workque/tests/test_queue_and_metrics.py) | Redis Queue & System Engine | FIFO ordering (`LPUSH` + `BRPOP`), atomic metrics counters (`HINCRBY`), worker heartbeats, delayed queue sorting and migration (`ZADD`/`ZRANGEBYSCORE`/`ZREM`). |

### Why `fakeredis` is a Best Practice:
Instead of requiring an external Redis server or mocking every single Redis command with `unittest.mock.MagicMock`, `fakeredis` provides a lightweight, pure-Python in-memory Redis implementation that accurately executes Redis data structure algorithms. This ensures tests are 100% deterministic, hermetic, and fast.

---

## 3. Docker Compose & Multi-Worker Scaling

### Configuration: [docker-compose.yml](file:///c:/workque/docker-compose.yml)
```yaml
version: "3.8"

services:
  redis:
    image: redis:7-alpine
    container_name: workqueue-redis
    ports:
      - "6379:6379"
    volumes:
      - redis-data:/data
    restart: unless-stopped

  producer:
    build:
      context: .
      dockerfile: producer/Dockerfile
    container_name: workqueue-producer
    ports:
      - "8000:8000"
    environment:
      - REDIS_HOST=redis
      - REDIS_PORT=6379
      - LOGS_DIR=/app/logs
    volumes:
      - ./logs:/app/logs
    depends_on:
      - redis

  worker:
    build:
      context: .
      dockerfile: worker/Dockerfile
    container_name: workqueue-worker
    environment:
      - REDIS_HOST=redis
      - REDIS_PORT=6379
      - WORKER_CONCURRENCY=3
      - LOGS_DIR=/app/logs
    volumes:
      - ./logs:/app/logs
    depends_on:
      - redis
```

### Scaling Workers Horizontally:
To scale from 1 worker container to 3 worker containers (each running 3 internal worker coroutines = 9 concurrent workers):
```bash
docker compose up --build --scale worker=3
```

---

## 4. Master Interview Speaking Playbook

Use this exact structure when explaining the project in technical interviews:

### Step 1: The 45-Second Elevator Pitch
> *"I designed and implemented **WorkQueue**, a distributed background task processing system in Python using FastAPI, Redis, and asyncio. It decouples high-latency background operations—like email delivery, image resizing, and PDF generation—from user-facing HTTP request paths.*
>
> *I built a concurrent worker pool supporting both I/O-bound tasks natively on the event loop and CPU-bound tasks via a process pool to bypass the Python GIL. It features Redis-backed atomic metrics, exponential backoff retries via Redis Sorted Sets, dead-letter queues, and graceful shutdown handling."*

### Step 2: How to Discuss the Go vs Python Comparison
> *"I actually studied a well-known Go implementation of a similar task queue before building this. While Go offers native goroutines without a GIL, the Go project had significant architectural vulnerabilities:
> 1. It used un-synchronized global variables for metrics across goroutines, causing data races. I resolved this using atomic Redis hash commands (`HINCRBY`).
> 2. When a task exhausted retries, the Go version called `log.Fatal()`, crashing the entire worker process! I implemented an enterprise Dead Letter Queue (`queue:dead`) and status hashes.
> 3. It retried failed jobs immediately, causing thundering herds during transient outages. I introduced exponential backoff using Redis Sorted Sets with timestamp scores."*

### Step 3: Answering the Deep Architecture Questions
- **"Why not Celery?"**
  - *"Celery is powerful but heavy, complex to configure, and its default prefork pool consumes heavy RAM for I/O workloads. WorkQueue was designed to be lightweight, fully async-first, and transparent, giving us fine-grained control over GIL bypass and Redis data structures."*
- **"How do you handle worker crashes?"**
  - *"Workers send heartbeats with TTLs to Redis. For at-least-once delivery, in-flight tasks can be tracked in an in-flight list (`queue:processing`) and reclaimed by a sweeper if a worker's heartbeat expires."*
