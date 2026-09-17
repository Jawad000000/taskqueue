# WorkQueue

A distributed background job processing system built with **FastAPI**, **Redis**, and **asyncio**.

## Architecture

```
Client → POST /enqueue → [Producer] → Redis Queue → [Worker] → Execute Job
                                 ↓                         ↓
                           Redis Hashes              Dead Letter Queue
                            (Job Status)               (queue:dead)
```

## Services

### Producer
- `GET /health` — Check system and Redis health status
- `POST /enqueue` — Accepts a job payload, assigns a UUID, initializes status tracking in Redis, and pushes to Redis queue
- `GET /jobs/{job_id}` — Query the current status of a specific job (`pending`, `processing`, `completed`, `retrying`, `dead`)
- `GET /dead-letter` — Retrieve failed jobs stored in the Dead Letter Queue (`queue:dead`)

### Worker
- Polls Redis with `BRPOP`
- Updates job status in Redis as work progresses (`processing`, `completed`, `retrying`, `dead`)
- Executes jobs by type (`send_email`, `resize_image`, `generate_pdf`)
- Retries failed jobs up to configured `retries` count
- Pushes unrecoverable jobs to Dead Letter Queue (`queue:dead`)
- Uses Python `logging` for structured output and handles `SIGINT`/`SIGTERM` for graceful shutdown

## Job Format

```json
{
    "type": "send_email",
    "retries": 3,
    "payload": {
        "to": "user@example.com",
        "subject": "Hello"
    }
}
```

## Project Structure

```
workqueue/
├── producer/       ← FastAPI app, /enqueue, /jobs, /health, /dead-letter routes
├── worker/         ← Polling loop, handlers, status updates, DLQ, graceful shutdown
├── shared/         ← Job schema, Redis client
├── docker-compose.yml
└── requirements.txt
```

## Run with Docker

```bash
docker-compose up --build
```

## Run Locally

**Terminal 1 — Producer**
```bash
python -m uvicorn producer.main:app --reload --port 8000
```

**Terminal 2 — Worker**
```bash
python -m worker.main
```

**Enqueue a job**
```bash
curl -X POST http://localhost:8000/enqueue \
  -H "Content-Type: application/json" \
  -d '{"type": "send_email", "retries": 3, "payload": {"to": "test@example.com", "subject": "Hello"}}'
```

**Check Job Status**
```bash
curl http://localhost:8000/jobs/<job_id>
```

**Check Health**
```bash
curl http://localhost:8000/health
```

**View Dead Letter Queue**
```bash
curl http://localhost:8000/dead-letter
```

## Tech Stack

- Python 3.11+
- FastAPI
- Redis (via `redis.asyncio`)
- Pydantic v2
- Docker + docker-compose

