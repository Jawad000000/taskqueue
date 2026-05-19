# WorkQueue

A distributed background job processing system built with **FastAPI**, **Redis**, and **asyncio**.

## Architecture

```
Client → POST /enqueue → [Producer] → Redis Queue → [Worker] → Execute Job
```

## Services

### Producer
- Exposes `POST /enqueue`
- Accepts a job payload, assigns a UUID, pushes to Redis

### Worker
- Polls Redis with `BRPOP`
- Executes jobs by type (`send_email`, `resize_image`, `generate_pdf`)
- Retries failed jobs, marks as dead after retries exhausted

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
├── producer/       ← FastAPI app, /enqueue route
├── worker/         ← polling loop, job handlers
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

## Tech Stack

- Python 3.11+
- FastAPI
- Redis (via `redis.asyncio`)
- Pydantic v2
- Docker + docker-compose
