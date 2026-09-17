# Phase 3: Persistent Audit & Event Logging System

## 1. Executive Summary & Core Motivation

In production job processors, relying solely on console logs (`stdout`) has severe limitations:
1. **Ephemeral:** When a Docker container crashes or restarts, console logs can disappear unless connected to an external log aggregator.
2. **Hard to Audit:** Operators need a distinct, structured audit trail answering:
   - *Which worker executed Job X?*
   - *What was the exact execution duration in milliseconds?*
   - *What were the payload parameters?*
   - *If it failed, what was the stack trace or root cause?*

In Abhinav's Go repo, a `logs.txt` file was created by `internal/logger/logger.go`:
```go
// From Abhinav's Go repo:
func LogFailure(cur_task task.Task, cur_err error) {
    f, err := os.OpenFile("/WorkQueue/logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    ...
}
```
However, the Go implementation had major issues:
- **Hardcoded Absolute Path (`/WorkQueue/logs.txt`):** Fails on Windows and any system where root `/` is not writable.
- **No Log Rotation:** Continually appending without size limits will eventually exhaust server disk storage.
- **Unstructured Format:** Non-standard string formatting making grep / log ingestion difficult.

In Phase 3, we built an enterprise-ready **Persistent Audit & Logging System** with **Rotating File Handlers** and an HTTP inspection endpoint (`GET /audit-logs`).

---

## 2. Architecture & File Layout

```
workqueue/
├── logs/
│   ├── worker.log       ← Technical operational logs (debug, info, error) with 5MB rotation
│   └── audit.log        ← Structured business event trail with 10MB rotation
```

### Structured Event Format
Every transition produces a standardized, grep-friendly line in `logs/audit.log`:

```text
[2026-09-18 00:43:00] [ENQUEUED] JobID=b1c2-48a1 Type=send_email RetriesLeft=3 Payload={"to": "user@test.com", "subject": "Welcome"} QueueDepth=1
[2026-09-18 00:43:01] [SUCCESS] JobID=b1c2-48a1 Type=send_email WorkerID=worker-1 Duration=503.2ms RetriesLeft=3 Payload={"to": "user@test.com"} Result="Email successfully transmitted..."
[2026-09-18 00:43:05] [FAILURE] JobID=f8e3-99b2 Type=resize_image WorkerID=worker-2 Duration=14.5ms RetriesLeft=2 Error="Invalid resize dimensions" NextRetryIn=2s
[2026-09-18 00:43:15] [DLQ] JobID=f8e3-99b2 Type=resize_image WorkerID=worker-2 Duration=12.1ms RetriesLeft=0 Error="Invalid resize dimensions"
```

---

## 3. Code Deep-Dive

### Centralized Dual-Handler Logger: [shared/logger.py](file:///c:/workque/shared/logger.py)
```python
def setup_logger(name: str = "workqueue", level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    ...
    # Console Stream Handler
    console_handler = logging.StreamHandler()
    logger.addHandler(console_handler)

    # 5MB Rotating File Handler
    file_handler = RotatingFileHandler(
        WORKER_LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    logger.addHandler(file_handler)
    return logger
```

### Dedicated Audit Logger: [shared/logger.py](file:///c:/workque/shared/logger.py)
```python
class AuditLogger:
    def log_event(
        self,
        event: str,
        job_id: str,
        job_type: str,
        worker_id: Optional[str] = None,
        duration_ms: Optional[float] = None,
        retries_left: Optional[int] = None,
        payload: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ):
        ...
```

### HTTP Inspection Endpoint: [producer/main.py](file:///c:/workque/producer/main.py)
Allows developers and interviewers to view recent audit logs directly from the browser or curl:
```python
@app.get("/audit-logs")
async def get_audit_logs(limit: int = Query(default=50, ge=1, le=500)):
    logs = get_recent_audit_logs(limit=limit)
    return {"count": len(logs), "logs": logs}
```

---

## 4. Senior Interview Questions & Answers

### Q1: "In modern cloud environments (Kubernetes/12-Factor Apps), why log to files instead of just `stdout`?"
> **Answer:** "12-Factor App methodology recommends streaming logs to `stdout` for container log collectors (like Fluentbit or Promtail). However, in high-throughput systems, mixing operational debug traces with compliance audit records creates noise. We implement a hybrid model: technical operational logs stream to `stdout` and rotating local files for immediate debugging, while business-critical lifecycle events (`ENQUEUED`, `COMPLETED`, `DLQ`) flow through a structured audit logger that can be mounted to a persistent volume or shipped directly to cold storage (S3/OpenSearch)."

### Q2: "Why use `RotatingFileHandler` rather than standard file append?"
> **Answer:** "Standard append (`open(path, 'a')`) will continuously write to disk until the filesystem runs out of inodes or disk space, causing the server to freeze. `RotatingFileHandler` caps each file (e.g. 5MB) and maintains a fixed sliding window of backup archives (e.g. 3 backups). Once the ceiling is reached, the oldest archive is purged automatically."
