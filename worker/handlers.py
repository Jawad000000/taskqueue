import asyncio
import math
import time
from concurrent.futures import ProcessPoolExecutor
from typing import Any, Dict

from shared.logger import setup_logger
from shared.schemas import Job

logger = setup_logger("workqueue.handlers")

# Process pool for CPU-bound tasks to bypass Python GIL
_cpu_pool: ProcessPoolExecutor | None = None


def get_cpu_pool() -> ProcessPoolExecutor:
    global _cpu_pool
    if _cpu_pool is None:
        _cpu_pool = ProcessPoolExecutor(max_workers=2)
    return _cpu_pool


def shutdown_cpu_pool():
    global _cpu_pool
    if _cpu_pool is not None:
        _cpu_pool.shutdown(wait=True)
        _cpu_pool = None


# ---------------------------------------------------------------------------
# CPU-Bound Task Implementations (Run in separate processes via ProcessPool)
# ---------------------------------------------------------------------------


def _execute_cpu_resize_image(payload: Dict[str, Any]) -> str:
    """
    Simulates intensive CPU-bound image manipulation (matrix transformation, resampling).
    Bypasses Python GIL by executing inside a separate worker process.
    """
    target_x = int(payload.get("new_x", payload.get("width", 800)))
    target_y = int(payload.get("new_y", payload.get("height", 600)))

    if target_x <= 0 or target_y <= 0:
        raise ValueError(f"Invalid resize dimensions: {target_x}x{target_y}")

    # Simulated CPU load: geometric calculations over simulated pixel grid
    total_pixels = min(target_x * target_y, 500_000)
    accumulator = 0.0
    for i in range(total_pixels // 10):
        accumulator += math.sqrt(i) * math.sin(i)

    return f"Resized image to {target_x}x{target_y} (Checksum: {int(accumulator) % 10000})"


def _execute_cpu_generate_pdf(payload: Dict[str, Any]) -> str:
    """
    Simulates intensive CPU-bound PDF layout rendering, vector plotting, and font subsetting.
    """
    pages = int(payload.get("pages", 3))
    title = payload.get("title", "Report")

    if pages <= 0:
        raise ValueError(f"Invalid page count: {pages}")

    # Simulated CPU rendering loop
    checksum = 0
    for page in range(1, pages + 1):
        for i in range(50_000):
            checksum = (checksum + page * i) % 1_000_007

    return f"Generated {pages}-page PDF '{title}' (RenderHash: {checksum})"


# ---------------------------------------------------------------------------
# I/O-Bound Task Implementations (Run natively in asyncio event loop)
# ---------------------------------------------------------------------------


async def _execute_io_send_email(payload: Dict[str, Any]) -> str:
    """
    Simulates asynchronous network I/O (SMTP handshake, TLS negotiation, email transmission).
    Does not consume CPU, perfectly non-blocking on the asyncio event loop.
    """
    recipient = payload.get("to")
    subject = payload.get("subject", "No Subject")

    if not recipient or "@" not in str(recipient):
        raise ValueError(f"Invalid or missing recipient email address: {recipient}")

    # Non-blocking network latency simulation
    await asyncio.sleep(0.5)
    return f"Email successfully transmitted to {recipient} [Subject: {subject}]"


# ---------------------------------------------------------------------------
# Master Task Dispatcher
# ---------------------------------------------------------------------------


async def process_job(job: Job) -> str:
    """
    Routes jobs based on their computational profile (CPU-bound vs I/O-bound).
    Returns execution result message or raises exception upon failure.
    """
    if not job.payload and job.payload != {}:
        raise ValueError("Job payload cannot be null")

    loop = asyncio.get_running_loop()

    match job.type:
        case "send_email":
            # I/O Bound: async coroutine on the main event loop
            return await _execute_io_send_email(job.payload)

        case "resize_image":
            # CPU Bound: offload to ProcessPoolExecutor to avoid blocking event loop
            pool = get_cpu_pool()
            return await loop.run_in_executor(
                pool, _execute_cpu_resize_image, job.payload
            )

        case "generate_pdf":
            # CPU Bound: offload to ProcessPoolExecutor
            pool = get_cpu_pool()
            return await loop.run_in_executor(
                pool, _execute_cpu_generate_pdf, job.payload
            )

        case _:
            raise ValueError(f"Unsupported job type: '{job.type}'")