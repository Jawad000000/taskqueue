import asyncio
import logging

from shared.schemas import Job

logger = logging.getLogger("worker.handlers")


async def process_job(job: Job):
    if job.payload is None:
        raise ValueError("payload is empty")

    match job.type:
        case "send_email":
            await asyncio.sleep(2)
            logger.info(f"Sending email to {job.payload.get('to')}")

        case "resize_image":
            await asyncio.sleep(1)
            logger.info("Resizing image...")

        case "generate_pdf":
            await asyncio.sleep(1)
            logger.info("Generating PDF...")

        case _:
            raise ValueError(f"Unsupported job type: {job.type}")