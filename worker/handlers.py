from shared.schemas import Job
import asyncio

async def process_job(job:Job):
    if job.payload is None:
        raise ValueError("payload is empty")
    
    match job.type:
        case "send_email":
            await asyncio.sleep(2)
            print(f"Sending email to {job.payload['to']}")

        case "resize_image":
            await asyncio.sleep(1)
            print("Resizing image...")

        case "generate_pdf":
            await asyncio.sleep(1)
            print("Generating PDF...")
        
        case _:
            raise ValueError(f"Unsupported job type: {job.type}")