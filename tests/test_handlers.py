import pytest
from shared.schemas import Job
from worker.handlers import process_job, shutdown_cpu_pool


@pytest.mark.asyncio
async def test_process_job_send_email():
    job = Job(
        type="send_email",
        payload={"to": "dev@company.com", "subject": "Welcome aboard"},
    )
    result = await process_job(job)
    assert "dev@company.com" in result
    assert "Email successfully transmitted" in result


@pytest.mark.asyncio
async def test_process_job_send_email_invalid():
    job = Job(type="send_email", payload={"to": "not-an-email"})
    with pytest.raises(ValueError, match="Invalid or missing recipient"):
        await process_job(job)


@pytest.mark.asyncio
async def test_process_job_resize_image():
    job = Job(type="resize_image", payload={"new_x": 400, "new_y": 300})
    result = await process_job(job)
    assert "Resized image to 400x300" in result


@pytest.mark.asyncio
async def test_process_job_resize_image_invalid():
    job = Job(type="resize_image", payload={"new_x": -10, "new_y": 100})
    with pytest.raises(ValueError, match="Invalid resize dimensions"):
        await process_job(job)


@pytest.mark.asyncio
async def test_process_job_generate_pdf():
    job = Job(type="generate_pdf", payload={"pages": 2, "title": "Quarterly Report"})
    result = await process_job(job)
    assert "Generated 2-page PDF 'Quarterly Report'" in result


@pytest.mark.asyncio
async def test_process_job_unsupported_type():
    job = Job(type="unknown_action", payload={})
    with pytest.raises(ValueError, match="Unsupported job type"):
        await process_job(job)


def teardown_module():
    shutdown_cpu_pool()
