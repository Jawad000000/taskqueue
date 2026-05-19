FROM python:3.11-slim

WORKDIR /app

COPY ../requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ../shared ./shared
COPY ../producer ./producer

CMD ["uvicorn", "producer.main:app", "--host", "0.0.0.0", "--port", "8000"]
