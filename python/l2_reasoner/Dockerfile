FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY l2_reasoner/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY l2_reasoner/ ./l2_reasoner/

RUN mkdir -p /data

ENV PYTHONUNBUFFERED=1
ENV L2_DB_PATH=/data/l2_reasoner.db

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

CMD ["python", "-m", "l2_reasoner.main"]
