FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# open3d runtime dependencies (bundled pdal/pyproj wheels need no system libs)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libgl1 \
        libgomp1 \
        libglib2.0-0 \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

COPY . .

ENV PYTHONPATH=/app

# Default entry point: RabbitMQ/SignalR listener.
# Override command per service in docker-compose.yml:
#   worker-ingest:        python point_cloud/workers/worker_ingest.py
#   worker-registration:  python point_cloud/workers/worker_registration.py
CMD ["python", "main.py"]
