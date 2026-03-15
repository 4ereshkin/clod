FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# pdal Python bindings (2.4.x) must be compiled against system libpdal.
# build-essential + cmake + libpdal-dev are kept in the image (runtime stage).
# open3d needs libgl1/libgomp1/libglib2.0-0 at runtime.
COPY requirements.txt ./
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        libpdal-dev \
        pdal \
        libgl1 \
        libgomp1 \
        libglib2.0-0 \
        ca-certificates \
    && pip install --upgrade pip \
    && pip install -r requirements.txt \
    && rm -rf /var/lib/apt/lists/*

COPY . .

ENV PYTHONPATH=/app

# Default entry point: RabbitMQ/SignalR listener.
# Override command per service in docker-compose.yml:
#   worker-ingest:        python point_cloud/workers/worker_ingest.py
#   worker-registration:  python point_cloud/workers/worker_registration.py
CMD ["python", "main.py"]
