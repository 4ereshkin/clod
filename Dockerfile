FROM mambaorg/micromamba:bookworm-slim

ARG MAMBA_DOCKERFILE_ACTIVATE=1

USER root

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Runtime libs: libpq for sqlalchemy postgres, gdal for geo, open3d system deps
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libpq5 \
        libgl1 \
        libgomp1 \
        libglib2.0-0 \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# PDAL C++ library + Python bindings via conda-forge (no system libpdal-dev needed)
RUN micromamba install -n base -c conda-forge -y \
        python=3.11 \
        pdal \
        python-pdal \
    && micromamba clean --all --yes

COPY requirements.txt ./
# pdal is managed by conda-forge above, skip it in pip
RUN grep -v "^pdal" requirements.txt | pip install --no-cache-dir -r /dev/stdin

COPY . .

ENV PYTHONPATH=/app

# Default entry point: RabbitMQ/SignalR listener.
# Override command per service in docker-compose.yml:
#   worker-ingest:        python point_cloud/workers/worker_ingest.py
#   worker-registration:  python point_cloud/workers/worker_registration.py
CMD ["python", "main.py"]
