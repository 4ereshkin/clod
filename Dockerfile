FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        g++ \
        libpq-dev \
        gdal-bin \
        libgdal-dev \
        pdal \
        libpdal-dev \
        libgl1 \
        libglib2.0-0 \
        ca-certificates \
    && echo "deb http://deb.debian.org/debian bookworm-backports main" > /etc/apt/sources.list.d/bookworm-backports.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends -t bookworm-backports \
        pdal \
        libpdal-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN python -m pip install --upgrade pip \
    && pip install -r requirements.txt

COPY . .

ENV PYTHONPATH=/app

CMD ["python", "-m", "lidar_app.app.cli", "--help"]
