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
        libgl1 \
        libglib2.0-0 \
        ca-certificates \
        gnupg \
        wget \
    && mkdir -p /etc/apt/keyrings \
    && wget -qO /etc/apt/keyrings/osgeo-archive-keyring.gpg https://download.osgeo.org/osgeo_keyring.gpg \
    && echo "deb [signed-by=/etc/apt/keyrings/osgeo-archive-keyring.gpg] https://download.osgeo.org/debian bookworm main" > /etc/apt/sources.list.d/osgeo.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        pdal \
        libpdal-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN python -m pip install --upgrade pip \
    && pip install -r requirements.txt

COPY . .

ENV PYTHONPATH=/app

CMD ["python", "-m", "lidar_app.app.cli", "--help"]
