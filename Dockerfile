FROM condaforge/miniforge3:latest

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Runtime libs for open3d
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libgl1 \
        libgomp1 \
        libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# Install Python 3.11 + pdal (system lib + Python binding) via conda-forge
RUN mamba install -y -c conda-forge \
        python=3.11 \
        python-pdal \
    && mamba clean -afy

COPY requirements-worker.txt ./
# pdal is already installed via conda — skip it here
RUN grep -v '^pdal\|^#\|^$' requirements-worker.txt > /tmp/reqs.txt \
    && pip install --no-cache-dir -r /tmp/reqs.txt

COPY . .

ENV PYTHONPATH=/app

CMD ["python", "main.py"]
