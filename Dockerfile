FROM code.pepega.club/4ereshkin/clod-base:v1.0

COPY . .

ENV PYTHONPATH=/app

CMD ["python", "main.py"]
