# Self-contained image for the live l2Book collector — independent of the passivbot
# image (no Rust, no conda, no SDK). Public WebSocket feed, so no credentials.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUTF8=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY l2lib.py collector.py ./
COPY tests ./tests

CMD ["python", "collector.py"]
