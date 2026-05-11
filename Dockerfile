FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
        tzdata \
        gcc \
    && rm -rf /var/lib/apt/lists/*


COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY bot/ ./bot/
COPY shared/ ./shared/
COPY data/ ./data/

CMD ["python", "-m", "bot.main"]
