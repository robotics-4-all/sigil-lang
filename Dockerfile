FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends redis-tools && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt pyproject.toml ./
COPY sigil/ ./sigil/
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir "commlib-py[redis,mqtt,amqp,kafka]>=0.13.0"

COPY . .
RUN pip install --no-cache-dir -e .

CMD ["pytest", "tests/", "-v"]
