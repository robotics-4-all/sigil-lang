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

EXPOSE 8080

HEALTHCHECK --interval=15s --timeout=5s --retries=5 --start-period=20s \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/health').status==200 else 1)"

CMD ["uvicorn", "sigil.api:app", "--host", "0.0.0.0", "--port", "8080"]
