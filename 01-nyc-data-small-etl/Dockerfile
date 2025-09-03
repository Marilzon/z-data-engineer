FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY . .

RUN pip install --upgrade pip
RUN pip install uv==0.7.3
RUN uv init .
RUN uv add -r requirements.txt

ENTRYPOINT  [ "uv", "run", "scripts/landing_to_raw.py" ]