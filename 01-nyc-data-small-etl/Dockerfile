FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY . .

RUN wget -P /app/drivers https://jdbc.postgresql.org/download/postgresql-42.7.6.jar

RUN pip install --upgrade pip
RUN pip install uv==0.7.3
RUN uv init .
RUN uv add -r requirements.txt

ENTRYPOINT  [ "uv", "run", "scripts/landing_to_raw.py" ]