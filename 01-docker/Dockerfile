FROM python:3.12-slim

WORKDIR /app

RUN pip install --upgrade pip && \
	apt-get update && \
	apt-get upgrade -y && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/*

COPY . .

RUN pip install uv
RUN uv init .
RUN uv add -r requirements.txt


ENTRYPOINT  [ "uv", "run", "pipeline.py" ]