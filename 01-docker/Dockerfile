FROM python:3.12-slim

WORKDIR /app

RUN pip install --upgrade pip && \
	apt-get update && \
	apt-get upgrade -y && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/*

RUN pip install uv
RUN uv init .
RUN uv add pandas
RUN uv tree

COPY . .

ENTRYPOINT  [ "uv", "run", "pipeline.py" ]