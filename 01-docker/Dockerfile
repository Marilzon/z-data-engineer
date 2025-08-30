FROM python:3.11-slim

WORKDIR /app

RUN pip install --upgrade pip && \
	apt-get update && \
	apt-get upgrade -y && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/*

RUN pip install pandas

COPY . .

ENTRYPOINT [ "python", "pipeline.py" ]