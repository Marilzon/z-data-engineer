import logging
import requests
import os
import sys
import glob
import polars as pl

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

## CONFIG
landing_path = "/app/landing/"

config = {
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
    "host": os.getenv("POSTGRES_HOST", "postgres"), 
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "db": os.getenv("POSTGRES_DB", "postgres"),
    "table_name": os.getenv("TABLE_NAME", "yellow_taxi_trip"),
    "data_url": os.getenv(
        "PARQUET_URL",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
    )
}

file_name = config["data_url"].split("/")[-1]
data_path = os.path.join(landing_path, file_name)
postgres_connection = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['db']}"

## DOWNLOAD FILE
logger.info(f"Downloading file from {config["data_url"]}")

data_from_url = requests.get(config["data_url"], stream=True)

with open(data_path, 'wb') as f:
    for chunk in data_from_url.iter_content(chunk_size=8192):
        f.write(chunk)
logger.info(f"File {file_name} downloaded successfully")

    
## LIST FILE
logger.info(f"{glob.glob(landing_path)}/*")

## READ WITH POLARS
logger.info("Reading parquet file with Polars...")
data_df = pl.read_parquet(data_path)
logger.info(f"Data shape: {data_df.shape}")

## WRITING TO POSTGRESQL
data_df.write_database(
    table_name=f"{config["table_name"]}",
    connection=postgres_connection,
    engine="adbc",
    if_table_exists="append"
)
logger.info("Data successfully written to PostgreSQL!")