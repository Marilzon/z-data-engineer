import logging
import os
import requests
import pandas as pd
from sqlalchemy import create_engine

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
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet",
    ),
}

file_name = config["data_url"].split("/")[-1]
data_path = os.path.join(landing_path, file_name)
postgres_connection = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['db']}"

## DOWNLOAD FILE
logger.info(f"Downloading file from {config['data_url']}")

data_from_url = requests.get(config["data_url"], stream=True)

with open(data_path, "wb") as f:
    for chunk in data_from_url.iter_content(chunk_size=8192):
        f.write(chunk)
logger.info(f"File {file_name} downloaded successfully")

## LIST FILE
logger.info(f"Files on landing: {os.listdir(landing_path)}")

## READ WITH PANDAS
logger.info("Reading parquet file with pandas...")
data_df = pd.read_parquet(data_path)
logger.info(f"Data types: {data_df.dtypes}")

## CONNECT TO POSTGRESQL
engine = create_engine(postgres_connection)

## GENERATING CREATE TABLE
def generate_create_table(df, table_name):
    type_mapping = {
        "int64": "BIGINT",
        "float64": "DOUBLE PRECISION",
        "object": "TEXT",
        "datetime64[ns]": "TIMESTAMP",
        "bool": "BOOLEAN",
    }

    columns = []
    for col, dtype in df.dtypes.items():
        postgres_type = type_mapping.get(str(dtype), "TEXT")
        columns.append(f'"{col}" {postgres_type}')

    return f"CREATE TABLE {table_name} (\n    " + ",\n    ".join(columns) + "\n);"


## WRITING TO POSTGRESQL
try:
    create_table_query = generate_create_table(data_df, config["table_name"])

    with engine.begin() as connection:
        connection.execute(create_table_query)

    data_df.to_sql(
        name=config["table_name"],
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
    )

    logger.info("Data successfully written to PostgreSQL!")
except Exception as e:
    logger.error(f"Error: {e}")
