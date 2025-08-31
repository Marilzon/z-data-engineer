import logging
import pandas as pd
import time
import os
from pandas import DataFrame
from sqlalchemy import create_engine
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    user = os.getenv("POSTGRES_USER", "root")
    password = os.getenv("POSTGRES_PASSWORD", "root")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "nyc_taxi")
    table_name = os.getenv("TABLE_NAME", "yellow_taxi_trips")

    url = os.getenv(
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"
    )

    if len(sys.argv) > 1:
        url = sys.argv[1]

    if len(sys.argv) > 2:
        table_name = sys.argv[2]

    parquet_name = "output.parquet"

    logger.info(f"Downloading file from {url}")
    os.system(f"wget {url} -O {parquet_name}")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    iter_df = pd.read_parquet(parquet_name, iterator=True, chunksize=100000)

    df: DataFrame = next(iter_df)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")
    df.to_sql(name=table_name, con=engine, if_exists="append")

    while True:
        try:
            start_timestamp = time.time()
            df = next(iter_df)

            df.to_sql(name=table_name, con=engine, if_exists="append")

            end_timestamp = time.time()

            logger.info(
                f"Inserted another chunk, took %.3f seconds",
                (end_timestamp - start_timestamp),
            )

        except StopIteration:
            logger.info("Finished ingesting data into the PostgreSQL database")
            break
        except Exception as e:
            logger.error("Error processing chunk: %s", str(e))
            break

    if os.path.exists(parquet_name):
        os.remove(parquet_name)


if __name__ == "__main__":
    main()
