import logging
import os
import sys
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

## CONFIG
jar_driver_path = "../drivers/postgresql-42.7.6"
landing_path = "../landing/"
user = os.getenv("POSTGRES_USER", "root")
password = os.getenv("POSTGRES_PASSWORD", "root")
host = os.getenv("POSTGRES_HOST", "localhost")
port = os.getenv("POSTGRES_PORT", "5432")
db = os.getenv("POSTGRES_DB", "nyc_taxi")
table_name = os.getenv("TABLE_NAME", "yellow_taxi_trips")
data_url = os.getenv(
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"
)
file_name = data_url.split("/")[-1]

## DOWNLOAD FILE
try:
    os.system(f"wget {data_url} -O {landing_path}/{file_name}")

    logger.info(f"File {file_name} downloaded successfully")
except Exception as e:
    logger.error(f"Error downloading file: {e}")
    sys.exit(1)

## SPARK READ
spark: SparkSession = SparkSession.builder.config(
    "spark.jars", jar_driver_path
).getOrCreate()
logger.info("Spark session created successfully")

try:
    df: DataFrame = spark.read.parquet(file_name)
    logger.info(f"DataFrame schema: {df.printSchema()}")
    logger.info(f"Total records: {df.count()}")
except Exception as e:
    logger.error(f"Error reading parquet file: {e}")
    sys.exit(1)

## SPARK WRITE

jdbc_url = f"jdbc:postgresql://{host}:{port}/db"
properties = {"user": user, "password": password, "driver": "ord.postgresql.Driver"}

try:
    (
        df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )

    logger.info("Data successfully written to PostgreSQL")
except Exception as e:
    logger.error(f"Error writing to PostgreSQL: {e}")

## Cleaning
try:
    if os.path.exists(f"{landing_path}/{file_name}"):
        os.remove(file_name)
        logger.info("Temporary file removed")
except Exception as e:
    logger.warning(f"Could not remove temporary file: {e}")

# Parar Spark session
spark.stop()
logger.info("Spark session stopped")
