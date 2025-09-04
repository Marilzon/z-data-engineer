from pyspark.sql.session import SparkSession
from pyspark.sql import DataFrame

def start_spark():
  return SparkSession.builder.getOrCreate()

def load_parquet(spark_session, file_path) -> DataFrame:
  return spark_session.read.format("parquet").load(file_path)
  
def show_schema(dataframe):
  return dataframe.printSchema()