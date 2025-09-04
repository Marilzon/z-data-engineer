from pyspark.sql.session import SparkSession
from pyspark.sql import DataFrame

class SparkJob:
  def __init__(self):
    self.spark_session = SparkSession.builder.getOrCreate()

  def load_parquet(self, file_path) -> DataFrame:
    return self.spark_session.read.format("parquet").load(file_path)
  
  @staticmethod
  def show_schema(dataframe):
    return dataframe.printSchema()