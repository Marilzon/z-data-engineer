from scripts.source_download import download_from_url
from scripts.job import load_parquet, show_schema, start_spark 


file_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-01.parquet"
target_dir = "landing"

local_file_path = download_from_url(file_url, target_dir)

spark = start_spark()
dataframe = load_parquet(spark, local_file_path)
show_schema(dataframe)