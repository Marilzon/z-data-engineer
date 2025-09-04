from scripts.source_download import SourceDownload
from scripts.job import SparkJob


file_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-01.parquet"
target_dir = "landing"

downloader = SourceDownload(file_url, target_dir)
downloader.download_from_url()

job = SparkJob()

job_df = job.load_parquet(downloader.full_path)

job.show_schema(job_df)