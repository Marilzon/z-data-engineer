import logging
import pandas as pd
import time
import datetime
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

current_time = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

logger.info(f"pandas version: {pd.__version__}")

def log_tasks(value=1):
  value = int(value) + 1
  logger.info(f"pipeline started at: {current_time}")
  
  for n in range(1, value):
    logger.info(f"task {n} success")
    
print(sys.argv)   
log_tasks(sys.argv[1])
  