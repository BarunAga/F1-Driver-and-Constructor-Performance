#This file contions commonly used functions in the bronze layer.
from pyspark.sql import functions as F

def add_ingestion_data(df):
    return  (
                df.
                withColumn('ingestion_timestamp',F.current_timestamp()).
                withColumn('source_file',F.col('_metadata.file_path'))
                               )

