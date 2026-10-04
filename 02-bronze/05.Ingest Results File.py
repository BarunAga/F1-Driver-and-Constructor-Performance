# This file is ingesting results files from landing volume and writing them to the bronze layer with ingesting timestamp.

# MAGIC %run /Workspace/databricks-course/F1-Project/00-common/01.enviroment-config

# MAGIC %run /Workspace/databricks-course/F1-Project/00-common/02.bronze-helpers

results_source_path = f"{landing_folder_path}/results"
results_table_name = f"{catalog_name}.{bronze_schema}.results"


from pyspark.sql.types import StructField, StringType, StructType, DateType, IntegerType, DoubleType

results_schema = StructType([
                StructField('date', DateType(), True),
                StructField('raceName', StringType(), True),
                StructField('round', IntegerType(), True),
                StructField('season', IntegerType(),True),
                StructField('url', StringType(),True),
                StructField('constructorId', StringType(),True),
                StructField('driverId', StringType(),True),
                StructField('grid', IntegerType(),True),
                StructField('laps', IntegerType(),True),
                StructField('number', IntegerType(),True),
                StructField('points', DoubleType(),True),
                StructField('position', IntegerType(),True),
                StructField('positionText', StringType(),True),
                StructField('status', StringType(),True)
])



results_df = (
    
    spark.read
                .format('json')
                .option('header', True)
                .option('mode','FAILFAST')   
                .schema(results_schema)
                .load(results_source_path)
    )


results_final_df = add_ingestion_data(results_df)


(
    results_final_df
    .write
    .format('delta')
    .mode('overWrite')
    .saveAsTable(results_table_name)

)
