# This file is ingesting sprints files from landing volume and writing them to the bronze layer with ingesting timestamp.

# MAGIC %run /Workspace/databricks-course/F1-Project/00-common/01.enviroment-config

# MAGIC %run /Workspace/databricks-course/F1-Project/00-common/02.bronze-helpers



sprints_source_path = f"{landing_folder_path}/sprints"
sprints_table_name = f"{catalog_name}.{bronze_schema}.sprints"

from pyspark.sql.types import StructField, StringType, StructType, DateType, IntegerType, DoubleType

sprints_schema = StructType([
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



sprints_df = (
    
    spark.read
                .format('json')
                .option('header', True)
                .option('mode','FAILFAST')
                .option('multiLine', True)   
                .schema(sprints_schema)
                .load(sprints_source_path)
    )


sprints_final_df = add_ingestion_data(sprints_df)


(
    sprints_final_df
    .write
    .format('delta')
    .mode('overWrite')
    .saveAsTable(sprints_table_name)

)
