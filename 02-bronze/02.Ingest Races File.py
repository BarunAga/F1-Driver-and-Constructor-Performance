# This file is ingesting races files from landing volume and writing them to the bronze layer with ingesting timestamp.

# MAGIC %run /Workspace/databricks-course/F1-Project/00-common/01.enviroment-config

# MAGIC %run /Workspace/databricks-course/F1-Project/00-common/02.bronze-helpers



races_source_path = f"{landing_folder_path}/races.csv"
races_table_name = f"{catalog_name}.{bronze_schema}.races"




from pyspark.sql.types import StructField, StringType, StructType, IntegerType, DateType

races_schema = StructType(
    [
        StructField('season', IntegerType(), True),
        StructField('round', IntegerType(), True),
        StructField('url', StringType(), True),
        StructField('raceName', StringType(), True),
        StructField('date', DateType(), True),
        StructField('circuitId', StringType(), True)
    ]
)


races_path = '/Volumes/formula1/landing/files/races.csv'
races_df = (
    
    spark.read
                .format('csv')
                .option('header', True)
                .option('mode','FAILFAST')   
                .schema(races_schema)
                .load(races_source_path)
    )



races_final_df = add_ingestion_data(races_df)


(
    races_final_df
    .write
    .format('delta')
    .mode('overWrite')
    .saveAsTable(races_table_name)

)
